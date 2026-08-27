using System.Globalization;

namespace PubSub.Filters;

/// <summary>
/// Value semantics for the filter language, following SQL rather than C#.
/// </summary>
/// <remarks>
/// <para>
/// Two rules drive everything here. First, <c>null</c> means SQL <c>NULL</c>: any comparison
/// involving it yields UNKNOWN, represented as a <c>null</c> <see cref="bool"/>?. Second, a
/// comparison between values that cannot sensibly be compared — a string against a number —
/// also yields UNKNOWN rather than throwing.
/// </para>
/// <para>
/// That second rule matters operationally. A filter is evaluated against every message on the
/// topic, including ones whose shape the rule's author never anticipated. Throwing would turn one
/// odd message into a dead-lettered message or a failed publish; yielding UNKNOWN simply means the
/// message does not match that rule, which is almost always what was intended.
/// </para>
/// </remarks>
public static class SqlValue
{
    /// <summary>
    /// Compares two values, returning a negative number, zero, or a positive number, or
    /// <c>null</c> when the comparison is UNKNOWN.
    /// </summary>
    public static int? Compare(object? left, object? right)
    {
        if (left is null || right is null)
        {
            return null;
        }

        if (TryAsNumber(left, out decimal leftNumber) && TryAsNumber(right, out decimal rightNumber))
        {
            return leftNumber.CompareTo(rightNumber);
        }

        if (left is string leftString && right is string rightString)
        {
            return string.CompareOrdinal(leftString, rightString);
        }

        if (left is bool leftBool && right is bool rightBool)
        {
            return leftBool.CompareTo(rightBool);
        }

        if (TryAsInstant(left, out DateTimeOffset leftInstant) && TryAsInstant(right, out DateTimeOffset rightInstant))
        {
            return leftInstant.CompareTo(rightInstant);
        }

        if (left is Guid leftGuid && right is Guid rightGuid)
        {
            return leftGuid.CompareTo(rightGuid);
        }

        // Mismatched types: UNKNOWN, not an error.
        return null;
    }

    /// <summary>Equality under SQL semantics, yielding UNKNOWN when either side is null.</summary>
    public static bool? AreEqual(object? left, object? right)
    {
        if (left is null || right is null)
        {
            return null;
        }

        int? comparison = Compare(left, right);
        return comparison is null ? null : comparison.Value == 0;
    }

    /// <summary>Logical AND over three-valued logic.</summary>
    /// <remarks>
    /// UNKNOWN AND FALSE is FALSE — a definite result even though one side is unknown, because no
    /// value of the unknown side could make the conjunction true.
    /// </remarks>
    public static bool? And(bool? left, bool? right)
    {
        if (left == false || right == false)
        {
            return false;
        }

        if (left is null || right is null)
        {
            return null;
        }

        return left.Value && right.Value;
    }

    /// <summary>Logical OR over three-valued logic.</summary>
    /// <remarks>UNKNOWN OR TRUE is TRUE, by the mirror of the argument for <see cref="And"/>.</remarks>
    public static bool? Or(bool? left, bool? right)
    {
        if (left == true || right == true)
        {
            return true;
        }

        if (left is null || right is null)
        {
            return null;
        }

        return left.Value || right.Value;
    }

    /// <summary>Logical NOT, which leaves UNKNOWN unchanged.</summary>
    public static bool? Not(bool? value) => value is null ? null : !value.Value;

    /// <summary>
    /// Coerces a value to a boolean for use in a logical position, yielding UNKNOWN for anything
    /// that is not a boolean.
    /// </summary>
    public static bool? AsBoolean(object? value) => value switch
    {
        null => null,
        bool b => b,
        _ => null,
    };

    /// <summary>Arithmetic negation, yielding <c>null</c> for non-numeric input.</summary>
    public static object? Negate(object? value)
    {
        if (value is null)
        {
            return null;
        }

        if (value is long integral)
        {
            return -integral;
        }

        if (TryAsNumber(value, out decimal number))
        {
            return IsIntegral(value) ? Reduce(-number) : (object)(double)(-number);
        }

        return null;
    }

    /// <summary>
    /// Applies an arithmetic operator, yielding <c>null</c> when either operand is null or
    /// non-numeric, or when the operation is undefined (division by zero, overflow).
    /// </summary>
    public static object? Arithmetic(BinaryOperator op, object? left, object? right)
    {
        if (left is null || right is null)
        {
            return null;
        }

        // String concatenation is the one non-numeric arithmetic case worth supporting.
        if (op == BinaryOperator.Add && left is string leftText && right is string rightText)
        {
            return leftText + rightText;
        }

        if (!TryAsNumber(left, out decimal a) || !TryAsNumber(right, out decimal b))
        {
            return null;
        }

        bool integral = IsIntegral(left) && IsIntegral(right);

        try
        {
            decimal result = op switch
            {
                BinaryOperator.Add => a + b,
                BinaryOperator.Subtract => a - b,
                BinaryOperator.Multiply => a * b,
                BinaryOperator.Divide => b == 0m ? throw new DivideByZeroException() : a / b,
                BinaryOperator.Modulo => b == 0m ? throw new DivideByZeroException() : a % b,
                _ => throw new ArgumentOutOfRangeException(nameof(op), op, "Not an arithmetic operator."),
            };

            // Division can produce a fraction from two integers, so it never stays integral.
            if (integral && op != BinaryOperator.Divide)
            {
                return Reduce(result);
            }

            return result == decimal.Truncate(result) && op != BinaryOperator.Divide
                ? (object)(double)result
                : (double)result;
        }
        catch (DivideByZeroException)
        {
            return null;
        }
        catch (OverflowException)
        {
            return null;
        }
    }

    /// <summary>Renders a value as text for <c>LIKE</c> matching, or <c>null</c> if it is not a string.</summary>
    public static string? AsString(object? value) => value as string;

    /// <summary>
    /// Narrows an exact integral result back to <see cref="long"/> so that integer arithmetic
    /// yields an integer.
    /// </summary>
    /// <remarks>
    /// Both branches are cast to <see cref="object"/> explicitly. Without that the conditional
    /// unifies on <see cref="decimal"/> — <see cref="long"/> converts to it implicitly — and every
    /// result silently widens, which is the opposite of the intent.
    /// </remarks>
    private static object Reduce(decimal value)
    {
        if (value >= long.MinValue && value <= long.MaxValue && value == decimal.Truncate(value))
        {
            long integral = (long)value;
            return integral;
        }

        return value;
    }

    private static bool IsIntegral(object value) =>
        value is byte or sbyte or short or ushort or int or uint or long or ulong;

    /// <summary>Widens any numeric CLR type to <see cref="decimal"/> for uniform comparison.</summary>
    private static bool TryAsNumber(object value, out decimal number)
    {
        switch (value)
        {
            case byte v: number = v; return true;
            case sbyte v: number = v; return true;
            case short v: number = v; return true;
            case ushort v: number = v; return true;
            case int v: number = v; return true;
            case uint v: number = v; return true;
            case long v: number = v; return true;
            case ulong v: number = v; return true;
            case decimal v: number = v; return true;

            case float v:
                if (float.IsNaN(v) || float.IsInfinity(v)) { number = 0; return false; }
                number = (decimal)v;
                return true;

            case double v:
                if (double.IsNaN(v) || double.IsInfinity(v)) { number = 0; return false; }
                try { number = (decimal)v; return true; }
                catch (OverflowException) { number = 0; return false; }

            default:
                number = 0;
                return false;
        }
    }

    private static bool TryAsInstant(object value, out DateTimeOffset instant)
    {
        switch (value)
        {
            case DateTimeOffset v:
                instant = v;
                return true;

            case DateTime v:
                instant = new DateTimeOffset(v.ToUniversalTime(), TimeSpan.Zero);
                return true;

            default:
                instant = default;
                return false;
        }
    }

    /// <summary>Formats a value for diagnostics using invariant culture.</summary>
    internal static string Describe(object? value) => value switch
    {
        null => "NULL",
        string s => $"'{s}'",
        bool b => b ? "TRUE" : "FALSE",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "NULL",
    };
}
