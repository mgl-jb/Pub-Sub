namespace PubSub.Filters;

/// <summary>A node in a parsed filter expression.</summary>
/// <remarks>
/// The hierarchy is closed: the compiler switches exhaustively over it, so a node type it does not
/// know about cannot be evaluated.
/// </remarks>
public abstract class FilterExpression
{
    private protected FilterExpression()
    {
    }

    /// <summary>Renders the node back to source-like text, for diagnostics and admin listings.</summary>
    public abstract override string ToString();
}

/// <summary>Where a property is read from.</summary>
public enum PropertySource
{
    /// <summary>A producer-set entry in the message's application properties.</summary>
    Application,

    /// <summary>A built-in message property, written <c>sys.Name</c> in an expression.</summary>
    System,
}

/// <summary>A constant: string, number, boolean, or null.</summary>
public sealed class LiteralExpression : FilterExpression
{
    /// <summary>Creates a literal.</summary>
    public LiteralExpression(object? value) => Value = value;

    /// <summary>The constant value. <c>null</c> represents the SQL <c>NULL</c>.</summary>
    public object? Value { get; }

    /// <inheritdoc />
    public override string ToString() => Value switch
    {
        null => "NULL",
        string s => $"'{s.Replace("'", "''", StringComparison.Ordinal)}'",
        bool b => b ? "TRUE" : "FALSE",
        _ => Convert.ToString(Value, System.Globalization.CultureInfo.InvariantCulture) ?? "NULL",
    };
}

/// <summary>A reference to a message property.</summary>
public sealed class PropertyExpression : FilterExpression
{
    /// <summary>Creates a property reference.</summary>
    public PropertyExpression(string name, PropertySource source)
    {
        Name = name;
        Source = source;
    }

    /// <summary>The property name.</summary>
    public string Name { get; }

    /// <summary>Whether this reads a system or an application property.</summary>
    public PropertySource Source { get; }

    /// <inheritdoc />
    public override string ToString() =>
        Source == PropertySource.System ? $"sys.{Name}" : Name;
}

/// <summary>Unary operators.</summary>
public enum UnaryOperator
{
    /// <summary>Logical negation, which propagates UNKNOWN.</summary>
    Not,

    /// <summary>Arithmetic negation.</summary>
    Negate,
}

/// <summary>A unary operation.</summary>
public sealed class UnaryExpression : FilterExpression
{
    /// <summary>Creates a unary operation.</summary>
    public UnaryExpression(UnaryOperator op, FilterExpression operand)
    {
        Operator = op;
        Operand = operand;
    }

    /// <summary>The operator.</summary>
    public UnaryOperator Operator { get; }

    /// <summary>The operand.</summary>
    public FilterExpression Operand { get; }

    /// <inheritdoc />
    public override string ToString() =>
        Operator == UnaryOperator.Not ? $"NOT ({Operand})" : $"-({Operand})";
}

/// <summary>Binary operators, logical, comparison, and arithmetic.</summary>
public enum BinaryOperator
{
    /// <summary>Logical conjunction.</summary>
    And,

    /// <summary>Logical disjunction.</summary>
    Or,

    /// <summary>Equality.</summary>
    Equal,

    /// <summary>Inequality.</summary>
    NotEqual,

    /// <summary>Less than.</summary>
    LessThan,

    /// <summary>Less than or equal.</summary>
    LessThanOrEqual,

    /// <summary>Greater than.</summary>
    GreaterThan,

    /// <summary>Greater than or equal.</summary>
    GreaterThanOrEqual,

    /// <summary>Addition.</summary>
    Add,

    /// <summary>Subtraction.</summary>
    Subtract,

    /// <summary>Multiplication.</summary>
    Multiply,

    /// <summary>Division.</summary>
    Divide,

    /// <summary>Remainder.</summary>
    Modulo,
}

/// <summary>A binary operation.</summary>
public sealed class BinaryExpression : FilterExpression
{
    /// <summary>Creates a binary operation.</summary>
    public BinaryExpression(BinaryOperator op, FilterExpression left, FilterExpression right)
    {
        Operator = op;
        Left = left;
        Right = right;
    }

    /// <summary>The operator.</summary>
    public BinaryOperator Operator { get; }

    /// <summary>The left operand.</summary>
    public FilterExpression Left { get; }

    /// <summary>The right operand.</summary>
    public FilterExpression Right { get; }

    /// <inheritdoc />
    public override string ToString() => $"({Left} {Symbol()} {Right})";

    private string Symbol() => Operator switch
    {
        BinaryOperator.And => "AND",
        BinaryOperator.Or => "OR",
        BinaryOperator.Equal => "=",
        BinaryOperator.NotEqual => "<>",
        BinaryOperator.LessThan => "<",
        BinaryOperator.LessThanOrEqual => "<=",
        BinaryOperator.GreaterThan => ">",
        BinaryOperator.GreaterThanOrEqual => ">=",
        BinaryOperator.Add => "+",
        BinaryOperator.Subtract => "-",
        BinaryOperator.Multiply => "*",
        BinaryOperator.Divide => "/",
        BinaryOperator.Modulo => "%",
        _ => "?",
    };
}

/// <summary>A <c>LIKE</c> pattern match.</summary>
public sealed class LikeExpression : FilterExpression
{
    /// <summary>Creates a pattern match.</summary>
    public LikeExpression(FilterExpression value, string pattern, char? escape, bool negated)
    {
        Value = value;
        Pattern = pattern;
        Escape = escape;
        Negated = negated;
    }

    /// <summary>The expression being matched.</summary>
    public FilterExpression Value { get; }

    /// <summary>The pattern, where <c>%</c> matches any run of characters and <c>_</c> matches one.</summary>
    public string Pattern { get; }

    /// <summary>The escape character that makes a following wildcard literal, if given.</summary>
    public char? Escape { get; }

    /// <summary>Whether this is <c>NOT LIKE</c>.</summary>
    public bool Negated { get; }

    /// <inheritdoc />
    public override string ToString()
    {
        string escape = Escape is null ? string.Empty : $" ESCAPE '{Escape}'";
        return $"({Value} {(Negated ? "NOT LIKE" : "LIKE")} '{Pattern}'{escape})";
    }
}

/// <summary>An <c>IN</c> membership test.</summary>
public sealed class InExpression : FilterExpression
{
    /// <summary>Creates a membership test.</summary>
    public InExpression(FilterExpression value, IReadOnlyList<FilterExpression> items, bool negated)
    {
        Value = value;
        Items = items;
        Negated = negated;
    }

    /// <summary>The expression being tested.</summary>
    public FilterExpression Value { get; }

    /// <summary>The candidate values.</summary>
    public IReadOnlyList<FilterExpression> Items { get; }

    /// <summary>Whether this is <c>NOT IN</c>.</summary>
    public bool Negated { get; }

    /// <inheritdoc />
    public override string ToString() =>
        $"({Value} {(Negated ? "NOT IN" : "IN")} ({string.Join(", ", Items)}))";
}

/// <summary>An <c>IS NULL</c> / <c>IS NOT NULL</c> test.</summary>
/// <remarks>
/// This is the only way to test for null, because <c>= NULL</c> yields UNKNOWN rather than true —
/// the same trap SQL has.
/// </remarks>
public sealed class IsNullExpression : FilterExpression
{
    /// <summary>Creates a null test.</summary>
    public IsNullExpression(FilterExpression operand, bool negated)
    {
        Operand = operand;
        Negated = negated;
    }

    /// <summary>The expression being tested.</summary>
    public FilterExpression Operand { get; }

    /// <summary>Whether this is <c>IS NOT NULL</c>.</summary>
    public bool Negated { get; }

    /// <inheritdoc />
    public override string ToString() => $"({Operand} IS {(Negated ? "NOT " : string.Empty)}NULL)";
}

/// <summary>
/// An <c>EXISTS(property)</c> test, true when the property is present at all — including when its
/// value is null, which is what distinguishes it from <c>IS NOT NULL</c>.
/// </summary>
public sealed class ExistsExpression : FilterExpression
{
    /// <summary>Creates a presence test.</summary>
    public ExistsExpression(PropertyExpression property) => Property = property;

    /// <summary>The property whose presence is tested.</summary>
    public PropertyExpression Property { get; }

    /// <inheritdoc />
    public override string ToString() => $"EXISTS({Property})";
}
