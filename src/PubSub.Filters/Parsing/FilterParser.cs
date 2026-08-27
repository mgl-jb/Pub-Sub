using PubSub.Abstractions;

namespace PubSub.Filters;

/// <summary>
/// Parses filter expression text into a <see cref="FilterExpression"/> tree.
/// </summary>
/// <remarks>
/// A precedence-climbing recursive-descent parser. Precedence, lowest binding first:
/// <c>OR</c>, <c>AND</c>, <c>NOT</c>, comparison and the <c>LIKE</c> / <c>IN</c> / <c>IS</c>
/// postfix forms, additive, multiplicative, unary minus, primary.
/// </remarks>
public sealed class FilterParser
{
    private readonly List<Token> _tokens;
    private readonly FilterLimits _limits;
    private int _index;
    private int _depth;

    private FilterParser(List<Token> tokens, FilterLimits limits)
    {
        _tokens = tokens;
        _limits = limits;
    }

    /// <summary>Parses a boolean filter expression.</summary>
    /// <param name="expression">The expression text.</param>
    /// <param name="limits">Parser bounds; defaults to <see cref="FilterLimits.Default"/>.</param>
    /// <exception cref="FilterSyntaxException">The expression is malformed or exceeds a limit.</exception>
    public static FilterExpression Parse(string expression, FilterLimits? limits = null)
    {
        ArgumentNullException.ThrowIfNull(expression);
        limits ??= FilterLimits.Default;

        List<Token> tokens = new Lexer(expression, limits).Tokenize();
        FilterParser parser = new(tokens, limits);

        FilterExpression result = parser.ParseOr();
        parser.Expect(TokenKind.EndOfInput, "end of expression");
        return result;
    }

    /// <summary>
    /// Parses without throwing.
    /// </summary>
    /// <returns><c>true</c> when the expression parsed; otherwise <c>false</c>, with <paramref name="error"/> set.</returns>
    public static bool TryParse(
        string expression,
        out FilterExpression? result,
        out string? error,
        FilterLimits? limits = null)
    {
        try
        {
            result = Parse(expression, limits);
            error = null;
            return true;
        }
        catch (FilterSyntaxException ex)
        {
            result = null;
            error = ex.Message;
            return false;
        }
    }

    private Token Current => _tokens[_index];

    private Token Advance() => _tokens[_index++];

    private bool Match(TokenKind kind)
    {
        if (Current.Kind != kind)
        {
            return false;
        }

        _index++;
        return true;
    }

    private Token Expect(TokenKind kind, string expected)
    {
        if (Current.Kind != kind)
        {
            throw new FilterSyntaxException($"Expected {expected} but found {Current}.", Current.Position);
        }

        return Advance();
    }

    /// <summary>
    /// Guards recursive descent. Without this a deeply parenthesised expression would overflow the
    /// stack while parsing — an uncatchable process kill, not an exception.
    /// </summary>
    private void EnterNesting()
    {
        if (++_depth > _limits.MaxDepth)
        {
            throw new FilterSyntaxException(
                $"The expression nests deeper than the maximum of {_limits.MaxDepth}.",
                Current.Position);
        }
    }

    private void ExitNesting() => _depth--;

    private FilterExpression ParseOr()
    {
        EnterNesting();
        try
        {
            FilterExpression left = ParseAnd();
            while (Match(TokenKind.Or))
            {
                FilterExpression right = ParseAnd();
                left = new BinaryExpression(BinaryOperator.Or, left, right);
            }

            return left;
        }
        finally
        {
            ExitNesting();
        }
    }

    private FilterExpression ParseAnd()
    {
        EnterNesting();
        try
        {
            FilterExpression left = ParseNot();
            while (Match(TokenKind.And))
            {
                FilterExpression right = ParseNot();
                left = new BinaryExpression(BinaryOperator.And, left, right);
            }

            return left;
        }
        finally
        {
            ExitNesting();
        }
    }

    private FilterExpression ParseNot()
    {
        if (Match(TokenKind.Not))
        {
            EnterNesting();
            try
            {
                return new UnaryExpression(UnaryOperator.Not, ParseNot());
            }
            finally
            {
                ExitNesting();
            }
        }

        return ParseComparison();
    }

    private FilterExpression ParseComparison()
    {
        EnterNesting();
        try
        {
            FilterExpression left = ParseAdditive();

            // The postfix forms bind at comparison level and do not chain.
            switch (Current.Kind)
            {
                case TokenKind.Is:
                    return ParseIsNull(left);

                case TokenKind.Like:
                    Advance();
                    return ParseLike(left, negated: false);

                case TokenKind.In:
                    Advance();
                    return ParseIn(left, negated: false);

                case TokenKind.Not:
                    // NOT here can only introduce NOT LIKE or NOT IN.
                    Advance();
                    if (Match(TokenKind.Like))
                    {
                        return ParseLike(left, negated: true);
                    }

                    if (Match(TokenKind.In))
                    {
                        return ParseIn(left, negated: true);
                    }

                    throw new FilterSyntaxException(
                        $"Expected LIKE or IN after NOT but found {Current}.", Current.Position);
            }

            BinaryOperator? op = Current.Kind switch
            {
                TokenKind.Equal => BinaryOperator.Equal,
                TokenKind.NotEqual => BinaryOperator.NotEqual,
                TokenKind.LessThan => BinaryOperator.LessThan,
                TokenKind.LessThanOrEqual => BinaryOperator.LessThanOrEqual,
                TokenKind.GreaterThan => BinaryOperator.GreaterThan,
                TokenKind.GreaterThanOrEqual => BinaryOperator.GreaterThanOrEqual,
                _ => null,
            };

            if (op is null)
            {
                return left;
            }

            Advance();
            FilterExpression right = ParseAdditive();
            return new BinaryExpression(op.Value, left, right);
        }
        finally
        {
            ExitNesting();
        }
    }

    private IsNullExpression ParseIsNull(FilterExpression left)
    {
        Expect(TokenKind.Is, "IS");
        bool negated = Match(TokenKind.Not);
        Expect(TokenKind.Null, "NULL");
        return new IsNullExpression(left, negated);
    }

    private LikeExpression ParseLike(FilterExpression left, bool negated)
    {
        Token pattern = Expect(TokenKind.StringLiteral, "a string pattern");

        char? escape = null;
        if (Match(TokenKind.Escape))
        {
            Token escapeToken = Expect(TokenKind.StringLiteral, "a single-character escape");
            string escapeText = (string)escapeToken.Value!;
            if (escapeText.Length != 1)
            {
                throw new FilterSyntaxException(
                    "The ESCAPE value must be exactly one character.", escapeToken.Position);
            }

            escape = escapeText[0];
        }

        return new LikeExpression(left, (string)pattern.Value!, escape, negated);
    }

    private InExpression ParseIn(FilterExpression left, bool negated)
    {
        Expect(TokenKind.LeftParen, "'(' after IN");

        List<FilterExpression> items = [];
        if (Current.Kind != TokenKind.RightParen)
        {
            do
            {
                if (items.Count >= _limits.MaxInListItems)
                {
                    throw new FilterSyntaxException(
                        $"An IN list may hold at most {_limits.MaxInListItems} values.",
                        Current.Position);
                }

                items.Add(ParseAdditive());
            }
            while (Match(TokenKind.Comma));
        }

        Expect(TokenKind.RightParen, "')' to close the IN list");

        if (items.Count == 0)
        {
            throw new FilterSyntaxException("An IN list cannot be empty.", Current.Position);
        }

        return new InExpression(left, items, negated);
    }

    private FilterExpression ParseAdditive()
    {
        EnterNesting();
        try
        {
            FilterExpression left = ParseMultiplicative();
            while (true)
            {
                BinaryOperator op;
                if (Match(TokenKind.Plus))
                {
                    op = BinaryOperator.Add;
                }
                else if (Match(TokenKind.Minus))
                {
                    op = BinaryOperator.Subtract;
                }
                else
                {
                    return left;
                }

                left = new BinaryExpression(op, left, ParseMultiplicative());
            }
        }
        finally
        {
            ExitNesting();
        }
    }

    private FilterExpression ParseMultiplicative()
    {
        EnterNesting();
        try
        {
            FilterExpression left = ParseUnary();
            while (true)
            {
                BinaryOperator op;
                if (Match(TokenKind.Star))
                {
                    op = BinaryOperator.Multiply;
                }
                else if (Match(TokenKind.Slash))
                {
                    op = BinaryOperator.Divide;
                }
                else if (Match(TokenKind.Percent))
                {
                    op = BinaryOperator.Modulo;
                }
                else
                {
                    return left;
                }

                left = new BinaryExpression(op, left, ParseUnary());
            }
        }
        finally
        {
            ExitNesting();
        }
    }

    private FilterExpression ParseUnary()
    {
        if (Match(TokenKind.Minus))
        {
            EnterNesting();
            try
            {
                return new UnaryExpression(UnaryOperator.Negate, ParseUnary());
            }
            finally
            {
                ExitNesting();
            }
        }

        if (Match(TokenKind.Plus))
        {
            return ParseUnary();
        }

        return ParsePrimary();
    }

    private FilterExpression ParsePrimary()
    {
        Token token = Current;

        switch (token.Kind)
        {
            case TokenKind.NumberLiteral:
            case TokenKind.StringLiteral:
                Advance();
                return new LiteralExpression(token.Value);

            case TokenKind.True:
                Advance();
                return new LiteralExpression(true);

            case TokenKind.False:
                Advance();
                return new LiteralExpression(false);

            case TokenKind.Null:
                Advance();
                return new LiteralExpression(null);

            case TokenKind.LeftParen:
                {
                    Advance();
                    EnterNesting();
                    try
                    {
                        FilterExpression inner = ParseOr();
                        Expect(TokenKind.RightParen, "')'");
                        return inner;
                    }
                    finally
                    {
                        ExitNesting();
                    }
                }

            case TokenKind.Exists:
                {
                    Advance();
                    Expect(TokenKind.LeftParen, "'(' after EXISTS");
                    FilterExpression operand = ParsePrimary();
                    Expect(TokenKind.RightParen, "')' to close EXISTS");

                    if (operand is not PropertyExpression property)
                    {
                        throw new FilterSyntaxException(
                            "EXISTS takes a property name.", token.Position);
                    }

                    return new ExistsExpression(property);
                }

            case TokenKind.Identifier:
                return ParsePropertyReference();

            default:
                throw new FilterSyntaxException($"Unexpected {token}.", token.Position);
        }
    }

    /// <summary>
    /// Reads a property reference. A bare name reads an application property; the reserved
    /// <c>sys.</c> prefix reads a built-in message property.
    /// </summary>
    private PropertyExpression ParsePropertyReference()
    {
        Token first = Expect(TokenKind.Identifier, "a property name");

        if (!Match(TokenKind.Dot))
        {
            return new PropertyExpression(first.Text, PropertySource.Application);
        }

        Token second = Expect(TokenKind.Identifier, "a property name after '.'");

        if (!string.Equals(first.Text, "sys", StringComparison.OrdinalIgnoreCase))
        {
            throw new FilterSyntaxException(
                $"'{first.Text}.' is not a recognised qualifier. Use 'sys.' for built-in message " +
                "properties, or an unqualified name for an application property.",
                first.Position);
        }

        if (!SystemProperties.IsKnown(second.Text))
        {
            throw new FilterSyntaxException(
                $"'sys.{second.Text}' is not a known system property. Valid names are: " +
                $"{string.Join(", ", SystemProperties.Names)}.",
                second.Position);
        }

        return new PropertyExpression(SystemProperties.Normalize(second.Text), PropertySource.System);
    }
}
