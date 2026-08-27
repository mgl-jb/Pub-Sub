using System.Globalization;
using PubSub.Abstractions;

namespace PubSub.Filters;

/// <summary>Turns filter expression text into a token stream.</summary>
internal sealed class Lexer
{
    private static readonly Dictionary<string, TokenKind> Keywords =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["AND"] = TokenKind.And,
            ["OR"] = TokenKind.Or,
            ["NOT"] = TokenKind.Not,
            ["LIKE"] = TokenKind.Like,
            ["IN"] = TokenKind.In,
            ["IS"] = TokenKind.Is,
            ["NULL"] = TokenKind.Null,
            ["TRUE"] = TokenKind.True,
            ["FALSE"] = TokenKind.False,
            ["EXISTS"] = TokenKind.Exists,
            ["ESCAPE"] = TokenKind.Escape,
            ["SET"] = TokenKind.Set,
            ["REMOVE"] = TokenKind.Remove,
        };

    private readonly string _text;
    private readonly FilterLimits _limits;
    private int _position;

    public Lexer(string text, FilterLimits limits)
    {
        ArgumentNullException.ThrowIfNull(text);
        _limits = limits;

        if (text.Length > limits.MaxExpressionLength)
        {
            throw new FilterSyntaxException(
                $"The expression is {text.Length} characters, which exceeds the maximum of " +
                $"{limits.MaxExpressionLength}.",
                0);
        }

        _text = text;
    }

    /// <summary>Reads the whole input into a token list, ending with <see cref="TokenKind.EndOfInput"/>.</summary>
    public List<Token> Tokenize()
    {
        List<Token> tokens = [];
        while (true)
        {
            Token token = Next();
            tokens.Add(token);
            if (token.Kind == TokenKind.EndOfInput)
            {
                return tokens;
            }
        }
    }

    private Token Next()
    {
        SkipWhitespace();

        if (_position >= _text.Length)
        {
            return new Token(TokenKind.EndOfInput, string.Empty, null, _position);
        }

        int start = _position;
        char c = _text[_position];

        switch (c)
        {
            case '(': _position++; return new Token(TokenKind.LeftParen, "(", null, start);
            case ')': _position++; return new Token(TokenKind.RightParen, ")", null, start);
            case ',': _position++; return new Token(TokenKind.Comma, ",", null, start);
            case ';': _position++; return new Token(TokenKind.Semicolon, ";", null, start);
            case '.': _position++; return new Token(TokenKind.Dot, ".", null, start);
            case '+': _position++; return new Token(TokenKind.Plus, "+", null, start);
            case '-': _position++; return new Token(TokenKind.Minus, "-", null, start);
            case '*': _position++; return new Token(TokenKind.Star, "*", null, start);
            case '/': _position++; return new Token(TokenKind.Slash, "/", null, start);
            case '%': _position++; return new Token(TokenKind.Percent, "%", null, start);
            case '=': _position++; return new Token(TokenKind.Equal, "=", null, start);

            case '<':
                _position++;
                if (Peek() == '=') { _position++; return new Token(TokenKind.LessThanOrEqual, "<=", null, start); }
                if (Peek() == '>') { _position++; return new Token(TokenKind.NotEqual, "<>", null, start); }
                return new Token(TokenKind.LessThan, "<", null, start);

            case '>':
                _position++;
                if (Peek() == '=') { _position++; return new Token(TokenKind.GreaterThanOrEqual, ">=", null, start); }
                return new Token(TokenKind.GreaterThan, ">", null, start);

            case '!':
                _position++;
                if (Peek() == '=') { _position++; return new Token(TokenKind.NotEqual, "!=", null, start); }
                throw new FilterSyntaxException("Expected '=' after '!'.", start);

            case '\'':
                return ReadStringLiteral();

            case '[':
                return ReadBracketedIdentifier();
        }

        if (char.IsAsciiDigit(c))
        {
            return ReadNumber();
        }

        if (char.IsLetter(c) || c == '_')
        {
            return ReadIdentifierOrKeyword();
        }

        throw new FilterSyntaxException($"Unexpected character '{c}'.", start);
    }

    private void SkipWhitespace()
    {
        while (_position < _text.Length && char.IsWhiteSpace(_text[_position]))
        {
            _position++;
        }
    }

    private char Peek() => _position < _text.Length ? _text[_position] : '\0';

    /// <summary>
    /// Reads a single-quoted string. A doubled quote (<c>''</c>) is a literal quote, matching SQL —
    /// there is no backslash escaping, so a caller cannot smuggle one in.
    /// </summary>
    private Token ReadStringLiteral()
    {
        int start = _position;
        _position++; // opening quote

        System.Text.StringBuilder builder = new();
        while (true)
        {
            if (_position >= _text.Length)
            {
                throw new FilterSyntaxException("Unterminated string literal.", start);
            }

            char c = _text[_position];
            if (c == '\'')
            {
                if (_position + 1 < _text.Length && _text[_position + 1] == '\'')
                {
                    builder.Append('\'');
                    _position += 2;
                    continue;
                }

                _position++; // closing quote
                break;
            }

            builder.Append(c);
            _position++;

            if (builder.Length > _limits.MaxStringLiteralLength)
            {
                throw new FilterSyntaxException(
                    $"String literal exceeds the maximum length of {_limits.MaxStringLiteralLength}.",
                    start);
            }
        }

        string value = builder.ToString();
        return new Token(TokenKind.StringLiteral, value, value, start);
    }

    /// <summary>Reads a <c>[bracketed]</c> identifier, which may contain spaces and punctuation.</summary>
    private Token ReadBracketedIdentifier()
    {
        int start = _position;
        _position++; // opening bracket

        int contentStart = _position;
        while (_position < _text.Length && _text[_position] != ']')
        {
            _position++;
        }

        if (_position >= _text.Length)
        {
            throw new FilterSyntaxException("Unterminated bracketed identifier.", start);
        }

        string name = _text[contentStart.._position];
        _position++; // closing bracket

        if (name.Length == 0)
        {
            throw new FilterSyntaxException("A bracketed identifier cannot be empty.", start);
        }

        if (name.Length > _limits.MaxIdentifierLength)
        {
            throw new FilterSyntaxException(
                $"Identifier exceeds the maximum length of {_limits.MaxIdentifierLength}.", start);
        }

        return new Token(TokenKind.Identifier, name, null, start);
    }

    private Token ReadNumber()
    {
        int start = _position;
        bool hasDecimalPoint = false;
        bool hasExponent = false;

        while (_position < _text.Length)
        {
            char c = _text[_position];

            if (char.IsAsciiDigit(c))
            {
                _position++;
            }
            else if (c == '.' && !hasDecimalPoint && !hasExponent
                     && _position + 1 < _text.Length && char.IsAsciiDigit(_text[_position + 1]))
            {
                hasDecimalPoint = true;
                _position++;
            }
            else if ((c is 'e' or 'E') && !hasExponent && _position + 1 < _text.Length)
            {
                int lookahead = _position + 1;
                if (_text[lookahead] is '+' or '-')
                {
                    lookahead++;
                }

                if (lookahead < _text.Length && char.IsAsciiDigit(_text[lookahead]))
                {
                    hasExponent = true;
                    _position = lookahead;
                }
                else
                {
                    break;
                }
            }
            else
            {
                break;
            }
        }

        string text = _text[start.._position];

        // Integers stay integral so that equality against an int-valued property is exact;
        // only genuinely fractional literals become double.
        if (!hasDecimalPoint && !hasExponent && long.TryParse(text, CultureInfo.InvariantCulture, out long integral))
        {
            return new Token(TokenKind.NumberLiteral, text, integral, start);
        }

        if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double real))
        {
            return new Token(TokenKind.NumberLiteral, text, real, start);
        }

        throw new FilterSyntaxException($"'{text}' is not a valid number.", start);
    }

    private Token ReadIdentifierOrKeyword()
    {
        int start = _position;
        while (_position < _text.Length && (char.IsLetterOrDigit(_text[_position]) || _text[_position] == '_'))
        {
            _position++;
        }

        string text = _text[start.._position];

        if (text.Length > _limits.MaxIdentifierLength)
        {
            throw new FilterSyntaxException(
                $"Identifier exceeds the maximum length of {_limits.MaxIdentifierLength}.", start);
        }

        return Keywords.TryGetValue(text, out TokenKind keyword)
            ? new Token(keyword, text, null, start)
            : new Token(TokenKind.Identifier, text, null, start);
    }
}
