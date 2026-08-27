namespace PubSub.Filters;

/// <summary>The lexical categories of the filter language.</summary>
internal enum TokenKind
{
    EndOfInput,

    Identifier,
    StringLiteral,
    NumberLiteral,

    // Punctuation
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Dot,

    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Arithmetic
    Plus,
    Minus,
    Star,
    Slash,
    Percent,

    // Keywords
    And,
    Or,
    Not,
    Like,
    In,
    Is,
    Null,
    True,
    False,
    Exists,
    Escape,
    Set,
    Remove,
}
