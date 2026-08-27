namespace PubSub.Filters;

/// <summary>One lexical token, with the source offset used for error messages.</summary>
internal readonly record struct Token(TokenKind Kind, string Text, object? Value, int Position)
{
    public override string ToString() => Kind switch
    {
        TokenKind.EndOfInput => "end of expression",
        TokenKind.StringLiteral => $"string '{Text}'",
        TokenKind.NumberLiteral => $"number {Text}",
        TokenKind.Identifier => $"identifier '{Text}'",
        _ => $"'{Text}'",
    };
}
