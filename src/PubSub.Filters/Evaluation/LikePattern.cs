using System.Text;
using System.Text.RegularExpressions;
using PubSub.Abstractions;

namespace PubSub.Filters;

/// <summary>
/// Compiles a SQL <c>LIKE</c> pattern into a regular expression.
/// </summary>
/// <remarks>
/// The translation escapes every regex metacharacter in the pattern before substituting the two
/// SQL wildcards, so a pattern containing <c>.*</c> or <c>(a+)+</c> matches those characters
/// literally instead of becoming a regex — which also means a caller cannot smuggle a
/// catastrophically backtracking expression in through a filter. A match timeout guards the
/// remainder.
/// </remarks>
internal static class LikePattern
{
    private static readonly TimeSpan MatchTimeout = TimeSpan.FromMilliseconds(100);

    public static Regex Compile(string pattern, char? escape)
    {
        ArgumentNullException.ThrowIfNull(pattern);

        StringBuilder builder = new(pattern.Length * 2);
        builder.Append(@"\A");

        for (int i = 0; i < pattern.Length; i++)
        {
            char c = pattern[i];

            if (escape.HasValue && c == escape.Value)
            {
                i++;
                if (i >= pattern.Length)
                {
                    throw new FilterSyntaxException(
                        $"The pattern ends with a dangling escape character '{escape.Value}'.");
                }

                builder.Append(Regex.Escape(pattern[i].ToString()));
                continue;
            }

            switch (c)
            {
                case '%':
                    builder.Append(".*");
                    break;

                case '_':
                    builder.Append('.');
                    break;

                default:
                    builder.Append(Regex.Escape(c.ToString()));
                    break;
            }
        }

        builder.Append(@"\z");

        return new Regex(
            builder.ToString(),
            RegexOptions.Singleline | RegexOptions.CultureInvariant,
            MatchTimeout);
    }
}
