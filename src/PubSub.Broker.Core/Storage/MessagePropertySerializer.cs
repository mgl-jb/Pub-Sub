using System.Text.Json;

namespace PubSub.Broker.Core;

/// <summary>
/// Serializes application properties to and from the JSON stored on a message.
/// </summary>
/// <remarks>
/// Round-tripping preserves enough type information for filters to work. JSON numbers come back as
/// <see cref="long"/> when integral and <see cref="double"/> otherwise, which is what the filter
/// language's numeric comparison expects — a property stored as <c>100</c> must still compare
/// equal to the literal <c>100</c> after a round trip.
/// </remarks>
public static class MessagePropertySerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
    };

    /// <summary>Serializes properties, returning <c>null</c> when there are none to store.</summary>
    public static string? Serialize(IDictionary<string, object?>? properties)
    {
        if (properties is null || properties.Count == 0)
        {
            return null;
        }

        return JsonSerializer.Serialize(properties, Options);
    }

    /// <summary>Deserializes properties, returning an empty dictionary for <c>null</c> input.</summary>
    public static Dictionary<string, object?> Deserialize(string? json)
    {
        Dictionary<string, object?> result = new(StringComparer.Ordinal);

        if (string.IsNullOrEmpty(json))
        {
            return result;
        }

        using JsonDocument document = JsonDocument.Parse(json);

        if (document.RootElement.ValueKind != JsonValueKind.Object)
        {
            return result;
        }

        foreach (JsonProperty property in document.RootElement.EnumerateObject())
        {
            result[property.Name] = ReadValue(property.Value);
        }

        return result;
    }

    /// <summary>
    /// Converts a value that may still be a <see cref="JsonElement"/> into a plain CLR value.
    /// </summary>
    /// <remarks>
    /// Properties arriving over HTTP deserialize into <see cref="JsonElement"/> rather than
    /// primitives. A filter comparing one of those against a number yields UNKNOWN — the message
    /// silently fails to route — so anything entering the broker from the wire is normalised here,
    /// using the same rules as the stored form.
    /// </remarks>
    public static object? Normalize(object? value) =>
        value is JsonElement element ? ReadValue(element) : value;

    /// <summary>Normalizes every value in a property dictionary.</summary>
    public static Dictionary<string, object?> NormalizeAll(IDictionary<string, object?>? properties)
    {
        Dictionary<string, object?> result = new(StringComparer.Ordinal);

        if (properties is null)
        {
            return result;
        }

        foreach (KeyValuePair<string, object?> property in properties)
        {
            result[property.Key] = Normalize(property.Value);
        }

        return result;
    }

    private static object? ReadValue(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null or JsonValueKind.Undefined => null,

        // Integral values stay integral so numeric comparison against an integer literal is exact.
        JsonValueKind.Number => element.TryGetInt64(out long integral)
            ? integral
            : element.GetDouble(),

        // Filters compare scalars; a nested object or array is kept as its raw text so nothing is
        // lost on the round trip, but it will not match a numeric or string comparison.
        _ => element.GetRawText(),
    };
}
