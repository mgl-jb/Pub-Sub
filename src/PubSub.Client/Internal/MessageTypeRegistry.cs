using System.Collections.Concurrent;

namespace PubSub.Client;

/// <summary>
/// Maps CLR payload types to the subject a message carries on the wire.
/// </summary>
/// <remarks>
/// Routing on a stable subject string rather than an assembly-qualified type name means a producer
/// and consumer can rename or relocate their own classes, or be written in different languages,
/// without breaking the contract between them.
/// </remarks>
public sealed class MessageTypeRegistry
{
    private readonly ConcurrentDictionary<Type, string> _subjectsByType = new();
    private readonly ConcurrentDictionary<string, Type> _typesBySubject = new(StringComparer.Ordinal);

    /// <summary>Registers a payload type under a subject, defaulting to the type's simple name.</summary>
    public MessageTypeRegistry Register<T>(string? subject = null)
    {
        string resolved = subject ?? typeof(T).Name;
        _subjectsByType[typeof(T)] = resolved;
        _typesBySubject[resolved] = typeof(T);
        return this;
    }

    /// <summary>Returns the subject for a type, falling back to its simple name.</summary>
    public string SubjectFor(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return _subjectsByType.TryGetValue(type, out string? subject) ? subject : type.Name;
    }

    /// <summary>Returns the type registered for a subject, if any.</summary>
    public Type? TypeFor(string subject) =>
        _typesBySubject.TryGetValue(subject, out Type? type) ? type : null;
}
