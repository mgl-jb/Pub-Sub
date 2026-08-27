namespace PubSub.Abstractions;

/// <summary>Base type for every error the messaging system raises.</summary>
public class PubSubException : Exception
{
    /// <summary>Creates an instance.</summary>
    public PubSubException()
    {
    }

    /// <summary>Creates an instance with a message.</summary>
    public PubSubException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an instance with a message and an inner exception.</summary>
    public PubSubException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Whether retrying the same operation could succeed. Callers use this to decide between
    /// backing off and giving up; the client library's resilience pipeline keys off it.
    /// </summary>
    public virtual bool IsTransient => false;
}

/// <summary>
/// The peek-lock on a message expired or was taken over before settlement.
/// </summary>
/// <remarks>
/// This is normal under load, not a defect: processing outran the lock duration. The message has
/// gone back to the subscription and will be redelivered, so the work may be done twice — which is
/// exactly why consumers must be idempotent. Either shorten processing, raise the lock duration,
/// or enable automatic lock renewal.
/// </remarks>
public sealed class MessageLockLostException : PubSubException
{
    /// <summary>Creates an instance for the given lock token.</summary>
    public MessageLockLostException(Guid lockToken)
        : base($"The lock '{lockToken}' has expired or is no longer held. The message has been " +
               "returned to the subscription and may already have been redelivered.")
        => LockToken = lockToken;

    /// <summary>Creates an instance with a custom message.</summary>
    public MessageLockLostException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an instance with a custom message and inner exception.</summary>
    public MessageLockLostException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates an instance.</summary>
    public MessageLockLostException()
        : base("The message lock has expired or is no longer held.")
    {
    }

    /// <summary>The token that is no longer valid, when known.</summary>
    public Guid? LockToken { get; }
}

/// <summary>The lock on a session expired or was taken over.</summary>
/// <remarks>
/// The session's remaining messages are available to other consumers. Ordering within the session
/// is preserved — a new consumer resumes from the earliest unsettled message.
/// </remarks>
public sealed class SessionLockLostException : PubSubException
{
    /// <summary>Creates an instance for the given session.</summary>
    public SessionLockLostException(string sessionId)
        : base($"The lock on session '{sessionId}' has expired or is no longer held.")
        => SessionId = sessionId;

    /// <summary>Creates an instance with a custom message.</summary>
    public SessionLockLostException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates an instance.</summary>
    public SessionLockLostException()
        : base("The session lock has expired or is no longer held.")
    {
    }

    /// <summary>The session whose lock was lost, when known.</summary>
    public string? SessionId { get; }
}

/// <summary>A topic, subscription, rule, or message could not be found.</summary>
public sealed class EntityNotFoundException : PubSubException
{
    /// <summary>Creates an instance naming the entity.</summary>
    public EntityNotFoundException(string entityType, string name)
        : base($"{entityType} '{name}' was not found.")
    {
        EntityType = entityType;
        Name = name;
    }

    /// <summary>Creates an instance with a custom message.</summary>
    public EntityNotFoundException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an instance with a custom message and inner exception.</summary>
    public EntityNotFoundException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates an instance.</summary>
    public EntityNotFoundException()
        : base("The requested entity was not found.")
    {
    }

    /// <summary>The kind of entity, for example <c>Topic</c>.</summary>
    public string? EntityType { get; }

    /// <summary>The name that was looked up.</summary>
    public string? Name { get; }
}

/// <summary>An entity with the same name already exists.</summary>
public sealed class EntityAlreadyExistsException : PubSubException
{
    /// <summary>Creates an instance naming the entity.</summary>
    public EntityAlreadyExistsException(string entityType, string name)
        : base($"{entityType} '{name}' already exists.")
    {
        EntityType = entityType;
        Name = name;
    }

    /// <summary>Creates an instance with a custom message.</summary>
    public EntityAlreadyExistsException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an instance with a custom message and inner exception.</summary>
    public EntityAlreadyExistsException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates an instance.</summary>
    public EntityAlreadyExistsException()
        : base("The entity already exists.")
    {
    }

    /// <summary>The kind of entity, for example <c>Subscription</c>.</summary>
    public string? EntityType { get; }

    /// <summary>The name that collided.</summary>
    public string? Name { get; }
}

/// <summary>A subscription filter expression could not be parsed.</summary>
public sealed class FilterSyntaxException : PubSubException
{
    /// <summary>Creates an instance describing where parsing failed.</summary>
    public FilterSyntaxException(string message, int position)
        : base($"{message} (at position {position})")
        => Position = position;

    /// <summary>Creates an instance with a message.</summary>
    public FilterSyntaxException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an instance with a message and inner exception.</summary>
    public FilterSyntaxException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates an instance.</summary>
    public FilterSyntaxException()
        : base("The filter expression could not be parsed.")
    {
    }

    /// <summary>Zero-based offset into the expression where the error was detected.</summary>
    public int? Position { get; }
}

/// <summary>The broker rejected an operation as invalid for the entity's current state.</summary>
public sealed class InvalidOperationForStateException : PubSubException
{
    /// <summary>Creates an instance with a message.</summary>
    public InvalidOperationForStateException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an instance with a message and inner exception.</summary>
    public InvalidOperationForStateException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates an instance.</summary>
    public InvalidOperationForStateException()
        : base("The operation is not valid for the entity's current state.")
    {
    }
}

/// <summary>
/// The broker was unreachable or returned a retryable failure.
/// </summary>
public sealed class BrokerUnavailableException : PubSubException
{
    /// <summary>Creates an instance with a message.</summary>
    public BrokerUnavailableException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an instance with a message and inner exception.</summary>
    public BrokerUnavailableException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates an instance.</summary>
    public BrokerUnavailableException()
        : base("The broker is unavailable.")
    {
    }

    /// <inheritdoc />
    public override bool IsTransient => true;
}
