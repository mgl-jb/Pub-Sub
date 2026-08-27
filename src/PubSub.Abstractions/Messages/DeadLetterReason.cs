namespace PubSub.Abstractions;

/// <summary>
/// Why a delivery was dead-lettered. Stored alongside a free-text description so operators
/// triaging the dead-letter queue can filter by cause before reading individual messages.
/// </summary>
public static class DeadLetterReason
{
    /// <summary>The delivery was attempted more times than the subscription's maximum.</summary>
    public const string MaxDeliveryCountExceeded = "MaxDeliveryCountExceeded";

    /// <summary>The message's time to live elapsed before it was settled.</summary>
    public const string TimeToLiveExpired = "TimeToLiveExpired";

    /// <summary>A subscription rule threw while being evaluated against the message.</summary>
    public const string FilterEvaluationError = "FilterEvaluationError";

    /// <summary>The receiving application dead-lettered the message explicitly.</summary>
    public const string ApplicationError = "ApplicationError";

    /// <summary>The message could not be deserialized into the handler's expected type.</summary>
    public const string DeserializationError = "DeserializationError";
}
