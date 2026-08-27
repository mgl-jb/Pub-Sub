using Microsoft.Extensions.Logging;

namespace PubSub.Broker.Core;

/// <summary>
/// Source-generated log methods for the broker.
/// </summary>
/// <remarks>
/// Publish and receive run per message, so logging on those paths must not allocate when the level
/// is disabled. The generator emits the level check and strongly typed parameters, which avoids
/// both the boxing and the message formatting that the extension-method overloads incur.
/// </remarks>
internal static partial class BrokerLog
{
    [LoggerMessage(
        EventId = 1000,
        Level = LogLevel.Debug,
        Message = "Suppressed duplicate message '{MessageId}' on topic '{Topic}'; the original was sequence {SequenceNumber}.")]
    public static partial void DuplicateSuppressed(
        ILogger logger,
        string messageId,
        string topic,
        long sequenceNumber);

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Error,
        Message = "Rule evaluation failed for subscription '{Subscription}' on message {SequenceNumber}.")]
    public static partial void RuleEvaluationFailed(
        ILogger logger,
        Exception exception,
        string subscription,
        long sequenceNumber);

    [LoggerMessage(
        EventId = 1002,
        Level = LogLevel.Warning,
        Message = "Dead-lettering delivery {DeliveryId} after {DeliveryCount} attempts on subscription {SubscriptionId}.")]
    public static partial void DeadLetteringExhausted(
        ILogger logger,
        long deliveryId,
        int deliveryCount,
        int subscriptionId);

    [LoggerMessage(
        EventId = 1003,
        Level = LogLevel.Information,
        Message = "Replayed {Count} dead-lettered messages on {Topic}/{Subscription}.")]
    public static partial void DeadLettersReplayed(
        ILogger logger,
        int count,
        string topic,
        string subscription);

    [LoggerMessage(
        EventId = 1004,
        Level = LogLevel.Error,
        Message = "Rule '{RuleName}' on subscription {SubscriptionId} failed to compile and will be ignored: {Reason}")]
    public static partial void RuleCompilationFailed(
        ILogger logger,
        Exception exception,
        string ruleName,
        int subscriptionId,
        string reason);

    [LoggerMessage(
        EventId = 1005,
        Level = LogLevel.Information,
        Message = "Sweeper released {Count} expired message locks.")]
    public static partial void ExpiredLocksReleased(ILogger logger, int count);

    [LoggerMessage(
        EventId = 1006,
        Level = LogLevel.Information,
        Message = "Sweeper dead-lettered {Count} messages whose time to live expired.")]
    public static partial void ExpiredMessagesDeadLettered(ILogger logger, int count);

    [LoggerMessage(
        EventId = 1007,
        Level = LogLevel.Information,
        Message = "Sweeper released {Count} expired session locks.")]
    public static partial void ExpiredSessionsReleased(ILogger logger, int count);

    [LoggerMessage(
        EventId = 1008,
        Level = LogLevel.Debug,
        Message = "Sweeper pruned {Deliveries} settled deliveries, {Messages} messages, and {DedupEntries} duplicate-detection records.")]
    public static partial void PrunedRows(ILogger logger, int deliveries, int messages, int dedupEntries);

    [LoggerMessage(
        EventId = 1009,
        Level = LogLevel.Error,
        Message = "The sweeper pass failed; it will retry on the next interval.")]
    public static partial void SweepFailed(ILogger logger, Exception exception);
}
