using Microsoft.Extensions.Logging;

namespace PubSub.Client;

/// <summary>
/// Source-generated log methods for the client.
/// </summary>
/// <remarks>
/// These sit on the per-message path, so the generator's level check and strongly typed parameters
/// matter: the extension-method overloads box every argument whether or not the level is enabled.
/// </remarks>
internal static partial class ClientLog
{
    [LoggerMessage(
        EventId = 2000,
        Level = LogLevel.Warning,
        Message = "No handler is registered for subject '{Subject}' on {Topic}/{Subscription}; dead-lettering message {MessageId}.")]
    public static partial void NoHandlerRegistered(
        ILogger logger, string? subject, string topic, string subscription, string messageId);

    [LoggerMessage(
        EventId = 2001,
        Level = LogLevel.Error,
        Message = "Handling message {MessageId} on {Topic}/{Subscription} failed on attempt {DeliveryCount}.")]
    public static partial void HandlerFailed(
        ILogger logger, Exception exception, string messageId, string topic, string subscription, int deliveryCount);

    [LoggerMessage(
        EventId = 2002,
        Level = LogLevel.Debug,
        Message = "The lock on message {DeliveryId} had already lapsed when abandoning it.")]
    public static partial void LockAlreadyLapsed(ILogger logger, long deliveryId);

    [LoggerMessage(
        EventId = 2003,
        Level = LogLevel.Warning,
        Message = "Failed to abandon message {DeliveryId}.")]
    public static partial void AbandonFailed(ILogger logger, Exception exception, long deliveryId);

    [LoggerMessage(
        EventId = 2004,
        Level = LogLevel.Error,
        Message = "Receiving from {Topic}/{Subscription} failed; retrying after a pause.")]
    public static partial void ReceiveFailed(
        ILogger logger, Exception exception, string topic, string subscription);

    [LoggerMessage(
        EventId = 2005,
        Level = LogLevel.Information,
        Message = "Processor started for {Topic}/{Subscription} with concurrency {Concurrency}.")]
    public static partial void ProcessorStarted(
        ILogger logger, string topic, string subscription, int concurrency);

    [LoggerMessage(
        EventId = 2006,
        Level = LogLevel.Information,
        Message = "Processor stopping for {Topic}/{Subscription}.")]
    public static partial void ProcessorStopping(ILogger logger, string topic, string subscription);

    [LoggerMessage(
        EventId = 2007,
        Level = LogLevel.Debug,
        Message = "Lock renewal for message {DeliveryId} stopped: {Reason}")]
    public static partial void LockRenewalStopped(ILogger logger, long deliveryId, string reason);

    [LoggerMessage(
        EventId = 2008,
        Level = LogLevel.Debug,
        Message = "Session '{SessionId}' released after being idle.")]
    public static partial void SessionReleased(ILogger logger, string sessionId);

    [LoggerMessage(
        EventId = 2009,
        Level = LogLevel.Warning,
        Message = "Lost the lock on session '{SessionId}'; its remaining messages return to the subscription.")]
    public static partial void SessionLockLost(ILogger logger, string sessionId);
}
