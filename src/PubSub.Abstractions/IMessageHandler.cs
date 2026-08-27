namespace PubSub.Abstractions;

/// <summary>
/// Handles messages of one type from a subscription. Register implementations with the client
/// library; one instance is resolved per message from a dedicated dependency injection scope.
/// </summary>
/// <typeparam name="T">The deserialized payload type.</typeparam>
/// <remarks>
/// <para>
/// Delivery is at-least-once. A handler will occasionally see the same message twice — after a
/// lock expiry, a consumer restart, or a settlement that succeeded but whose acknowledgement was
/// lost. Handlers must therefore be idempotent. The simplest route is a naturally idempotent
/// write (an upsert keyed on a business identifier); where that is not possible, wrap the handler
/// with the inbox decorator from <c>PubSub.Outbox</c>.
/// </para>
/// <para>
/// Returning normally completes the message. Throwing abandons it, so it is redelivered until the
/// subscription's maximum delivery count is reached and it is dead-lettered. To dead-letter
/// immediately — for a message that will never succeed, such as one that fails validation — call
/// <see cref="MessageContext{T}.DeadLetterAsync"/> rather than throwing.
/// </para>
/// </remarks>
public interface IMessageHandler<T>
{
    /// <summary>Processes one message.</summary>
    /// <param name="context">The payload plus its metadata and settlement operations.</param>
    /// <param name="cancellationToken">
    /// Signalled when the processor is shutting down. Honour it so in-flight work stops promptly;
    /// unsettled messages return to the subscription and are redelivered.
    /// </param>
    Task HandleAsync(MessageContext<T> context, CancellationToken cancellationToken = default);
}
