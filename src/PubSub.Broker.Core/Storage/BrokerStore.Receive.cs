using Microsoft.EntityFrameworkCore;
using PubSub.Abstractions;

namespace PubSub.Broker.Core;

public sealed partial class BrokerStore
{
    /// <summary>
    /// Claims up to <see cref="ReceiveRequest.MaxMessages"/> messages, waiting up to
    /// <see cref="ReceiveRequest.MaxWaitTime"/> for one to arrive.
    /// </summary>
    /// <remarks>
    /// Long-polling: the first claim is attempted immediately, and only if it comes back empty does
    /// the call wait. Waiting is interrupted by a publish notification where one is available, and
    /// otherwise falls back to re-checking on an interval — so the wait costs latency at worst,
    /// never messages.
    /// </remarks>
    public async Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(
        ReceiveRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        SubscriptionEntity subscription =
            await FindSubscriptionAsync(request.Topic, request.Subscription, cancellationToken);

        if (subscription.ReceivingSuspended)
        {
            throw new InvalidOperationForStateException(
                $"Receiving from subscription '{request.Subscription}' is suspended.");
        }

        if (subscription.RequiresSession && request.SessionId is null && !request.FromDeadLetterQueue)
        {
            throw new InvalidOperationForStateException(
                $"Subscription '{request.Subscription}' requires a session. Accept a session first, " +
                "then receive within it.");
        }

        int maxMessages = Math.Clamp(request.MaxMessages, 1, _options.MaxReceiveBatchSize);

        TimeSpan maxWait = request.MaxWaitTime > _options.MaxLongPollDuration
            ? _options.MaxLongPollDuration
            : request.MaxWaitTime;

        DateTimeOffset deadline = _time.GetUtcNow().Add(maxWait);

        while (true)
        {
            IReadOnlyList<ReceivedMessage> claimed = await DeliveryClaim.ClaimAsync(
                _context,
                subscription.Id,
                maxMessages,
                subscription.LockDuration,
                _time.GetUtcNow(),
                request.SessionId,
                request.ReceiverId,
                request.FromDeadLetterQueue,
                _options.DatabaseCommandTimeout,
                cancellationToken);

            if (claimed.Count > 0)
            {
                return claimed;
            }

            DateTimeOffset now = _time.GetUtcNow();
            if (now >= deadline)
            {
                return [];
            }

            TimeSpan remaining = deadline - now;
            TimeSpan waitSlice = remaining < _options.LongPollInterval
                ? remaining
                : _options.LongPollInterval;

            // A signal shortens the wait; its absence does not lengthen it beyond the interval,
            // which is what keeps a lost notification a latency issue rather than a stall.
            await _notifier.WaitAsync(subscription.Id, waitSlice, cancellationToken);
        }
    }

    /// <summary>
    /// Retrieves deferred messages by sequence number, locking them like an ordinary receive.
    /// </summary>
    /// <remarks>
    /// Deferred messages are invisible to the normal claim, so this is the only way back to one.
    /// A caller that deferred a message without recording its sequence number has stranded it
    /// until its time to live expires.
    /// </remarks>
    public async Task<IReadOnlyList<ReceivedMessage>> ReceiveDeferredAsync(
        string topicName,
        string subscriptionName,
        IReadOnlyList<long> sequenceNumbers,
        string? receiverId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sequenceNumbers);

        if (sequenceNumbers.Count == 0)
        {
            return [];
        }

        SubscriptionEntity subscription =
            await FindSubscriptionAsync(topicName, subscriptionName, cancellationToken);

        DateTimeOffset now = _time.GetUtcNow();
        Guid lockToken = Guid.NewGuid();
        DateTimeOffset lockedUntil = now.Add(subscription.LockDuration);

        int updated = await _context.Deliveries
            .Where(d => d.SubscriptionId == subscription.Id
                        && d.State == MessageState.Deferred
                        && sequenceNumbers.Contains(d.SequenceNumber))
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(d => d.State, MessageState.Locked)
                    .SetProperty(d => d.LockToken, lockToken)
                    .SetProperty(d => d.LockedUntil, lockedUntil)
                    .SetProperty(d => d.LockedBy, receiverId)
                    .SetProperty(d => d.DeliveryCount, d => d.DeliveryCount + 1),
                cancellationToken);

        if (updated == 0)
        {
            return [];
        }

        List<DeliveryEntity> deliveries = await _context.Deliveries
            .AsNoTracking()
            .Include(d => d.Message)
            .Where(d => d.SubscriptionId == subscription.Id && d.LockToken == lockToken)
            .OrderBy(d => d.SequenceNumber)
            .ToListAsync(cancellationToken);

        return [.. deliveries.Select(d => ToReceivedMessage(d, lockToken, lockedUntil))];
    }

    /// <summary>
    /// Cancels a scheduled message that has not yet become visible.
    /// </summary>
    /// <remarks>
    /// Only deliveries still waiting for their scheduled time are removed. Once a message has
    /// become visible it may already be in a receiver's hands, so cancelling it would be a
    /// retraction the broker cannot honour — the caller is told it lost the race instead.
    /// </remarks>
    /// <returns><c>true</c> when at least one pending delivery was cancelled.</returns>
    public async Task<bool> CancelScheduledAsync(
        string topicName,
        long sequenceNumber,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        DateTimeOffset now = _time.GetUtcNow();

        int cancelled = await _context.Deliveries
            .Where(d => d.SequenceNumber == sequenceNumber
                        && d.Message!.Topic!.Name == topicName
                        && d.State == MessageState.Available
                        && d.AvailableAt > now)
            .ExecuteDeleteAsync(cancellationToken);

        return cancelled > 0;
    }

    private static ReceivedMessage ToReceivedMessage(
        DeliveryEntity delivery,
        Guid lockToken,
        DateTimeOffset lockedUntil)
    {
        MessageEntity message = delivery.Message
            ?? throw new InvalidOperationException(
                $"Delivery {delivery.Id} has no message loaded.");

        MessageEnvelope envelope = new()
        {
            MessageId = message.MessageId,
            CorrelationId = message.CorrelationId,
            Subject = message.Subject,
            ContentType = message.ContentType,
            Body = message.Body,
            ApplicationProperties = MessagePropertySerializer.Deserialize(
                delivery.OverriddenPropertiesJson ?? message.ApplicationPropertiesJson),
            SessionId = message.SessionId,
            ReplyTo = message.ReplyTo,
            ReplyToSessionId = message.ReplyToSessionId,
            To = message.To,
            SequenceNumber = message.SequenceNumber,
            EnqueuedTime = message.EnqueuedTime,
            DeliveryCount = delivery.DeliveryCount,
            LockToken = lockToken,
            LockedUntil = lockedUntil,
            State = MessageState.Locked,
            DeadLetterReason = delivery.DeadLetterReason,
            DeadLetterDescription = delivery.DeadLetterDescription,
        };

        return new ReceivedMessage
        {
            DeliveryId = delivery.Id,
            LockToken = lockToken,
            LockedUntil = lockedUntil,
            Message = envelope,
        };
    }

    private async Task<SubscriptionEntity> FindSubscriptionAsync(
        string topicName,
        string subscriptionName,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriptionName);

        SubscriptionEntity? subscription = await _context.Subscriptions
            .AsNoTracking()
            .Include(s => s.Topic)
            .FirstOrDefaultAsync(
                s => s.Topic!.Name == topicName && s.Name == subscriptionName,
                cancellationToken);

        return subscription
               ?? throw new EntityNotFoundException("Subscription", $"{topicName}/{subscriptionName}");
    }
}
