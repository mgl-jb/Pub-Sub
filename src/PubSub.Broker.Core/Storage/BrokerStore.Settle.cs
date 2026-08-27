using Microsoft.EntityFrameworkCore;
using PubSub.Abstractions;

namespace PubSub.Broker.Core;

public sealed partial class BrokerStore
{
    /// <summary>Settles a message successfully; it will not be delivered again.</summary>
    public async Task<SettlementResult> CompleteAsync(
        long deliveryId,
        Guid lockToken,
        CancellationToken cancellationToken = default)
    {
        DateTimeOffset now = _time.GetUtcNow();

        int updated = await LockedDelivery(deliveryId, lockToken, now)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(d => d.State, MessageState.Completed)
                    .SetProperty(d => d.SettledAt, now)
                    .SetProperty(d => d.LockToken, (Guid?)null)
                    .SetProperty(d => d.LockedUntil, (DateTimeOffset?)null),
                cancellationToken);

        return await ClassifyAsync(updated, deliveryId, cancellationToken);
    }

    /// <summary>
    /// Returns a message to its subscription for redelivery, dead-lettering it instead when it has
    /// already been delivered as many times as the subscription allows.
    /// </summary>
    /// <param name="deliveryId">The delivery to abandon.</param>
    /// <param name="lockToken">The lock held on it.</param>
    /// <param name="propertiesToModify">Properties to merge before redelivery.</param>
    /// <param name="delay">
    /// Withholds the message for this long before it becomes visible again. A delayed retry keeps
    /// a repeatedly failing message from consuming the whole retry budget in a few milliseconds.
    /// </param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    public async Task<SettlementResult> AbandonAsync(
        long deliveryId,
        Guid lockToken,
        IDictionary<string, object?>? propertiesToModify = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
    {
        DateTimeOffset now = _time.GetUtcNow();

        DeliveryEntity? delivery = await _context.Deliveries
            .Include(d => d.Subscription)
            .FirstOrDefaultAsync(d => d.Id == deliveryId, cancellationToken);

        if (delivery is null)
        {
            return SettlementResult.NotFound;
        }

        if (delivery.LockToken != lockToken || delivery.LockedUntil is null || delivery.LockedUntil <= now)
        {
            return SettlementResult.LockLost;
        }

        int maxDeliveryCount = delivery.Subscription?.MaxDeliveryCount ?? 10;

        if (propertiesToModify is { Count: > 0 })
        {
            Dictionary<string, object?> merged = MessagePropertySerializer.Deserialize(
                delivery.OverriddenPropertiesJson
                ?? (await _context.Messages
                        .AsNoTracking()
                        .Where(m => m.SequenceNumber == delivery.MessageSequenceNumber)
                        .Select(m => m.ApplicationPropertiesJson)
                        .FirstOrDefaultAsync(cancellationToken)));

            foreach (KeyValuePair<string, object?> property in propertiesToModify)
            {
                merged[property.Key] = property.Value;
            }

            delivery.OverriddenPropertiesJson = MessagePropertySerializer.Serialize(merged);
        }

        // The delivery count was already incremented when the message was claimed, so this
        // compares the attempts made so far against the budget rather than anticipating one more.
        if (delivery.DeliveryCount >= maxDeliveryCount)
        {
            delivery.State = MessageState.DeadLettered;
            delivery.DeadLetterReason = DeadLetterReason.MaxDeliveryCountExceeded;
            delivery.DeadLetterDescription =
                $"Delivered {delivery.DeliveryCount} times, reaching the subscription's maximum of " +
                $"{maxDeliveryCount}.";
            delivery.SettledAt = now;

            BrokerLog.DeadLetteringExhausted(
                _logger, deliveryId, delivery.DeliveryCount, delivery.SubscriptionId);
        }
        else
        {
            delivery.State = MessageState.Available;
            delivery.AvailableAt = delay is { } wait ? now.Add(wait) : now;
        }

        delivery.LockToken = null;
        delivery.LockedUntil = null;
        delivery.LockedBy = null;

        await _context.SaveChangesAsync(cancellationToken);

        if (delivery.State == MessageState.Available && delivery.AvailableAt <= now)
        {
            await _notifier.NotifyAsync(delivery.SubscriptionId, cancellationToken);
        }

        return SettlementResult.Settled;
    }

    /// <summary>
    /// Moves a message to the dead-letter queue without further retries.
    /// </summary>
    /// <remarks>
    /// For messages that can never succeed — bad data, a rejected schema — where retrying only
    /// burns the delivery budget and delays the inevitable.
    /// </remarks>
    public async Task<SettlementResult> DeadLetterAsync(
        long deliveryId,
        Guid lockToken,
        string reason,
        string? description = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);

        DateTimeOffset now = _time.GetUtcNow();

        int updated = await LockedDelivery(deliveryId, lockToken, now)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(d => d.State, MessageState.DeadLettered)
                    .SetProperty(d => d.DeadLetterReason, reason)
                    .SetProperty(d => d.DeadLetterDescription, description)
                    .SetProperty(d => d.SettledAt, now)
                    .SetProperty(d => d.LockToken, (Guid?)null)
                    .SetProperty(d => d.LockedUntil, (DateTimeOffset?)null),
                cancellationToken);

        return await ClassifyAsync(updated, deliveryId, cancellationToken);
    }

    /// <summary>
    /// Sets a message aside. It leaves the normal delivery flow and can only be retrieved by its
    /// sequence number.
    /// </summary>
    public async Task<SettlementResult> DeferAsync(
        long deliveryId,
        Guid lockToken,
        CancellationToken cancellationToken = default)
    {
        DateTimeOffset now = _time.GetUtcNow();

        int updated = await LockedDelivery(deliveryId, lockToken, now)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(d => d.State, MessageState.Deferred)
                    .SetProperty(d => d.LockToken, (Guid?)null)
                    .SetProperty(d => d.LockedUntil, (DateTimeOffset?)null),
                cancellationToken);

        return await ClassifyAsync(updated, deliveryId, cancellationToken);
    }

    /// <summary>Extends the peek-lock on a message that is legitimately slow to process.</summary>
    /// <returns>The new expiry, or <c>null</c> when the lock had already been lost.</returns>
    public async Task<DateTimeOffset?> RenewLockAsync(
        long deliveryId,
        Guid lockToken,
        CancellationToken cancellationToken = default)
    {
        DateTimeOffset now = _time.GetUtcNow();

        TimeSpan lockDuration = await _context.Deliveries
            .Where(d => d.Id == deliveryId)
            .Select(d => d.Subscription!.LockDuration)
            .FirstOrDefaultAsync(cancellationToken);

        if (lockDuration == default)
        {
            return null;
        }

        DateTimeOffset renewedUntil = now.Add(lockDuration);

        int updated = await LockedDelivery(deliveryId, lockToken, now)
            .ExecuteUpdateAsync(
                setters => setters.SetProperty(d => d.LockedUntil, renewedUntil),
                cancellationToken);

        return updated == 1 ? renewedUntil : null;
    }

    /// <summary>
    /// Returns dead-lettered messages to their subscription for another attempt, with a fresh
    /// delivery budget.
    /// </summary>
    /// <remarks>
    /// The delivery count is reset because a replay follows a fix — a corrected consumer, a
    /// repaired downstream service — so charging the message for its previous failures would
    /// dead-letter it again almost immediately.
    /// </remarks>
    /// <returns>How many messages were re-enqueued.</returns>
    public async Task<int> ReplayDeadLetteredAsync(
        string topicName,
        string subscriptionName,
        IReadOnlyList<long>? sequenceNumbers = null,
        int maxCount = 100,
        CancellationToken cancellationToken = default)
    {
        SubscriptionEntity subscription =
            await FindSubscriptionAsync(topicName, subscriptionName, cancellationToken);

        DateTimeOffset now = _time.GetUtcNow();

        IQueryable<DeliveryEntity> query = _context.Deliveries
            .Where(d => d.SubscriptionId == subscription.Id
                        && d.State == MessageState.DeadLettered);

        if (sequenceNumbers is { Count: > 0 })
        {
            query = query.Where(d => sequenceNumbers.Contains(d.SequenceNumber));
        }

        List<long> ids = await query
            .OrderBy(d => d.SequenceNumber)
            .Take(maxCount)
            .Select(d => d.Id)
            .ToListAsync(cancellationToken);

        if (ids.Count == 0)
        {
            return 0;
        }

        // A replayed message keeps its original expiry only if that is still in the future;
        // otherwise the sweeper would dead-letter it again before anyone could receive it.
        DateTimeOffset revivedExpiry = now.Add(subscription.DefaultTimeToLive ?? TimeSpan.FromDays(14));

        int replayed = await _context.Deliveries
            .Where(d => ids.Contains(d.Id))
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(d => d.State, MessageState.Available)
                    .SetProperty(d => d.AvailableAt, now)
                    .SetProperty(d => d.DeliveryCount, 0)
                    .SetProperty(d => d.DeadLetterReason, (string?)null)
                    .SetProperty(d => d.DeadLetterDescription, (string?)null)
                    .SetProperty(d => d.SettledAt, (DateTimeOffset?)null)
                    .SetProperty(d => d.LockToken, (Guid?)null)
                    .SetProperty(d => d.LockedUntil, (DateTimeOffset?)null)
                    .SetProperty(d => d.ExpiresAt, d => d.ExpiresAt > now ? d.ExpiresAt : revivedExpiry),
                cancellationToken);

        if (replayed > 0)
        {
            await _notifier.NotifyAsync(subscription.Id, cancellationToken);

            BrokerLog.DeadLettersReplayed(_logger, replayed, topicName, subscriptionName);
        }

        return replayed;
    }

    /// <summary>
    /// Matches a delivery only while the supplied token still holds a live lock on it.
    /// </summary>
    /// <remarks>
    /// Both halves matter: the token proves the caller is the current holder, and the expiry check
    /// rejects a token that was valid but has since lapsed — by which point the message may already
    /// be in another receiver's hands.
    /// </remarks>
    private IQueryable<DeliveryEntity> LockedDelivery(long deliveryId, Guid lockToken, DateTimeOffset now) =>
        _context.Deliveries.Where(d =>
            d.Id == deliveryId
            && d.LockToken == lockToken
            && d.LockedUntil != null
            && d.LockedUntil > now);

    /// <summary>
    /// Turns "no rows updated" into the reason: the delivery is gone, or the lock was lost.
    /// </summary>
    private async Task<SettlementResult> ClassifyAsync(
        int updated,
        long deliveryId,
        CancellationToken cancellationToken)
    {
        if (updated == 1)
        {
            return SettlementResult.Settled;
        }

        bool exists = await _context.Deliveries
            .AsNoTracking()
            .AnyAsync(d => d.Id == deliveryId, cancellationToken);

        return exists ? SettlementResult.LockLost : SettlementResult.NotFound;
    }
}
