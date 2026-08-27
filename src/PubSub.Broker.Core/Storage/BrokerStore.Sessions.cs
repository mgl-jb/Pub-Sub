using Microsoft.EntityFrameworkCore;
using PubSub.Abstractions;

namespace PubSub.Broker.Core;

public sealed partial class BrokerStore
{
    /// <summary>
    /// Takes exclusive ownership of a session so its messages can be processed in order.
    /// </summary>
    /// <param name="topicName">The topic.</param>
    /// <param name="subscriptionName">The subscription.</param>
    /// <param name="sessionId">
    /// The session to accept, or <c>null</c> to accept whichever session has the oldest unprocessed
    /// message and is not already locked.
    /// </param>
    /// <param name="receiverId">Identifies the holder in diagnostics.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>The accepted session, or <c>null</c> when none was available.</returns>
    /// <remarks>
    /// Ordering comes from exclusivity rather than from any ordering in the claim: because only the
    /// lock holder may claim that session's deliveries, its messages are processed one at a time in
    /// sequence order. Two consumers taking adjacent messages from one session concurrently is
    /// exactly what this prevents.
    /// </remarks>
    public async Task<AcceptedSession?> AcceptSessionAsync(
        string topicName,
        string subscriptionName,
        string? sessionId = null,
        string? receiverId = null,
        CancellationToken cancellationToken = default)
    {
        SubscriptionEntity subscription =
            await FindSubscriptionAsync(topicName, subscriptionName, cancellationToken);

        if (!subscription.RequiresSession)
        {
            throw new InvalidOperationForStateException(
                $"Subscription '{subscriptionName}' is not session-enabled.");
        }

        DateTimeOffset now = _time.GetUtcNow();

        string? target = sessionId
                         ?? await FindNextAvailableSessionAsync(subscription.Id, now, cancellationToken);

        if (target is null)
        {
            return null;
        }

        // A lapsed lock is reclaimed rather than blocking the session forever: the previous holder
        // crashed or stalled, and its unsettled messages have already returned to Available.
        SessionLockEntity? existing = await _context.SessionLocks
            .FirstOrDefaultAsync(
                s => s.SubscriptionId == subscription.Id && s.SessionId == target,
                cancellationToken);

        Guid lockToken = Guid.NewGuid();
        DateTimeOffset lockedUntil = now.Add(subscription.SessionLockDuration);

        if (existing is not null)
        {
            if (existing.LockedUntil > now)
            {
                return null;
            }

            existing.LockToken = lockToken;
            existing.LockedUntil = lockedUntil;
            existing.LockedBy = receiverId;
            existing.AcquiredAt = now;
        }
        else
        {
            _context.SessionLocks.Add(new SessionLockEntity
            {
                SubscriptionId = subscription.Id,
                SessionId = target,
                LockToken = lockToken,
                LockedUntil = lockedUntil,
                LockedBy = receiverId,
                AcquiredAt = now,
            });
        }

        try
        {
            await _context.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateException)
        {
            // The unique index on (SubscriptionId, SessionId) is what actually enforces
            // exclusivity. Two consumers racing to accept the same session both reach this insert
            // and exactly one succeeds; losing the race is an ordinary outcome, not an error.
            _context.ChangeTracker.Clear();
            return null;
        }

        byte[]? state = existing?.State;

        return new AcceptedSession
        {
            SessionId = target,
            LockToken = lockToken,
            LockedUntil = lockedUntil,
            State = state,
        };
    }

    /// <summary>Extends a session lock while its holder is still working.</summary>
    /// <returns>The new expiry, or <c>null</c> when the lock had already been lost.</returns>
    public async Task<DateTimeOffset?> RenewSessionLockAsync(
        string topicName,
        string subscriptionName,
        string sessionId,
        Guid lockToken,
        CancellationToken cancellationToken = default)
    {
        SubscriptionEntity subscription =
            await FindSubscriptionAsync(topicName, subscriptionName, cancellationToken);

        DateTimeOffset now = _time.GetUtcNow();
        DateTimeOffset renewedUntil = now.Add(subscription.SessionLockDuration);

        int updated = await _context.SessionLocks
            .Where(s => s.SubscriptionId == subscription.Id
                        && s.SessionId == sessionId
                        && s.LockToken == lockToken
                        && s.LockedUntil > now)
            .ExecuteUpdateAsync(
                setters => setters.SetProperty(s => s.LockedUntil, renewedUntil),
                cancellationToken);

        return updated == 1 ? renewedUntil : null;
    }

    /// <summary>Releases a session so another consumer can take it.</summary>
    public async Task<bool> ReleaseSessionAsync(
        string topicName,
        string subscriptionName,
        string sessionId,
        Guid lockToken,
        CancellationToken cancellationToken = default)
    {
        SubscriptionEntity subscription =
            await FindSubscriptionAsync(topicName, subscriptionName, cancellationToken);

        DateTimeOffset now = _time.GetUtcNow();

        // Expiring the lock rather than deleting the row preserves the session state, which a
        // consumer resuming the session is entitled to read back.
        int updated = await _context.SessionLocks
            .Where(s => s.SubscriptionId == subscription.Id
                        && s.SessionId == sessionId
                        && s.LockToken == lockToken)
            .ExecuteUpdateAsync(
                setters => setters
                    .SetProperty(s => s.LockedUntil, now)
                    .SetProperty(s => s.LockedBy, (string?)null),
                cancellationToken);

        if (updated == 1)
        {
            await _notifier.NotifyAsync(subscription.Id, cancellationToken);
        }

        return updated == 1;
    }

    /// <summary>Reads the state a consumer stored against a session.</summary>
    public async Task<byte[]?> GetSessionStateAsync(
        string topicName,
        string subscriptionName,
        string sessionId,
        CancellationToken cancellationToken = default)
    {
        SubscriptionEntity subscription =
            await FindSubscriptionAsync(topicName, subscriptionName, cancellationToken);

        return await _context.SessionLocks
            .AsNoTracking()
            .Where(s => s.SubscriptionId == subscription.Id && s.SessionId == sessionId)
            .Select(s => s.State)
            .FirstOrDefaultAsync(cancellationToken);
    }

    /// <summary>
    /// Stores state against a session, letting a handler checkpoint its progress without a
    /// separate store.
    /// </summary>
    /// <returns><c>false</c> when the session lock has been lost.</returns>
    public async Task<bool> SetSessionStateAsync(
        string topicName,
        string subscriptionName,
        string sessionId,
        Guid lockToken,
        byte[]? state,
        CancellationToken cancellationToken = default)
    {
        SubscriptionEntity subscription =
            await FindSubscriptionAsync(topicName, subscriptionName, cancellationToken);

        DateTimeOffset now = _time.GetUtcNow();

        int updated = await _context.SessionLocks
            .Where(s => s.SubscriptionId == subscription.Id
                        && s.SessionId == sessionId
                        && s.LockToken == lockToken
                        && s.LockedUntil > now)
            .ExecuteUpdateAsync(
                setters => setters.SetProperty(s => s.State, state),
                cancellationToken);

        return updated == 1;
    }

    /// <summary>
    /// Finds the session with the oldest deliverable message that no consumer currently holds.
    /// </summary>
    private async Task<string?> FindNextAvailableSessionAsync(
        int subscriptionId,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        return await _context.Deliveries
            .AsNoTracking()
            .Where(d => d.SubscriptionId == subscriptionId
                        && d.State == MessageState.Available
                        && d.AvailableAt <= now
                        && d.ExpiresAt > now
                        && d.SessionId != null
                        && !_context.SessionLocks.Any(
                            s => s.SubscriptionId == subscriptionId
                                 && s.SessionId == d.SessionId
                                 && s.LockedUntil > now))
            .OrderBy(d => d.SequenceNumber)
            .Select(d => d.SessionId)
            .FirstOrDefaultAsync(cancellationToken);
    }
}
