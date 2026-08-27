using System.Collections.Concurrent;

namespace PubSub.Broker.Core;

/// <summary>
/// Wakes receivers that are long-polling a subscription when a message becomes available.
/// </summary>
/// <remarks>
/// <para>
/// This is an optimisation, never a correctness requirement. A receiver that is never signalled
/// still finds the message on its next poll; the notifier only shortens the wait from "up to one
/// poll interval" to "as soon as the publish commits". Every implementation may therefore drop
/// signals freely — the broker must behave correctly if none is ever delivered.
/// </para>
/// <para>
/// The Redis implementation extends this across broker instances. The in-process one below covers
/// a single instance and is the fallback when Redis is unavailable.
/// </para>
/// </remarks>
public interface IDeliveryNotifier
{
    /// <summary>Signals that a subscription has work available.</summary>
    Task NotifyAsync(int subscriptionId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Waits for a signal on a subscription, or for the timeout to elapse.
    /// </summary>
    /// <returns><c>true</c> if a signal arrived; <c>false</c> on timeout.</returns>
    Task<bool> WaitAsync(int subscriptionId, TimeSpan timeout, CancellationToken cancellationToken = default);
}

/// <summary>
/// An in-process notifier covering receivers attached to this broker instance.
/// </summary>
/// <remarks>
/// Used on its own for single-instance deployments and local development, and as the fallback
/// whenever the distributed notifier is unavailable. Cross-instance wakeups are lost, which costs
/// latency and nothing else.
/// </remarks>
public sealed class InProcessDeliveryNotifier : IDeliveryNotifier
{
    private readonly ConcurrentDictionary<int, TaskCompletionSource> _waiters = new();

    /// <inheritdoc />
    public Task NotifyAsync(int subscriptionId, CancellationToken cancellationToken = default)
    {
        if (_waiters.TryRemove(subscriptionId, out TaskCompletionSource? waiter))
        {
            waiter.TrySetResult();
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<bool> WaitAsync(
        int subscriptionId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        TaskCompletionSource waiter = _waiters.GetOrAdd(
            subscriptionId,
            static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        using CancellationTokenSource timeoutSource = new(timeout);
        using CancellationTokenSource linked =
            CancellationTokenSource.CreateLinkedTokenSource(timeoutSource.Token, cancellationToken);

        try
        {
            await waiter.Task.WaitAsync(linked.Token);
            return true;
        }
        catch (OperationCanceledException) when (timeoutSource.IsCancellationRequested)
        {
            return false;
        }
    }
}

/// <summary>A notifier that signals nothing, leaving receivers to poll.</summary>
/// <remarks>Useful in tests that need to exercise the polling fallback deliberately.</remarks>
public sealed class NullDeliveryNotifier : IDeliveryNotifier
{
    /// <summary>The single shared instance.</summary>
    public static readonly NullDeliveryNotifier Instance = new();

    /// <inheritdoc />
    public Task NotifyAsync(int subscriptionId, CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    /// <inheritdoc />
    public async Task<bool> WaitAsync(
        int subscriptionId,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await Task.Delay(timeout, cancellationToken);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // Timeout elapsed rather than the caller cancelling.
        }

        return false;
    }
}
