using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;

namespace PubSub.Client;

/// <summary>
/// Pumps messages from one subscription into registered handlers.
/// </summary>
/// <remarks>
/// Started as a hosted service, this is the only piece of consuming machinery an application
/// normally touches: handlers deal with payloads, and the receive loop, concurrency, lock renewal,
/// and settlement live here.
/// </remarks>
public sealed class MessageProcessor : IAsyncDisposable
{
    private readonly BrokerHttpClient _broker;
    private readonly MessageDispatcher _dispatcher;
    private readonly MessageProcessorOptions _options;
    private readonly PubSubClientOptions _clientOptions;
    private readonly ILogger<MessageProcessor> _logger;
    private readonly SemaphoreSlim _concurrency;

    private CancellationTokenSource? _stopping;
    private Task? _pump;

    /// <summary>Creates the processor.</summary>
    internal MessageProcessor(
        BrokerHttpClient broker,
        MessageDispatcher dispatcher,
        MessageProcessorOptions options,
        IOptions<PubSubClientOptions> clientOptions,
        ILogger<MessageProcessor> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientOptions);

        _broker = broker;
        _dispatcher = dispatcher;
        _options = options;
        _clientOptions = clientOptions.Value;
        _logger = logger;
        _concurrency = new SemaphoreSlim(Math.Max(1, options.MaxConcurrentCalls));
    }

    /// <summary>Starts the receive loop.</summary>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_pump is not null)
        {
            return Task.CompletedTask;
        }

        _stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _pump = Task.Run(() => PumpAsync(_stopping.Token), CancellationToken.None);

        ClientLog.ProcessorStarted(
            _logger, _options.Topic, _options.Subscription, _options.MaxConcurrentCalls);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Stops the receive loop and waits for in-flight handlers to finish.
    /// </summary>
    /// <remarks>
    /// Draining rather than abandoning matters: a message whose handler is cut off mid-flight is
    /// redelivered, so a clean stop turns "some work repeats on every deploy" into "none does".
    /// </remarks>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_stopping is null || _pump is null)
        {
            return;
        }

        ClientLog.ProcessorStopping(_logger, _options.Topic, _options.Subscription);

        await _stopping.CancelAsync();

        try
        {
            await _pump.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Shutdown ran out of time; the unsettled messages return to the subscription.
        }

        _pump = null;
    }

    private async Task PumpAsync(CancellationToken cancellationToken)
    {
        List<Task> inFlight = [];

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                IReadOnlyList<WireReceivedMessage> received = await _broker.ReceiveAsync(
                    _options.Topic,
                    _options.Subscription,
                    new WireReceiveRequest
                    {
                        MaxMessages = Math.Max(1, _options.PrefetchCount),
                        MaxWaitTime = _options.MaxWaitTime,
                        ReceiverId = _clientOptions.ReceiverId,
                    },
                    _options.ProcessDeadLetterQueue,
                    cancellationToken);

                foreach (WireReceivedMessage message in received)
                {
                    await _concurrency.WaitAsync(cancellationToken);
                    inFlight.Add(ProcessOneAsync(message, cancellationToken));
                }

                inFlight.RemoveAll(t => t.IsCompleted);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // The broker being briefly unreachable must not end the consumer; backing off and
                // retrying is the difference between a blip and an outage that needs a restart.
                ClientLog.ReceiveFailed(_logger, ex, _options.Topic, _options.Subscription);

                try
                {
                    await Task.Delay(_options.ErrorBackoff, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        // Let in-flight handlers finish rather than cutting them off mid-message.
        await Task.WhenAll(inFlight);
    }

    private async Task ProcessOneAsync(WireReceivedMessage message, CancellationToken cancellationToken)
    {
        using CancellationTokenSource renewal = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        Task renewalTask = RenewLockAsync(message, renewal.Token);

        try
        {
            await _dispatcher.DispatchAsync(
                _options.Topic,
                _options.Subscription,
                message,
                _options.AutoComplete,
                cancellationToken);
        }
        finally
        {
            await renewal.CancelAsync();

            try
            {
                await renewalTask;
            }
            catch (OperationCanceledException)
            {
                // Expected: the handler finished, so renewal was cancelled.
            }

            _concurrency.Release();
        }
    }

    /// <summary>
    /// Keeps a message's lock alive while its handler is still working.
    /// </summary>
    /// <remarks>
    /// Renewing at half the remaining lock leaves room for a failed attempt before expiry. The
    /// ceiling on total renewal is what stops a hung handler from holding a message indefinitely —
    /// past it the lock lapses and the message goes back for another consumer to try.
    /// </remarks>
    private async Task RenewLockAsync(WireReceivedMessage message, CancellationToken cancellationToken)
    {
        DateTimeOffset renewUntil = DateTimeOffset.UtcNow.Add(_options.MaxAutoLockRenewalDuration);
        DateTimeOffset lockedUntil = message.LockedUntil;

        while (!cancellationToken.IsCancellationRequested)
        {
            TimeSpan remaining = lockedUntil - DateTimeOffset.UtcNow;
            TimeSpan delay = remaining / 2;

            if (delay < TimeSpan.FromMilliseconds(100))
            {
                delay = TimeSpan.FromMilliseconds(100);
            }

            try
            {
                await Task.Delay(delay, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (DateTimeOffset.UtcNow >= renewUntil)
            {
                ClientLog.LockRenewalStopped(
                    _logger,
                    message.DeliveryId,
                    "the maximum renewal duration was reached");
                return;
            }

            try
            {
                lockedUntil = await _broker.RenewLockAsync(
                    _options.Topic,
                    _options.Subscription,
                    message.DeliveryId,
                    message.LockToken,
                    cancellationToken);
            }
            catch (MessageLockLostException)
            {
                ClientLog.LockRenewalStopped(_logger, message.DeliveryId, "the lock was already lost");
                return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex) when (ex is PubSubException)
            {
                ClientLog.LockRenewalStopped(_logger, message.DeliveryId, ex.Message);
                return;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _stopping?.Dispose();
        _concurrency.Dispose();
    }
}
