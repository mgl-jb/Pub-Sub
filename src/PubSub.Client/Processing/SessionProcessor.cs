using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;

namespace PubSub.Client;

/// <summary>
/// Pumps session-enabled subscriptions, processing each session's messages strictly in order.
/// </summary>
/// <remarks>
/// Concurrency here is across sessions, never within one. That is the whole bargain sessions make:
/// throughput scales with the number of distinct session keys, and messages sharing a key are
/// handled one at a time. A session key too coarse — one per tenant rather than one per entity —
/// serialises far more than intended.
/// </remarks>
public sealed class SessionProcessor : IAsyncDisposable
{
    private readonly BrokerHttpClient _broker;
    private readonly MessageDispatcher _dispatcher;
    private readonly SessionProcessorOptions _options;
    private readonly PubSubClientOptions _clientOptions;
    private readonly ILogger<SessionProcessor> _logger;

    private CancellationTokenSource? _stopping;
    private Task[]? _workers;

    /// <summary>Creates the processor.</summary>
    internal SessionProcessor(
        BrokerHttpClient broker,
        MessageDispatcher dispatcher,
        SessionProcessorOptions options,
        IOptions<PubSubClientOptions> clientOptions,
        ILogger<SessionProcessor> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientOptions);

        _broker = broker;
        _dispatcher = dispatcher;
        _options = options;
        _clientOptions = clientOptions.Value;
        _logger = logger;
    }

    /// <summary>Starts the session workers.</summary>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (_workers is not null)
        {
            return Task.CompletedTask;
        }

        _stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        int workerCount = Math.Max(1, _options.MaxConcurrentSessions);
        _workers = new Task[workerCount];

        for (int i = 0; i < workerCount; i++)
        {
            _workers[i] = Task.Run(() => WorkerAsync(_stopping.Token), CancellationToken.None);
        }

        ClientLog.ProcessorStarted(_logger, _options.Topic, _options.Subscription, workerCount);

        return Task.CompletedTask;
    }

    /// <summary>Stops the workers and waits for in-flight sessions to finish.</summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_stopping is null || _workers is null)
        {
            return;
        }

        ClientLog.ProcessorStopping(_logger, _options.Topic, _options.Subscription);

        await _stopping.CancelAsync();

        try
        {
            await Task.WhenAll(_workers).WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Shutdown ran out of time; the session locks lapse and another consumer resumes.
        }

        _workers = null;
    }

    /// <summary>
    /// One worker: take a session, drain it in order, release it, repeat.
    /// </summary>
    private async Task WorkerAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            WireAcceptedSession? session;

            try
            {
                session = await _broker.AcceptSessionAsync(
                    _options.Topic,
                    _options.Subscription,
                    sessionId: null,
                    _clientOptions.ReceiverId,
                    cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                ClientLog.ReceiveFailed(_logger, ex, _options.Topic, _options.Subscription);
                await DelayQuietlyAsync(_options.ErrorBackoff, cancellationToken);
                continue;
            }

            if (session is null)
            {
                // Every session is busy or empty. Pausing keeps an idle worker from spinning on
                // accept calls that will keep coming back empty.
                await DelayQuietlyAsync(_options.MaxWaitTime, cancellationToken);
                continue;
            }

            await DrainSessionAsync(session, cancellationToken);
        }
    }

    private async Task DrainSessionAsync(WireAcceptedSession session, CancellationToken cancellationToken)
    {
        DateTimeOffset idleSince = DateTimeOffset.UtcNow;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                IReadOnlyList<WireReceivedMessage> received = await _broker.ReceiveAsync(
                    _options.Topic,
                    _options.Subscription,
                    new WireReceiveRequest
                    {
                        // Strictly one at a time: claiming a batch would let the handler for a
                        // later message start before an earlier one finished, which is precisely
                        // the ordering guarantee a session exists to provide.
                        MaxMessages = 1,
                        MaxWaitTime = _options.MaxWaitTime,
                        SessionId = session.SessionId,
                        ReceiverId = _clientOptions.ReceiverId,
                    },
                    fromDeadLetterQueue: false,
                    cancellationToken);

                if (received.Count == 0)
                {
                    if (DateTimeOffset.UtcNow - idleSince >= _options.SessionIdleTimeout)
                    {
                        ClientLog.SessionReleased(_logger, session.SessionId);
                        return;
                    }

                    await RenewSessionAsync(session, cancellationToken);
                    continue;
                }

                idleSince = DateTimeOffset.UtcNow;

                foreach (WireReceivedMessage message in received)
                {
                    await _dispatcher.DispatchAsync(
                        _options.Topic,
                        _options.Subscription,
                        message,
                        _options.AutoComplete,
                        cancellationToken);
                }

                await RenewSessionAsync(session, cancellationToken);
            }
        }
        catch (SessionLockLostException)
        {
            ClientLog.SessionLockLost(_logger, session.SessionId);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutting down.
        }
        catch (Exception ex)
        {
            ClientLog.ReceiveFailed(_logger, ex, _options.Topic, _options.Subscription);
        }
        finally
        {
            await ReleaseQuietlyAsync(session);
        }
    }

    private async Task RenewSessionAsync(WireAcceptedSession session, CancellationToken cancellationToken)
    {
        try
        {
            await _broker.RenewSessionLockAsync(
                _options.Topic,
                _options.Subscription,
                session.SessionId,
                session.LockToken,
                cancellationToken);
        }
        catch (SessionLockLostException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex) when (ex is PubSubException)
        {
            ClientLog.LockRenewalStopped(_logger, 0, ex.Message);
        }
    }

    /// <summary>
    /// Releases a session on the way out, ignoring failures.
    /// </summary>
    /// <remarks>
    /// A release that fails costs one idle-timeout of latency before another consumer can take the
    /// session — worth a log line, never worth masking the error that got us here.
    /// </remarks>
    private async Task ReleaseQuietlyAsync(WireAcceptedSession session)
    {
        try
        {
            await _broker.ReleaseSessionAsync(
                _options.Topic,
                _options.Subscription,
                session.SessionId,
                session.LockToken,
                CancellationToken.None);
        }
        catch (Exception ex) when (ex is PubSubException or HttpRequestException)
        {
            ClientLog.LockRenewalStopped(_logger, 0, ex.Message);
        }
    }

    private static async Task DelayQuietlyAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _stopping?.Dispose();
    }
}
