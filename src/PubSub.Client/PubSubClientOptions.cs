namespace PubSub.Client;

/// <summary>How the client reaches and behaves toward the broker.</summary>
public sealed class PubSubClientOptions
{
    /// <summary>Configuration section these bind from.</summary>
    public const string SectionName = "PubSub";

    /// <summary>The broker's base address.</summary>
    public Uri? BrokerUri { get; set; }

    /// <summary>Timeout for a single broker call, excluding the long-poll wait.</summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Identifies this client in the broker's lock diagnostics.</summary>
    public string ReceiverId { get; set; } = $"{Environment.MachineName}-{Environment.ProcessId}";

    /// <summary>Retry attempts for a transient broker failure.</summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>Base delay for retry backoff; each attempt roughly doubles it, with jitter.</summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(500);
}

/// <summary>How one subscription's processor pumps messages.</summary>
public sealed class MessageProcessorOptions
{
    /// <summary>
    /// The handlers this processor dispatches to.
    /// </summary>
    /// <remarks>
    /// Per-processor rather than per-process, because one worker commonly consumes several
    /// subscriptions of the same topic and needs a different handler for each — a shipping
    /// subscription and a high-value subscription both carry <c>OrderPlaced</c>, and a single
    /// shared registry could only route one of them.
    /// </remarks>
    public HandlerRegistry Handlers { get; } = new();

    /// <summary>The topic to consume from.</summary>
    public required string Topic { get; set; }

    /// <summary>The subscription to consume from.</summary>
    public required string Subscription { get; set; }

    /// <summary>
    /// How many messages are processed at once.
    /// </summary>
    /// <remarks>
    /// This is the main throughput lever and the main way to cause lock expiry: every in-flight
    /// message holds a lock, so a concurrency far above what the handler can actually keep up with
    /// produces redeliveries rather than throughput.
    /// </remarks>
    public int MaxConcurrentCalls { get; set; } = 1;

    /// <summary>How many messages to claim per receive call.</summary>
    public int PrefetchCount { get; set; } = 1;

    /// <summary>How long each receive call waits before returning empty.</summary>
    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Completes a message when its handler returns, and abandons it when the handler throws.
    /// </summary>
    /// <remarks>
    /// Turning this off makes the handler responsible for settling every message. Forgetting to do
    /// so means the message is redelivered on lock expiry, so leave it on unless the handler
    /// genuinely settles on every path.
    /// </remarks>
    public bool AutoComplete { get; set; } = true;

    /// <summary>
    /// How long the processor keeps renewing a lock for a handler that is still working.
    /// </summary>
    /// <remarks>
    /// Renewal covers work that is legitimately slower than the lock duration without setting that
    /// duration high for everything — a long lock would leave a crashed consumer's messages idle
    /// for just as long. The ceiling stops a hung handler from holding a message forever.
    /// </remarks>
    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>How long to pause after a receive failure before trying again.</summary>
    public TimeSpan ErrorBackoff { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Consumes the subscription's dead-letter queue instead of its live messages.</summary>
    public bool ProcessDeadLetterQueue { get; set; }
}

/// <summary>How a session processor pumps ordered sessions.</summary>
public sealed class SessionProcessorOptions
{
    /// <summary>The handlers this processor dispatches to. See <see cref="MessageProcessorOptions.Handlers"/>.</summary>
    public HandlerRegistry Handlers { get; } = new();

    /// <summary>The topic to consume from.</summary>
    public required string Topic { get; set; }

    /// <summary>The session-enabled subscription to consume from.</summary>
    public required string Subscription { get; set; }

    /// <summary>
    /// How many sessions are processed at once.
    /// </summary>
    /// <remarks>
    /// Concurrency is across sessions, never within one — that is the whole point. Messages inside
    /// a single session are always handled one at a time, in order.
    /// </remarks>
    public int MaxConcurrentSessions { get; set; } = 1;

    /// <summary>How long a session is held with no messages before it is released.</summary>
    /// <remarks>
    /// Releasing an idle session lets another consumer take it, and stops one worker accumulating
    /// locks on sessions that have gone quiet.
    /// </remarks>
    public TimeSpan SessionIdleTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>How long each receive call within a session waits.</summary>
    public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Completes a message when its handler returns.</summary>
    public bool AutoComplete { get; set; } = true;

    /// <summary>How long the processor keeps renewing a message lock.</summary>
    public TimeSpan MaxAutoLockRenewalDuration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>How long to pause after a failure before trying again.</summary>
    public TimeSpan ErrorBackoff { get; set; } = TimeSpan.FromSeconds(5);
}
