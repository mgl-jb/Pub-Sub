using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PubSub.Abstractions;
using PubSub.Filters;

namespace PubSub.Broker.Core;

/// <summary>
/// The broker's message operations: publish, receive, and settle.
/// </summary>
/// <remarks>
/// Entity administration lives on <see cref="BrokerAdmin"/>; this type deals only with messages in
/// flight.
/// </remarks>
public sealed partial class BrokerStore
{
    private readonly BrokerDbContext _context;
    private readonly RuleSetCache _ruleCache;
    private readonly IDeliveryNotifier _notifier;
    private readonly TimeProvider _time;
    private readonly BrokerOptions _options;
    private readonly ILogger<BrokerStore> _logger;

    /// <summary>Creates the store.</summary>
    public BrokerStore(
        BrokerDbContext context,
        RuleSetCache ruleCache,
        IDeliveryNotifier notifier,
        TimeProvider time,
        IOptions<BrokerOptions> options,
        ILogger<BrokerStore> logger)
    {
        ArgumentNullException.ThrowIfNull(options);

        _context = context;
        _ruleCache = ruleCache;
        _notifier = notifier;
        _time = time;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>Publishes a batch of messages to a topic in one transaction.</summary>
    /// <remarks>
    /// The batch is atomic: either every message is stored and fanned out, or none is. Callers
    /// therefore never see a partially published batch after a failure.
    /// </remarks>
    public async Task<IReadOnlyList<PublishResult>> PublishAsync(
        string topicName,
        IReadOnlyList<MessageEnvelope> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentNullException.ThrowIfNull(messages);

        if (messages.Count == 0)
        {
            return [];
        }

        if (messages.Count > _options.MaxBatchPublishCount)
        {
            throw new InvalidOperationForStateException(
                $"A publish batch may hold at most {_options.MaxBatchPublishCount} messages; " +
                $"{messages.Count} were supplied.");
        }

        TopicEntity topic = await _context.Topics
                                .AsNoTracking()
                                .FirstOrDefaultAsync(t => t.Name == topicName, cancellationToken)
                            ?? throw new EntityNotFoundException("Topic", topicName);

        if (topic.PublishingSuspended)
        {
            throw new InvalidOperationForStateException(
                $"Publishing to topic '{topicName}' is suspended.");
        }

        foreach (MessageEnvelope message in messages)
        {
            if (message.Body.Length > topic.MaxMessageSizeBytes)
            {
                throw new InvalidOperationForStateException(
                    $"Message '{message.MessageId}' is {message.Body.Length} bytes, which exceeds " +
                    $"the topic's limit of {topic.MaxMessageSizeBytes}.");
            }
        }

        IReadOnlyList<(SubscriptionEntity Subscription, RuleSet Rules)> subscriptions =
            await _ruleCache.GetForTopicAsync(_context, topic.Id, cancellationToken);

        DateTimeOffset now = _time.GetUtcNow();
        List<PublishResult> results = new(messages.Count);
        HashSet<int> notified = [];

        await using Microsoft.EntityFrameworkCore.Storage.IDbContextTransaction transaction =
            await _context.Database.BeginTransactionAsync(cancellationToken);

        foreach (MessageEnvelope message in messages)
        {
            PublishResult result =
                await PublishOneAsync(topic, subscriptions, message, now, notified, cancellationToken);

            results.Add(result);
        }

        await _context.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        // Signalling only after the commit is deliberate: a receiver woken before the rows are
        // visible would find nothing and go back to sleep, turning a fast path into a slow one.
        foreach (int subscriptionId in notified)
        {
            await _notifier.NotifyAsync(subscriptionId, cancellationToken);
        }

        return results;
    }

    private async Task<PublishResult> PublishOneAsync(
        TopicEntity topic,
        IReadOnlyList<(SubscriptionEntity Subscription, RuleSet Rules)> subscriptions,
        MessageEnvelope message,
        DateTimeOffset now,
        HashSet<int> notified,
        CancellationToken cancellationToken)
    {
        if (topic.DuplicateDetectionEnabled)
        {
            DedupEntity? existing = await _context.DedupEntries
                .AsNoTracking()
                .FirstOrDefaultAsync(
                    d => d.TopicId == topic.Id
                         && d.MessageId == message.MessageId
                         && d.ExpiresAt > now,
                    cancellationToken);

            if (existing is not null)
            {
                BrokerLog.DuplicateSuppressed(
                    _logger, message.MessageId, topic.Name, existing.SequenceNumber);

                return new PublishResult(existing.SequenceNumber, WasDuplicate: true, MatchedSubscriptions: 0);
            }
        }

        TimeSpan timeToLive = message.TimeToLive ?? topic.DefaultTimeToLive;
        DateTimeOffset availableAt = message.ScheduledEnqueueTime ?? now;

        MessageEntity entity = new()
        {
            TopicId = topic.Id,
            MessageId = message.MessageId,
            CorrelationId = message.CorrelationId,
            Subject = message.Subject,
            ContentType = message.ContentType,
            Body = message.Body.ToArray(),
            ApplicationPropertiesJson = MessagePropertySerializer.Serialize(message.ApplicationProperties),
            SessionId = message.SessionId,
            ReplyTo = message.ReplyTo,
            ReplyToSessionId = message.ReplyToSessionId,
            To = message.To,
            EnqueuedTime = now,

            // TTL is measured from the point the message becomes visible, not from publish.
            // Otherwise a message scheduled beyond its own time to live would expire before it
            // could ever be delivered.
            ExpiresAt = availableAt.Add(timeToLive),
        };

        _context.Messages.Add(entity);

        // The sequence number is assigned by the database, and the fan-out rows need it, so this
        // batch is flushed before the deliveries are built.
        await _context.SaveChangesAsync(cancellationToken);

        int matched = FanOut(entity, subscriptions, message, now, availableAt, notified);

        if (topic.DuplicateDetectionEnabled)
        {
            _context.DedupEntries.Add(new DedupEntity
            {
                TopicId = topic.Id,
                MessageId = message.MessageId,
                SequenceNumber = entity.SequenceNumber,
                PublishedAt = now,
                ExpiresAt = now.Add(topic.DuplicateDetectionWindow),
            });
        }

        return new PublishResult(entity.SequenceNumber, WasDuplicate: false, MatchedSubscriptions: matched);
    }

    /// <summary>
    /// Creates one delivery row per subscription whose rules match, applying any rule action to
    /// that subscription's copy of the properties.
    /// </summary>
    private int FanOut(
        MessageEntity entity,
        IReadOnlyList<(SubscriptionEntity Subscription, RuleSet Rules)> subscriptions,
        MessageEnvelope message,
        DateTimeOffset now,
        DateTimeOffset availableAt,
        HashSet<int> notified)
    {
        int matched = 0;

        foreach ((SubscriptionEntity subscription, RuleSet rules) in subscriptions)
        {
            // The envelope is rebuilt per subscription because a rule action mutates the
            // properties, and one subscription's transformation must not leak into another's.
            MessageEnvelope candidate = CloneForEvaluation(message, entity, now);

            bool isMatch;
            CompiledRule? rule;

            try
            {
                isMatch = rules.TryMatch(candidate, out rule);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A rule that throws is a bug in the rule, not in the message. Dead-lettering the
                // delivery makes it visible; dropping it silently would look like a routing gap.
                BrokerLog.RuleEvaluationFailed(
                    _logger, ex, subscription.Name, entity.SequenceNumber);

                if (subscription.DeadLetterOnFilterEvaluationError)
                {
                    AddDelivery(
                        entity,
                        subscription,
                        availableAt,
                        propertiesOverride: null,
                        state: MessageState.DeadLettered,
                        deadLetterReason: DeadLetterReason.FilterEvaluationError,
                        deadLetterDescription: ex.Message,
                        now: now);

                    matched++;
                    notified.Add(subscription.Id);
                }

                continue;
            }

            if (!isMatch)
            {
                continue;
            }

            string? propertiesOverride = null;

            if (rule?.HasAction == true)
            {
                rule.ApplyAction(candidate);
                propertiesOverride = MessagePropertySerializer.Serialize(candidate.ApplicationProperties);
            }

            AddDelivery(
                entity,
                subscription,
                availableAt,
                propertiesOverride,
                state: MessageState.Available,
                deadLetterReason: null,
                deadLetterDescription: null,
                now: now);

            matched++;

            // Only wake receivers for messages that are visible now. A scheduled message will be
            // picked up by the poll that follows its availability time.
            if (availableAt <= now)
            {
                notified.Add(subscription.Id);
            }
        }

        return matched;
    }

    private void AddDelivery(
        MessageEntity entity,
        SubscriptionEntity subscription,
        DateTimeOffset availableAt,
        string? propertiesOverride,
        MessageState state,
        string? deadLetterReason,
        string? deadLetterDescription,
        DateTimeOffset now)
    {
        TimeSpan? subscriptionTtl = subscription.DefaultTimeToLive;

        _context.Deliveries.Add(new DeliveryEntity
        {
            MessageSequenceNumber = entity.SequenceNumber,
            SubscriptionId = subscription.Id,
            SequenceNumber = entity.SequenceNumber,
            SessionId = entity.SessionId,
            State = state,
            AvailableAt = availableAt,
            DeliveryCount = 0,
            OverriddenPropertiesJson = propertiesOverride,
            DeadLetterReason = deadLetterReason,
            DeadLetterDescription = deadLetterDescription,
            CreatedAt = now,

            // A subscription may shorten the message's lifetime but never extend it beyond what
            // the topic allows, so the earlier of the two wins.
            ExpiresAt = subscriptionTtl is null
                ? entity.ExpiresAt
                : Min(entity.ExpiresAt, availableAt.Add(subscriptionTtl.Value)),
        });
    }

    private static DateTimeOffset Min(DateTimeOffset left, DateTimeOffset right) =>
        left <= right ? left : right;

    private static MessageEnvelope CloneForEvaluation(
        MessageEnvelope source,
        MessageEntity entity,
        DateTimeOffset now) =>
        new()
        {
            MessageId = source.MessageId,
            CorrelationId = source.CorrelationId,
            Subject = source.Subject,
            ContentType = source.ContentType,
            Body = source.Body,
            ApplicationProperties = new Dictionary<string, object?>(
                source.ApplicationProperties,
                StringComparer.Ordinal),
            SessionId = source.SessionId,
            ReplyTo = source.ReplyTo,
            ReplyToSessionId = source.ReplyToSessionId,
            To = source.To,
            SequenceNumber = entity.SequenceNumber,
            EnqueuedTime = now,
            DeliveryCount = 0,
        };
}
