using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using PubSub.Abstractions;
using PubSub.Filters;

namespace PubSub.Broker.Core;

/// <summary>
/// Creates and manages topics, subscriptions, and rules.
/// </summary>
/// <remarks>
/// Kept apart from <see cref="BrokerStore"/> because the two have opposite cost profiles: message
/// operations run constantly and must stay lean, while administration runs rarely and can afford
/// to validate thoroughly.
/// </remarks>
public sealed class BrokerAdmin
{
    private readonly BrokerDbContext _context;
    private readonly RuleSetCache _ruleCache;
    private readonly TimeProvider _time;

    /// <summary>Creates the administration surface.</summary>
    public BrokerAdmin(BrokerDbContext context, RuleSetCache ruleCache, TimeProvider time)
    {
        _context = context;
        _ruleCache = ruleCache;
        _time = time;
    }

    /// <summary>Creates a topic.</summary>
    /// <exception cref="EntityAlreadyExistsException">A topic with that name already exists.</exception>
    public async Task<TopicEntity> CreateTopicAsync(
        string name,
        TopicOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ValidateEntityName(name, nameof(name));
        options ??= new TopicOptions();

        if (await _context.Topics.AnyAsync(t => t.Name == name, cancellationToken))
        {
            throw new EntityAlreadyExistsException("Topic", name);
        }

        TopicEntity topic = new()
        {
            Name = name,
            DefaultTimeToLive = options.DefaultTimeToLive,
            DuplicateDetectionEnabled = options.DuplicateDetectionEnabled,
            DuplicateDetectionWindow = options.DuplicateDetectionWindow,
            MaxMessageSizeBytes = options.MaxMessageSizeBytes,
            PublishingSuspended = options.PublishingSuspended,
            CreatedAt = _time.GetUtcNow(),
        };

        _context.Topics.Add(topic);
        await _context.SaveChangesAsync(cancellationToken);
        return topic;
    }

    /// <summary>Creates a topic if it does not already exist.</summary>
    public async Task<TopicEntity> EnsureTopicAsync(
        string name,
        TopicOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        TopicEntity? existing = await _context.Topics
            .FirstOrDefaultAsync(t => t.Name == name, cancellationToken);

        return existing ?? await CreateTopicAsync(name, options, cancellationToken);
    }

    /// <summary>Lists every topic.</summary>
    public async Task<IReadOnlyList<TopicEntity>> ListTopicsAsync(
        CancellationToken cancellationToken = default) =>
        await _context.Topics.AsNoTracking().OrderBy(t => t.Name).ToListAsync(cancellationToken);

    /// <summary>Deletes a topic together with its subscriptions and messages.</summary>
    public async Task<bool> DeleteTopicAsync(string name, CancellationToken cancellationToken = default)
    {
        TopicEntity? topic = await _context.Topics
            .Include(t => t.Subscriptions)
            .FirstOrDefaultAsync(t => t.Name == name, cancellationToken);

        if (topic is null)
        {
            return false;
        }

        int[] subscriptionIds = [.. topic.Subscriptions.Select(s => s.Id)];

        foreach (int subscriptionId in subscriptionIds)
        {
            _ruleCache.Invalidate(subscriptionId);
        }

        // Same reason as DeleteSubscriptionAsync: this foreign key is deliberately non-cascading.
        await _context.Deliveries
            .Where(d => subscriptionIds.Contains(d.SubscriptionId))
            .ExecuteDeleteAsync(cancellationToken);

        _context.Topics.Remove(topic);
        await _context.SaveChangesAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Creates a subscription on a topic, with a catch-all rule unless one is supplied.
    /// </summary>
    /// <remarks>
    /// The default rule exists because a subscription with no rules receives nothing. Creating one
    /// silently empty would look like a broker fault rather than a missing rule, so the common
    /// intent — "give me everything on this topic" — is what a bare subscription gets.
    /// </remarks>
    public async Task<SubscriptionEntity> CreateSubscriptionAsync(
        string topicName,
        string subscriptionName,
        SubscriptionOptions? options = null,
        RuleDescriptor? rule = null,
        CancellationToken cancellationToken = default)
    {
        ValidateEntityName(subscriptionName, nameof(subscriptionName));
        options ??= new SubscriptionOptions();

        TopicEntity topic = await _context.Topics
                                .FirstOrDefaultAsync(t => t.Name == topicName, cancellationToken)
                            ?? throw new EntityNotFoundException("Topic", topicName);

        bool exists = await _context.Subscriptions
            .AnyAsync(s => s.TopicId == topic.Id && s.Name == subscriptionName, cancellationToken);

        if (exists)
        {
            throw new EntityAlreadyExistsException("Subscription", $"{topicName}/{subscriptionName}");
        }

        DateTimeOffset now = _time.GetUtcNow();

        SubscriptionEntity subscription = new()
        {
            TopicId = topic.Id,
            Name = subscriptionName,
            LockDuration = options.LockDuration,
            MaxDeliveryCount = options.MaxDeliveryCount,
            RequiresSession = options.RequiresSession,
            SessionLockDuration = options.SessionLockDuration,
            DeadLetterOnMessageExpiration = options.DeadLetterOnMessageExpiration,
            DeadLetterOnFilterEvaluationError = options.DeadLetterOnFilterEvaluationError,
            DefaultTimeToLive = options.DefaultTimeToLive,
            ReceivingSuspended = options.ReceivingSuspended,
            CreatedAt = now,
            RulesVersion = 1,
        };

        _context.Subscriptions.Add(subscription);
        await _context.SaveChangesAsync(cancellationToken);

        rule ??= new RuleDescriptor(RuleDescriptor.DefaultRuleName, TrueFilter.Instance);
        await AddRuleAsync(topicName, subscriptionName, rule, cancellationToken);

        return subscription;
    }

    /// <summary>Lists a topic's subscriptions.</summary>
    public async Task<IReadOnlyList<SubscriptionEntity>> ListSubscriptionsAsync(
        string topicName,
        CancellationToken cancellationToken = default) =>
        await _context.Subscriptions
            .AsNoTracking()
            .Where(s => s.Topic!.Name == topicName)
            .OrderBy(s => s.Name)
            .ToListAsync(cancellationToken);

    /// <summary>Deletes a subscription and everything queued for it.</summary>
    public async Task<bool> DeleteSubscriptionAsync(
        string topicName,
        string subscriptionName,
        CancellationToken cancellationToken = default)
    {
        SubscriptionEntity? subscription = await _context.Subscriptions
            .FirstOrDefaultAsync(
                s => s.Topic!.Name == topicName && s.Name == subscriptionName,
                cancellationToken);

        if (subscription is null)
        {
            return false;
        }

        _ruleCache.Invalidate(subscription.Id);

        // The delivery-to-subscription foreign key does not cascade (see BrokerDbContext), so its
        // rows are removed here rather than by the database.
        await _context.Deliveries
            .Where(d => d.SubscriptionId == subscription.Id)
            .ExecuteDeleteAsync(cancellationToken);

        _context.Subscriptions.Remove(subscription);
        await _context.SaveChangesAsync(cancellationToken);
        return true;
    }

    /// <summary>
    /// Adds a rule to a subscription, validating its filter and action before storing them.
    /// </summary>
    /// <remarks>
    /// Compilation happens here so a malformed expression is rejected by the caller who wrote it,
    /// rather than surfacing later as a subscription that mysteriously receives nothing.
    /// </remarks>
    /// <exception cref="FilterSyntaxException">The filter or action text is malformed.</exception>
    public async Task<RuleEntity> AddRuleAsync(
        string topicName,
        string subscriptionName,
        RuleDescriptor rule,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);

        SubscriptionEntity subscription = await _context.Subscriptions
                                              .FirstOrDefaultAsync(
                                                  s => s.Topic!.Name == topicName && s.Name == subscriptionName,
                                                  cancellationToken)
                                          ?? throw new EntityNotFoundException(
                                              "Subscription", $"{topicName}/{subscriptionName}");

        bool exists = await _context.Rules
            .AnyAsync(r => r.SubscriptionId == subscription.Id && r.Name == rule.Name, cancellationToken);

        if (exists)
        {
            throw new EntityAlreadyExistsException("Rule", $"{topicName}/{subscriptionName}/{rule.Name}");
        }

        // Throws FilterSyntaxException on malformed input, before anything is written.
        _ = CompiledRule.Compile(rule);

        RuleEntity entity = new()
        {
            SubscriptionId = subscription.Id,
            Name = rule.Name,
            CreatedAt = _time.GetUtcNow(),
            ActionExpression = rule.Action?.Expression,
        };

        ApplyFilter(entity, rule.Filter);

        _context.Rules.Add(entity);

        // Bumping the version is what invalidates every broker instance's cached compilation,
        // including instances that never see this call.
        subscription.RulesVersion++;

        await _context.SaveChangesAsync(cancellationToken);
        _ruleCache.Invalidate(subscription.Id);

        return entity;
    }

    /// <summary>Removes a rule from a subscription.</summary>
    public async Task<bool> RemoveRuleAsync(
        string topicName,
        string subscriptionName,
        string ruleName,
        CancellationToken cancellationToken = default)
    {
        SubscriptionEntity? subscription = await _context.Subscriptions
            .FirstOrDefaultAsync(
                s => s.Topic!.Name == topicName && s.Name == subscriptionName,
                cancellationToken);

        if (subscription is null)
        {
            return false;
        }

        RuleEntity? rule = await _context.Rules
            .FirstOrDefaultAsync(
                r => r.SubscriptionId == subscription.Id && r.Name == ruleName,
                cancellationToken);

        if (rule is null)
        {
            return false;
        }

        _context.Rules.Remove(rule);
        subscription.RulesVersion++;
        await _context.SaveChangesAsync(cancellationToken);
        _ruleCache.Invalidate(subscription.Id);

        return true;
    }

    /// <summary>Lists a subscription's rules.</summary>
    public async Task<IReadOnlyList<RuleEntity>> ListRulesAsync(
        string topicName,
        string subscriptionName,
        CancellationToken cancellationToken = default) =>
        await _context.Rules
            .AsNoTracking()
            .Where(r => r.Subscription!.Name == subscriptionName
                        && r.Subscription!.Topic!.Name == topicName)
            .OrderBy(r => r.Name)
            .ToListAsync(cancellationToken);

    private static void ApplyFilter(RuleEntity entity, MessageFilter filter)
    {
        switch (filter)
        {
            case TrueFilter:
                entity.FilterKind = RuleFilterKind.True;
                break;

            case FalseFilter:
                entity.FilterKind = RuleFilterKind.False;
                break;

            case SqlFilter sql:
                entity.FilterKind = RuleFilterKind.Sql;
                entity.SqlExpression = sql.Expression;
                break;

            case CorrelationFilter correlation:
                entity.FilterKind = RuleFilterKind.Correlation;
                entity.CorrelationJson = JsonSerializer.Serialize(
                    new
                    {
                        correlation.CorrelationId,
                        correlation.MessageId,
                        correlation.Subject,
                        correlation.To,
                        correlation.ReplyTo,
                        correlation.SessionId,
                        correlation.ContentType,
                        ApplicationProperties =
                            MessagePropertySerializer.Serialize(correlation.ApplicationProperties),
                    },
                    JsonSerializerOptions.Web);
                break;

            default:
                throw new NotSupportedException(
                    $"Filter type '{filter.GetType().Name}' cannot be stored.");
        }
    }

    /// <summary>
    /// Rejects names that would be ambiguous in a URL path, since entities are addressed as
    /// <c>/topics/{topic}/subscriptions/{subscription}</c>.
    /// </summary>
    private static void ValidateEntityName(string name, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name, parameterName);

        if (name.Length > 260)
        {
            throw new ArgumentException(
                "An entity name may be at most 260 characters.", parameterName);
        }

        if (name.StartsWith('$'))
        {
            throw new ArgumentException(
                "Names beginning with '$' are reserved for the broker.", parameterName);
        }

        foreach (char c in name)
        {
            bool allowed = char.IsLetterOrDigit(c) || c is '-' or '_' or '.';
            if (!allowed)
            {
                throw new ArgumentException(
                    $"'{name}' contains '{c}'. Entity names may use letters, digits, hyphens, " +
                    "underscores, and dots.",
                    parameterName);
            }
        }
    }
}
