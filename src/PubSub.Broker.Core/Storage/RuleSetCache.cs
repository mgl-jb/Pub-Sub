using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using PubSub.Abstractions;
using PubSub.Filters;

namespace PubSub.Broker.Core;

/// <summary>A subscription's compiled rules, together with the version they were compiled from.</summary>
public sealed record CachedRuleSet(int SubscriptionId, int RulesVersion, RuleSet Rules);

/// <summary>
/// Caches compiled rule sets so a publish does not re-parse every rule on the topic.
/// </summary>
/// <remarks>
/// Compilation is the expensive half of filtering and rules change rarely, so the cache is keyed on
/// the subscription's <see cref="SubscriptionEntity.RulesVersion"/>: an admin change bumps the
/// version, which invalidates the entry without anyone having to compare rule text. Stale entries
/// are therefore impossible rather than merely unlikely.
/// </remarks>
public sealed class RuleSetCache
{
    private readonly ConcurrentDictionary<int, CachedRuleSet> _cache = new();
    private readonly ILogger<RuleSetCache> _logger;

    /// <summary>Creates the cache.</summary>
    public RuleSetCache(ILogger<RuleSetCache> logger) => _logger = logger;

    /// <summary>
    /// Returns the compiled rules for every subscription on a topic, compiling any whose cached
    /// version is missing or stale.
    /// </summary>
    public async Task<IReadOnlyList<(SubscriptionEntity Subscription, RuleSet Rules)>> GetForTopicAsync(
        BrokerDbContext context,
        int topicId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        List<SubscriptionEntity> subscriptions = await context.Subscriptions
            .AsNoTracking()
            .Where(s => s.TopicId == topicId)
            .ToListAsync(cancellationToken);

        List<(SubscriptionEntity, RuleSet)> result = new(subscriptions.Count);
        List<int> needCompilation = [];

        foreach (SubscriptionEntity subscription in subscriptions)
        {
            if (_cache.TryGetValue(subscription.Id, out CachedRuleSet? cached)
                && cached.RulesVersion == subscription.RulesVersion)
            {
                result.Add((subscription, cached.Rules));
            }
            else
            {
                needCompilation.Add(subscription.Id);
            }
        }

        if (needCompilation.Count == 0)
        {
            return result;
        }

        List<RuleEntity> rules = await context.Rules
            .AsNoTracking()
            .Where(r => needCompilation.Contains(r.SubscriptionId))
            .ToListAsync(cancellationToken);

        ILookup<int, RuleEntity> bySubscription = rules.ToLookup(r => r.SubscriptionId);

        foreach (SubscriptionEntity subscription in subscriptions.Where(s => needCompilation.Contains(s.Id)))
        {
            RuleSet compiled = CompileRules(subscription, bySubscription[subscription.Id]);
            _cache[subscription.Id] = new CachedRuleSet(subscription.Id, subscription.RulesVersion, compiled);
            result.Add((subscription, compiled));
        }

        return result;
    }

    /// <summary>Drops a subscription's cached rules, forcing recompilation on next use.</summary>
    public void Invalidate(int subscriptionId) => _cache.TryRemove(subscriptionId, out _);

    /// <summary>Drops every cached rule set.</summary>
    public void InvalidateAll() => _cache.Clear();

    private RuleSet CompileRules(SubscriptionEntity subscription, IEnumerable<RuleEntity> rules)
    {
        List<CompiledRule> compiled = [];

        foreach (RuleEntity rule in rules)
        {
            try
            {
                compiled.Add(CompiledRule.Compile(ToDescriptor(rule)));
            }
            catch (FilterSyntaxException ex)
            {
                // A rule stored in a state the compiler rejects would otherwise take the whole
                // topic down on every publish. Dropping just that rule keeps the subscription's
                // remaining rules working, and the log names the rule to fix.
                BrokerLog.RuleCompilationFailed(
                    _logger, ex, rule.Name, subscription.Id, ex.Message);
            }
        }

        return new RuleSet(compiled);
    }

    private static RuleDescriptor ToDescriptor(RuleEntity rule)
    {
        MessageFilter filter = rule.FilterKind switch
        {
            RuleFilterKind.True => TrueFilter.Instance,
            RuleFilterKind.False => FalseFilter.Instance,
            RuleFilterKind.Sql => new SqlFilter(rule.SqlExpression ?? "1=1"),
            RuleFilterKind.Correlation => DeserializeCorrelation(rule.CorrelationJson),
            _ => TrueFilter.Instance,
        };

        RuleAction? action = string.IsNullOrWhiteSpace(rule.ActionExpression)
            ? null
            : new RuleAction(rule.ActionExpression);

        return new RuleDescriptor(rule.Name, filter, action);
    }

    private static CorrelationFilter DeserializeCorrelation(string? json)
    {
        if (string.IsNullOrEmpty(json))
        {
            return new CorrelationFilter();
        }

        CorrelationFilterDto? dto = JsonSerializer.Deserialize<CorrelationFilterDto>(
            json,
            JsonSerializerOptions.Web);

        if (dto is null)
        {
            return new CorrelationFilter();
        }

        CorrelationFilter filter = new()
        {
            CorrelationId = dto.CorrelationId,
            MessageId = dto.MessageId,
            Subject = dto.Subject,
            To = dto.To,
            ReplyTo = dto.ReplyTo,
            SessionId = dto.SessionId,
            ContentType = dto.ContentType,
        };

        if (dto.ApplicationProperties is not null)
        {
            foreach (KeyValuePair<string, object?> property in
                     MessagePropertySerializer.Deserialize(dto.ApplicationProperties))
            {
                filter.ApplicationProperties[property.Key] = property.Value;
            }
        }

        return filter;
    }

    private sealed record CorrelationFilterDto(
        string? CorrelationId,
        string? MessageId,
        string? Subject,
        string? To,
        string? ReplyTo,
        string? SessionId,
        string? ContentType,
        string? ApplicationProperties);
}
