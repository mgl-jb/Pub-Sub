using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Api;

/// <summary>Converts between wire contracts and the broker's own types.</summary>
internal static class ContractMapping
{
    /// <summary>Converts a wire message into an envelope for publishing.</summary>
    /// <remarks>
    /// Broker-assigned fields on the incoming DTO are ignored rather than trusted: a producer
    /// cannot choose its own sequence number or claim a delivery count.
    /// </remarks>
    public static MessageEnvelope ToEnvelope(this MessageDto dto)
    {
        ArgumentNullException.ThrowIfNull(dto);

        byte[] body = string.IsNullOrEmpty(dto.Body)
            ? []
            : Convert.FromBase64String(dto.Body);

        return new MessageEnvelope
        {
            MessageId = string.IsNullOrWhiteSpace(dto.MessageId)
                ? Guid.NewGuid().ToString("n")
                : dto.MessageId,
            CorrelationId = dto.CorrelationId,
            Subject = dto.Subject,
            ContentType = string.IsNullOrWhiteSpace(dto.ContentType) ? "application/json" : dto.ContentType,
            Body = body,
            // Values arrive as JsonElement; normalising them here is what lets a numeric filter
            // actually compare against a number rather than yielding UNKNOWN.
            ApplicationProperties = MessagePropertySerializer.NormalizeAll(dto.ApplicationProperties),
            SessionId = dto.SessionId,
            ReplyTo = dto.ReplyTo,
            ReplyToSessionId = dto.ReplyToSessionId,
            To = dto.To,
            ScheduledEnqueueTime = dto.ScheduledEnqueueTime,
            TimeToLive = dto.TimeToLive,
        };
    }

    /// <summary>Converts an envelope into its wire form.</summary>
    public static MessageDto ToDto(this MessageEnvelope message) => new()
    {
        MessageId = message.MessageId,
        CorrelationId = message.CorrelationId,
        Subject = message.Subject,
        ContentType = message.ContentType,
        Body = Convert.ToBase64String(message.Body.Span),
        ApplicationProperties = new Dictionary<string, object?>(
            message.ApplicationProperties,
            StringComparer.Ordinal),
        SessionId = message.SessionId,
        ReplyTo = message.ReplyTo,
        ReplyToSessionId = message.ReplyToSessionId,
        To = message.To,
        SequenceNumber = message.SequenceNumber,
        EnqueuedTime = message.EnqueuedTime,
        DeliveryCount = message.DeliveryCount,
        DeadLetterReason = message.DeadLetterReason,
        DeadLetterDescription = message.DeadLetterDescription,
    };

    /// <summary>Converts a claimed message into its wire form.</summary>
    public static ReceivedMessageDto ToDto(this ReceivedMessage received) => new()
    {
        DeliveryId = received.DeliveryId,
        LockToken = received.LockToken,
        LockedUntil = received.LockedUntil,
        Message = received.Message.ToDto(),
    };

    /// <summary>Converts a publish outcome into its wire form.</summary>
    public static PublishResultDto ToDto(this PublishResult result) => new()
    {
        SequenceNumber = result.SequenceNumber,
        WasDuplicate = result.WasDuplicate,
        MatchedSubscriptions = result.MatchedSubscriptions,
    };

    /// <summary>Converts an accepted session into its wire form.</summary>
    public static AcceptedSessionDto ToDto(this AcceptedSession session) => new()
    {
        SessionId = session.SessionId,
        LockToken = session.LockToken,
        LockedUntil = session.LockedUntil,
        State = session.State is null ? null : Convert.ToBase64String(session.State),
    };

    /// <summary>Converts a topic entity into its wire form.</summary>
    public static TopicDto ToDto(this TopicEntity topic) => new()
    {
        Name = topic.Name,
        DefaultTimeToLive = topic.DefaultTimeToLive,
        DuplicateDetectionEnabled = topic.DuplicateDetectionEnabled,
        CreatedAt = topic.CreatedAt,
    };

    /// <summary>Converts a subscription entity into its wire form.</summary>
    public static SubscriptionDto ToDto(this SubscriptionEntity subscription) => new()
    {
        Name = subscription.Name,
        LockDuration = subscription.LockDuration,
        MaxDeliveryCount = subscription.MaxDeliveryCount,
        RequiresSession = subscription.RequiresSession,
        CreatedAt = subscription.CreatedAt,
    };

    /// <summary>Converts a rule entity into its wire form.</summary>
    public static RuleDto ToDto(this RuleEntity rule) => new()
    {
        Name = rule.Name,
        FilterKind = rule.FilterKind.ToString(),
        Filter = rule.FilterKind switch
        {
            RuleFilterKind.Sql => rule.SqlExpression,
            RuleFilterKind.Correlation => rule.CorrelationJson,
            RuleFilterKind.True => "1=1",
            RuleFilterKind.False => "1=0",
            _ => null,
        },
        Action = rule.ActionExpression,
    };

    /// <summary>Builds topic options from a create request, leaving unset fields at their defaults.</summary>
    public static TopicOptions ToOptions(this CreateTopicDto dto)
    {
        ArgumentNullException.ThrowIfNull(dto);

        TopicOptions options = new()
        {
            DuplicateDetectionEnabled = dto.DuplicateDetectionEnabled,
        };

        if (dto.DefaultTimeToLive is { } ttl)
        {
            options.DefaultTimeToLive = ttl;
        }

        if (dto.DuplicateDetectionWindow is { } window)
        {
            options.DuplicateDetectionWindow = window;
        }

        if (dto.MaxMessageSizeBytes is { } size)
        {
            options.MaxMessageSizeBytes = size;
        }

        return options;
    }

    /// <summary>Builds subscription options from a create request.</summary>
    public static SubscriptionOptions ToOptions(this CreateSubscriptionDto dto)
    {
        ArgumentNullException.ThrowIfNull(dto);

        SubscriptionOptions options = new()
        {
            RequiresSession = dto.RequiresSession,
            DeadLetterOnMessageExpiration = dto.DeadLetterOnMessageExpiration,
        };

        if (dto.LockDuration is { } lockDuration)
        {
            options.LockDuration = lockDuration;
        }

        if (dto.MaxDeliveryCount is { } maxDeliveryCount)
        {
            options.MaxDeliveryCount = maxDeliveryCount;
        }

        if (dto.SessionLockDuration is { } sessionLock)
        {
            options.SessionLockDuration = sessionLock;
        }

        return options;
    }

    /// <summary>Builds a rule descriptor from a create request.</summary>
    /// <remarks>
    /// A request naming neither a SQL expression nor a correlation filter is a catch-all, which is
    /// the intent behind "subscribe me to this topic".
    /// </remarks>
    public static RuleDescriptor ToDescriptor(this CreateRuleDto dto)
    {
        ArgumentNullException.ThrowIfNull(dto);

        MessageFilter filter;

        if (!string.IsNullOrWhiteSpace(dto.SqlExpression))
        {
            filter = new SqlFilter(dto.SqlExpression);
        }
        else if (dto.CorrelationFilter is { } correlation)
        {
            CorrelationFilter built = new()
            {
                CorrelationId = correlation.CorrelationId,
                Subject = correlation.Subject,
                To = correlation.To,
                ReplyTo = correlation.ReplyTo,
                SessionId = correlation.SessionId,
                ContentType = correlation.ContentType,
            };

            if (correlation.ApplicationProperties is not null)
            {
                foreach (KeyValuePair<string, object?> property in correlation.ApplicationProperties)
                {
                    built.ApplicationProperties[property.Key] = property.Value;
                }
            }

            filter = built;
        }
        else
        {
            filter = TrueFilter.Instance;
        }

        RuleAction? action = string.IsNullOrWhiteSpace(dto.Action) ? null : new RuleAction(dto.Action);

        return new RuleDescriptor(dto.Name, filter, action);
    }
}
