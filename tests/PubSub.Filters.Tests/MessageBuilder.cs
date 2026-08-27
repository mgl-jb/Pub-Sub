using PubSub.Abstractions;

namespace PubSub.Filters.Tests;

/// <summary>Builds messages for tests without repeating envelope boilerplate.</summary>
internal static class MessageBuilder
{
    public static MessageEnvelope Message(
        object? properties = null,
        string? subject = null,
        string? correlationId = null,
        string? sessionId = null,
        string? to = null,
        string? replyTo = null,
        string contentType = "application/json",
        long sequenceNumber = 1,
        int deliveryCount = 1)
    {
        Dictionary<string, object?> bag = new(StringComparer.Ordinal);

        if (properties is not null)
        {
            foreach (System.Reflection.PropertyInfo property in properties.GetType().GetProperties())
            {
                bag[property.Name] = property.GetValue(properties);
            }
        }

        return new MessageEnvelope
        {
            MessageId = "test-message",
            Subject = subject,
            CorrelationId = correlationId,
            SessionId = sessionId,
            To = to,
            ReplyTo = replyTo,
            ContentType = contentType,
            SequenceNumber = sequenceNumber,
            DeliveryCount = deliveryCount,
            EnqueuedTime = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            ApplicationProperties = bag,
        };
    }

    /// <summary>Evaluates an expression against a message, returning whether it matched.</summary>
    public static bool Eval(string expression, MessageEnvelope message) =>
        FilterCompiler.Compile(expression)(message);

    /// <summary>
    /// Evaluates an expression to its raw three-valued result: <c>true</c>, <c>false</c>, or
    /// <c>null</c> for UNKNOWN.
    /// </summary>
    /// <remarks>
    /// The predicate path collapses UNKNOWN to "did not match", which hides the distinction the
    /// three-valued tests are about — so those go through the value evaluator instead.
    /// </remarks>
    public static bool? EvalRaw(string expression, MessageEnvelope message) =>
        SqlValue.AsBoolean(ValueEvaluator.Build(expression)(message));
}
