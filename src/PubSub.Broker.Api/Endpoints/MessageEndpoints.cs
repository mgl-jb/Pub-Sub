using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using PubSub.Abstractions;
using PubSub.Broker.Core;

namespace PubSub.Broker.Api;

/// <summary>Publish, receive, and settlement endpoints.</summary>
public static class MessageEndpoints
{
    /// <summary>Maps the message-plane routes.</summary>
    public static IEndpointRouteBuilder MapMessageEndpoints(this IEndpointRouteBuilder routes)
    {
        ArgumentNullException.ThrowIfNull(routes);

        RouteGroupBuilder publish = routes.MapGroup("/topics/{topic}")
            .WithTags("Publish")
            .RequireAuthorization(BrokerPolicies.Publish);

        publish.MapPost("/messages", PublishAsync)
            .WithName("PublishMessages")
            .WithSummary("Publishes one or more messages to a topic as a single atomic batch.");

        publish.MapDelete("/scheduled/{sequenceNumber:long}", CancelScheduledAsync)
            .WithName("CancelScheduledMessage")
            .WithSummary("Cancels a scheduled message that has not yet become visible.");

        RouteGroupBuilder receive = routes.MapGroup("/topics/{topic}/subscriptions/{subscription}")
            .WithTags("Receive")
            .RequireAuthorization(BrokerPolicies.Subscribe);

        receive.MapPost("/messages/receive", ReceiveAsync)
            .WithName("ReceiveMessages")
            .WithSummary("Claims messages under a peek-lock, waiting up to the requested duration.");

        receive.MapPost("/messages/receive-deferred", ReceiveDeferredAsync)
            .WithName("ReceiveDeferredMessages")
            .WithSummary("Retrieves deferred messages by sequence number.");

        receive.MapPost("/messages/{deliveryId:long}/complete", CompleteAsync)
            .WithName("CompleteMessage")
            .WithSummary("Settles a message successfully.");

        receive.MapPost("/messages/{deliveryId:long}/abandon", AbandonAsync)
            .WithName("AbandonMessage")
            .WithSummary("Returns a message for redelivery, optionally after a delay.");

        receive.MapPost("/messages/{deliveryId:long}/dead-letter", DeadLetterAsync)
            .WithName("DeadLetterMessage")
            .WithSummary("Moves a message to the dead-letter queue without further retries.");

        receive.MapPost("/messages/{deliveryId:long}/defer", DeferAsync)
            .WithName("DeferMessage")
            .WithSummary("Sets a message aside, retrievable only by sequence number.");

        receive.MapPost("/messages/{deliveryId:long}/renew-lock", RenewLockAsync)
            .WithName("RenewMessageLock")
            .WithSummary("Extends the peek-lock on a message being processed.");

        receive.MapPost("/dead-letter/receive", ReceiveDeadLetteredAsync)
            .WithName("ReceiveDeadLetteredMessages")
            .WithSummary("Reads the dead-letter queue without consuming the retry budget.");

        receive.MapPost("/dead-letter/replay", ReplayAsync)
            .WithName("ReplayDeadLetteredMessages")
            .WithSummary("Returns dead-lettered messages for another attempt with a fresh budget.");

        return routes;
    }

    private static async Task<Results<Ok<PublishResponseDto>, ProblemHttpResult>> PublishAsync(
        string topic,
        [FromBody] PublishRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        if (request.Messages.Count == 0)
        {
            return TypedResults.Problem(
                "A publish request must contain at least one message.",
                statusCode: StatusCodes.Status400BadRequest);
        }

        MessageEnvelope[] envelopes = [.. request.Messages.Select(m => m.ToEnvelope())];

        IReadOnlyList<PublishResult> results =
            await store.PublishAsync(topic, envelopes, cancellationToken);

        return TypedResults.Ok(new PublishResponseDto
        {
            Results = [.. results.Select(r => r.ToDto())],
        });
    }

    private static async Task<Results<Ok<ReceiveResponseDto>, ProblemHttpResult>> ReceiveAsync(
        string topic,
        string subscription,
        [FromBody] ReceiveRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<ReceivedMessage> received = await store.ReceiveAsync(
            new ReceiveRequest
            {
                Topic = topic,
                Subscription = subscription,
                MaxMessages = request.MaxMessages,
                MaxWaitTime = request.MaxWaitTime,
                SessionId = request.SessionId,
                ReceiverId = request.ReceiverId,
            },
            cancellationToken);

        // An empty result is a normal outcome of long-polling, not an error: the caller waited and
        // nothing arrived. Returning 200 with an empty list keeps that distinct from a fault.
        return TypedResults.Ok(new ReceiveResponseDto
        {
            Messages = [.. received.Select(m => m.ToDto())],
        });
    }

    private static async Task<Ok<ReceiveResponseDto>> ReceiveDeadLetteredAsync(
        string topic,
        string subscription,
        [FromBody] ReceiveRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<ReceivedMessage> received = await store.ReceiveAsync(
            new ReceiveRequest
            {
                Topic = topic,
                Subscription = subscription,
                MaxMessages = request.MaxMessages,
                MaxWaitTime = TimeSpan.Zero,
                ReceiverId = request.ReceiverId,
                FromDeadLetterQueue = true,
            },
            cancellationToken);

        return TypedResults.Ok(new ReceiveResponseDto
        {
            Messages = [.. received.Select(m => m.ToDto())],
        });
    }

    private static async Task<Ok<ReceiveResponseDto>> ReceiveDeferredAsync(
        string topic,
        string subscription,
        [FromBody] ReceiveDeferredRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<ReceivedMessage> received = await store.ReceiveDeferredAsync(
            topic,
            subscription,
            request.SequenceNumbers,
            request.ReceiverId,
            cancellationToken);

        return TypedResults.Ok(new ReceiveResponseDto
        {
            Messages = [.. received.Select(m => m.ToDto())],
        });
    }

    private static async Task<IResult> CompleteAsync(
        long deliveryId,
        [FromBody] SettleRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken) =>
        ToResult(await store.CompleteAsync(deliveryId, request.LockToken, cancellationToken));

    private static async Task<IResult> AbandonAsync(
        long deliveryId,
        [FromBody] SettleRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken) =>
        ToResult(await store.AbandonAsync(
            deliveryId,
            request.LockToken,
            request.PropertiesToModify,
            request.Delay,
            cancellationToken));

    private static async Task<IResult> DeadLetterAsync(
        long deliveryId,
        [FromBody] SettleRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken) =>
        ToResult(await store.DeadLetterAsync(
            deliveryId,
            request.LockToken,
            request.Reason ?? DeadLetterReason.ApplicationError,
            request.Description,
            cancellationToken));

    private static async Task<IResult> DeferAsync(
        long deliveryId,
        [FromBody] SettleRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken) =>
        ToResult(await store.DeferAsync(deliveryId, request.LockToken, cancellationToken));

    private static async Task<IResult> RenewLockAsync(
        long deliveryId,
        [FromBody] SettleRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        DateTimeOffset? renewedUntil =
            await store.RenewLockAsync(deliveryId, request.LockToken, cancellationToken);

        if (renewedUntil is null)
        {
            return LockLostProblem();
        }

        return TypedResults.Ok(new { lockedUntil = renewedUntil.Value });
    }

    private static async Task<Ok<object>> ReplayAsync(
        string topic,
        string subscription,
        [FromBody] ReplayRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        int replayed = await store.ReplayDeadLetteredAsync(
            topic,
            subscription,
            request.SequenceNumbers,
            request.MaxCount,
            cancellationToken);

        return TypedResults.Ok<object>(new { replayed });
    }

    private static async Task<IResult> CancelScheduledAsync(
        string topic,
        long sequenceNumber,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        bool cancelled = await store.CancelScheduledAsync(topic, sequenceNumber, cancellationToken);

        // Losing the race is not an error: the message simply became visible first.
        return TypedResults.Ok(new { cancelled });
    }

    /// <summary>
    /// Maps a settlement outcome onto a status code.
    /// </summary>
    /// <remarks>
    /// A lost lock is 409 Conflict rather than 404 or 500 because it is a concurrency outcome the
    /// caller can reason about: someone else now owns the message, and the work may be repeated.
    /// </remarks>
    private static IResult ToResult(SettlementResult result) => result switch
    {
        SettlementResult.Settled => TypedResults.NoContent(),
        SettlementResult.LockLost => LockLostProblem(),
        SettlementResult.NotFound => TypedResults.Problem(
            "No delivery with that identifier exists.",
            statusCode: StatusCodes.Status404NotFound),
        _ => TypedResults.Problem("Unrecognised settlement result."),
    };

    private static ProblemHttpResult LockLostProblem() => TypedResults.Problem(
        "The lock has expired or is no longer held. The message has returned to the subscription " +
        "and may already have been redelivered.",
        statusCode: StatusCodes.Status409Conflict,
        title: "Message lock lost");
}
