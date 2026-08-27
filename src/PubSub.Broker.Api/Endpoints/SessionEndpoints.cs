using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using PubSub.Broker.Core;

namespace PubSub.Broker.Api;

/// <summary>Session lifecycle endpoints.</summary>
public static class SessionEndpoints
{
    /// <summary>Maps the session routes.</summary>
    public static IEndpointRouteBuilder MapSessionEndpoints(this IEndpointRouteBuilder routes)
    {
        ArgumentNullException.ThrowIfNull(routes);

        RouteGroupBuilder sessions = routes
            .MapGroup("/topics/{topic}/subscriptions/{subscription}/sessions")
            .WithTags("Sessions")
            .RequireAuthorization(BrokerPolicies.Subscribe);

        sessions.MapPost("/accept", AcceptAsync)
            .WithName("AcceptSession")
            .WithSummary("Takes exclusive ownership of a session so its messages process in order.");

        sessions.MapPost("/{sessionId}/renew", RenewAsync)
            .WithName("RenewSessionLock")
            .WithSummary("Extends a session lock while its holder is still working.");

        sessions.MapPost("/{sessionId}/release", ReleaseAsync)
            .WithName("ReleaseSession")
            .WithSummary("Releases a session so another consumer can take it.");

        sessions.MapGet("/{sessionId}/state", GetStateAsync)
            .WithName("GetSessionState")
            .WithSummary("Reads the state a consumer stored against a session.");

        sessions.MapPut("/{sessionId}/state", SetStateAsync)
            .WithName("SetSessionState")
            .WithSummary("Stores state against a session, for checkpointing progress.");

        return routes;
    }

    private static async Task<IResult> AcceptAsync(
        string topic,
        string subscription,
        [FromBody] AcceptSessionRequestDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        AcceptedSession? session = await store.AcceptSessionAsync(
            topic,
            subscription,
            request.SessionId,
            request.ReceiverId,
            cancellationToken);

        // No session available is an ordinary outcome — every session is either busy or empty —
        // so it is 204, not an error.
        return session is null
            ? TypedResults.NoContent()
            : TypedResults.Ok(session.ToDto());
    }

    private static async Task<IResult> RenewAsync(
        string topic,
        string subscription,
        string sessionId,
        [FromBody] SessionStateDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        DateTimeOffset? renewed = await store.RenewSessionLockAsync(
            topic, subscription, sessionId, request.LockToken, cancellationToken);

        return renewed is null
            ? SessionLockLost(sessionId)
            : TypedResults.Ok(new { lockedUntil = renewed.Value });
    }

    private static async Task<IResult> ReleaseAsync(
        string topic,
        string subscription,
        string sessionId,
        [FromBody] SessionStateDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        bool released = await store.ReleaseSessionAsync(
            topic, subscription, sessionId, request.LockToken, cancellationToken);

        return released ? TypedResults.NoContent() : SessionLockLost(sessionId);
    }

    private static async Task<Ok<object>> GetStateAsync(
        string topic,
        string subscription,
        string sessionId,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        byte[]? state = await store.GetSessionStateAsync(
            topic, subscription, sessionId, cancellationToken);

        return TypedResults.Ok<object>(new
        {
            state = state is null ? null : Convert.ToBase64String(state),
        });
    }

    private static async Task<IResult> SetStateAsync(
        string topic,
        string subscription,
        string sessionId,
        [FromBody] SessionStateDto request,
        BrokerStore store,
        CancellationToken cancellationToken)
    {
        byte[]? state = string.IsNullOrEmpty(request.State)
            ? null
            : Convert.FromBase64String(request.State);

        bool stored = await store.SetSessionStateAsync(
            topic, subscription, sessionId, request.LockToken, state, cancellationToken);

        return stored ? TypedResults.NoContent() : SessionLockLost(sessionId);
    }

    private static ProblemHttpResult SessionLockLost(string sessionId) => TypedResults.Problem(
        $"The lock on session '{sessionId}' has expired or is no longer held. Its remaining " +
        "messages are available to other consumers.",
        statusCode: StatusCodes.Status409Conflict,
        title: "Session lock lost");
}
