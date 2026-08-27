using System.Security.Claims;
using System.Text.Encodings.Web;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PubSub.Broker.Api;

/// <summary>Authentication scheme names.</summary>
public static class BrokerAuthentication
{
    /// <summary>The scheme used when authentication is deliberately switched off.</summary>
    public const string AnonymousScheme = "Anonymous";
}

/// <summary>
/// Grants every request a synthetic identity, for local development and tests.
/// </summary>
/// <remarks>
/// Only reachable when <c>Broker:DisableAuthentication</c> is explicitly set. It is opt-in rather
/// than a fallback so that a missing identity-provider configuration fails closed — an
/// unauthenticated broker should never be something you get by forgetting to configure one.
/// </remarks>
public sealed class AnonymousAuthenticationHandler : AuthenticationHandler<AuthenticationSchemeOptions>
{
    /// <summary>Creates the handler.</summary>
    public AnonymousAuthenticationHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder)
        : base(options, logger, encoder)
    {
    }

    /// <inheritdoc />
    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        ClaimsIdentity identity = new(
            [
                new Claim(ClaimTypes.Name, "local-development"),
                new Claim("roles", BrokerPolicies.Publish),
                new Claim("roles", BrokerPolicies.Subscribe),
                new Claim("roles", BrokerPolicies.Admin),
            ],
            BrokerAuthentication.AnonymousScheme);

        AuthenticationTicket ticket = new(
            new ClaimsPrincipal(identity),
            BrokerAuthentication.AnonymousScheme);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}

/// <summary>Claim inspection helpers.</summary>
public static class ClaimsPrincipalExtensions
{
    /// <summary>
    /// Whether the principal carries a capability as either a delegated scope or an application
    /// role.
    /// </summary>
    /// <remarks>
    /// Entra ID puts delegated permissions in <c>scp</c> as a space-separated string and
    /// application permissions in <c>roles</c> as separate claims. Accepting either lets one
    /// policy serve a user-facing app and a daemon alike.
    /// </remarks>
    public static bool HasScopeOrRole(this ClaimsPrincipal principal, string capability)
    {
        ArgumentNullException.ThrowIfNull(principal);

        foreach (Claim claim in principal.FindAll("roles"))
        {
            if (string.Equals(claim.Value, capability, StringComparison.Ordinal))
            {
                return true;
            }
        }

        if (principal.IsInRole(capability))
        {
            return true;
        }

        foreach (Claim claim in principal.FindAll("scp"))
        {
            foreach (string scope in claim.Value.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            {
                if (string.Equals(scope, capability, StringComparison.Ordinal))
                {
                    return true;
                }
            }
        }

        return false;
    }
}
