namespace PubSub.Broker.Api;

/// <summary>
/// Authorization policy names, one per capability rather than one per endpoint.
/// </summary>
/// <remarks>
/// Splitting publish from subscribe from administration means a credential can be scoped to what a
/// service actually does: a producer that can publish cannot drain a subscription, and neither can
/// delete a topic.
/// </remarks>
public static class BrokerPolicies
{
    /// <summary>Permits publishing to topics.</summary>
    public const string Publish = "PubSub.Publish";

    /// <summary>Permits receiving and settling messages.</summary>
    public const string Subscribe = "PubSub.Subscribe";

    /// <summary>Permits creating and deleting topics, subscriptions, and rules.</summary>
    public const string Admin = "PubSub.Admin";
}
