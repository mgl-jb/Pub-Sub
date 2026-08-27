using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace PubSub.Broker.Core;

/// <summary>
/// Builds a context for the EF Core command-line tools.
/// </summary>
/// <remarks>
/// Used only when scaffolding or applying migrations from the CLI, which has no host to resolve
/// configuration from. The connection string is a design-time placeholder: migrations describe
/// schema, and generating them never touches a database.
/// </remarks>
public sealed class BrokerDbContextFactory : IDesignTimeDbContextFactory<BrokerDbContext>
{
    /// <inheritdoc />
    public BrokerDbContext CreateDbContext(string[] args)
    {
        string connectionString = Environment.GetEnvironmentVariable("PUBSUB_BROKER_CONNECTION")
                                  ?? "Server=(localdb)\\MSSQLLocalDB;Database=PubSubBroker;Trusted_Connection=True;";

        DbContextOptionsBuilder<BrokerDbContext> builder = new();
        builder.UseSqlServer(connectionString);

        return new BrokerDbContext(builder.Options);
    }
}
