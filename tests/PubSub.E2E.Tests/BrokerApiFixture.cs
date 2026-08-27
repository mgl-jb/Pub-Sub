using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using PubSub.Broker.Core;
using Testcontainers.MsSql;

namespace PubSub.E2E.Tests;

/// <summary>
/// The real broker API, hosted in-process over a real SQL Server, with the real client talking to
/// it over HTTP.
/// </summary>
/// <remarks>
/// Nothing between the client and the database is faked. That is the point: the unit tests already
/// cover each layer, and what remains unproven is whether serialization, routing, authorization,
/// and settlement actually line up across the wire.
/// </remarks>
public sealed class BrokerApiFixture : WebApplicationFactory<Program>, IAsyncLifetime
{
    private readonly MsSqlContainer _sqlServer =
        new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithCleanUp(true)
            .Build();

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        await _sqlServer.StartAsync();

        // The API reads its connection string in top-level statements, before
        // WebApplicationFactory's ConfigureAppConfiguration hooks can run. Environment variables
        // are read by CreateBuilder itself, so they are the one source that lands early enough.
        Environment.SetEnvironmentVariable(
            "ConnectionStrings__Broker", _sqlServer.GetConnectionString());

        // Authentication is exercised separately; these tests are about the message plane.
        Environment.SetEnvironmentVariable("Broker__DisableAuthentication", "true");

        // Keep long-polling short so an empty receive does not stretch the suite out.
        Environment.SetEnvironmentVariable("Broker__LongPollInterval", "00:00:00.100");
        Environment.SetEnvironmentVariable("Broker__MaxLongPollDuration", "00:00:05");

        // Force the host to build now, so migrations run before the first test rather than
        // inside whichever test happens to make the first request.
        using IServiceScope scope = Services.CreateScope();
        BrokerDbContext context = scope.ServiceProvider.GetRequiredService<BrokerDbContext>();
        await context.Database.MigrateAsync();
    }

    /// <inheritdoc />
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.UseEnvironment("Testing");
    }

    /// <summary>Disposes the API host and the database container.</summary>
    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        await _sqlServer.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    /// <summary>Generates a name unique to one test.</summary>
    public static string UniqueName(string prefix) => $"{prefix}-{Guid.NewGuid():n}"[..24];
}

/// <summary>Shares one hosted broker across the end-to-end tests.</summary>
[CollectionDefinition(Name)]
public sealed class BrokerApiCollection : ICollectionFixture<BrokerApiFixture>
{
    /// <summary>The collection's name.</summary>
    public const string Name = "broker-api";
}
