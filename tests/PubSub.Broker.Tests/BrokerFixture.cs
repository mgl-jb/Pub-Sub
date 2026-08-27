using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PubSub.Broker.Core;
using Testcontainers.MsSql;

namespace PubSub.Broker.Tests;

/// <summary>
/// A real SQL Server in a container, shared across the tests in a collection.
/// </summary>
/// <remarks>
/// These tests deliberately do not use the EF in-memory provider. The behaviour under test —
/// <c>READPAST</c> claiming, row locking, unique-constraint races, identity ordering — exists only
/// in a real database engine. An in-memory provider would pass while proving nothing.
/// </remarks>
public sealed class BrokerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlServer =
        new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithCleanUp(true)
            .Build();

    private ServiceProvider? _services;

    /// <summary>A controllable clock, so lock expiry can be tested without waiting for it.</summary>
    public FakeClock Clock { get; } = new(new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero));

    /// <summary>The notifier under test; in-process is the default and the Redis fallback.</summary>
    public InProcessDeliveryNotifier Notifier { get; } = new();

    /// <summary>The container's connection string.</summary>
    public string ConnectionString => _sqlServer.GetConnectionString();

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        await _sqlServer.StartAsync();

        ServiceCollection services = new();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning).AddProvider(NullLoggerProvider.Instance));
        services.AddDbContext<BrokerDbContext>(
            options => options.UseSqlServer(ConnectionString),
            ServiceLifetime.Scoped);

        services.AddSingleton<TimeProvider>(Clock);
        services.AddSingleton<IDeliveryNotifier>(Notifier);
        services.AddSingleton<RuleSetCache>();
        services.AddScoped<ISweepCoordinator, SqlSweepCoordinator>();
        services.AddScoped<BrokerStore>();
        services.AddScoped<BrokerAdmin>();
        services.AddOptions<BrokerOptions>().Configure(o =>
        {
            // Tests drive time explicitly, so waiting is never the thing under test.
            o.LongPollInterval = TimeSpan.FromMilliseconds(50);
            o.MaxLongPollDuration = TimeSpan.FromSeconds(5);
        });

        _services = services.BuildServiceProvider();

        await using AsyncServiceScope scope = _services.CreateAsyncScope();
        BrokerDbContext context = scope.ServiceProvider.GetRequiredService<BrokerDbContext>();
        await context.Database.MigrateAsync();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_services is not null)
        {
            await _services.DisposeAsync();
        }

        await _sqlServer.DisposeAsync();
    }

    /// <summary>Creates a scope for one logical unit of work.</summary>
    public AsyncServiceScope CreateScope() =>
        (_services ?? throw new InvalidOperationException("The fixture is not initialised."))
        .CreateAsyncScope();

    /// <summary>Runs an action against a fresh scope's services.</summary>
    public async Task<T> WithScopeAsync<T>(Func<IServiceProvider, Task<T>> action)
    {
        await using AsyncServiceScope scope = CreateScope();
        return await action(scope.ServiceProvider);
    }

    /// <summary>Runs an action against a fresh scope's services.</summary>
    public async Task WithScopeAsync(Func<IServiceProvider, Task> action)
    {
        await using AsyncServiceScope scope = CreateScope();
        await action(scope.ServiceProvider);
    }

    /// <summary>Generates a name unique to one test, so tests can share the container safely.</summary>
    public static string UniqueName(string prefix) =>
        $"{prefix}-{Guid.NewGuid():n}"[..Math.Min(60, prefix.Length + 33)];
}

/// <summary>
/// A clock the tests advance by hand.
/// </summary>
/// <remarks>
/// Lock expiry, time to live, and scheduled delivery are all time-driven. Testing them against the
/// wall clock would mean sleeping for the real durations, which makes the suite slow and flaky;
/// advancing a fake clock makes the same assertions instant and deterministic.
/// </remarks>
public sealed class FakeClock : TimeProvider
{
    private DateTimeOffset _now;

    /// <summary>Creates the clock at a fixed instant.</summary>
    public FakeClock(DateTimeOffset start) => _now = start;

    /// <inheritdoc />
    public override DateTimeOffset GetUtcNow() => _now;

    /// <summary>Moves the clock forward.</summary>
    public void Advance(TimeSpan by) => _now = _now.Add(by);

    /// <summary>Moves the clock to a specific instant.</summary>
    public void Set(DateTimeOffset to) => _now = to;
}

/// <summary>Shares one SQL Server container across every broker test.</summary>
[CollectionDefinition(Name)]
public sealed class BrokerCollection : ICollectionFixture<BrokerFixture>
{
    /// <summary>The collection's name.</summary>
    public const string Name = "broker";
}
