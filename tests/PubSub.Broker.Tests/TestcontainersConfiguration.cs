using System.Runtime.CompilerServices;

namespace PubSub.Broker.Tests;

/// <summary>
/// Configures Testcontainers before any container is created.
/// </summary>
/// <remarks>
/// Ryuk is the sidecar Testcontainers normally starts to reap containers if the test process dies
/// abruptly. Its image comes from Docker Hub, which this environment cannot reach, so it is turned
/// off here. The fixture disposes its own container in <c>DisposeAsync</c>, so the only cost is
/// that a hard kill of the test process could leave a container behind.
/// </remarks>
internal static class TestcontainersConfiguration
{
    [ModuleInitializer]
    internal static void Initialize()
    {
        // Only set when unset, so CI can still opt back into Ryuk where Docker Hub is reachable.
        if (Environment.GetEnvironmentVariable("TESTCONTAINERS_RYUK_DISABLED") is null)
        {
            Environment.SetEnvironmentVariable("TESTCONTAINERS_RYUK_DISABLED", "true");
        }
    }
}
