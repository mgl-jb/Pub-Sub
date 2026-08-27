using System.Runtime.CompilerServices;

namespace PubSub.E2E.Tests;

/// <summary>Turns off the Testcontainers reaper, whose image this environment cannot pull.</summary>
internal static class TestcontainersConfiguration
{
    [ModuleInitializer]
    internal static void Initialize()
    {
        if (Environment.GetEnvironmentVariable("TESTCONTAINERS_RYUK_DISABLED") is null)
        {
            Environment.SetEnvironmentVariable("TESTCONTAINERS_RYUK_DISABLED", "true");
        }
    }
}
