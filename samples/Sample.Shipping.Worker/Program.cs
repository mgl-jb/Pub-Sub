// Placeholder host. Replaced when the sample consumers land.
HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);
IHost host = builder.Build();
await host.RunAsync();
