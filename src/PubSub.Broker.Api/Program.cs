using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.EntityFrameworkCore;
using PubSub.Abstractions;
using PubSub.Broker.Api;
using PubSub.Broker.Core;
using PubSub.Broker.Redis;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

string connectionString = builder.Configuration.GetConnectionString("Broker")
                          ?? throw new InvalidOperationException(
                              "No 'Broker' connection string is configured.");

builder.Services.AddPubSubBrokerOptions(builder.Configuration);
builder.Services.AddPubSubBroker(connectionString);

// Registered before the broker's own defaults so the Redis implementations win where Redis is
// configured, and the in-process ones remain in place where it is not.
builder.Services.AddPubSubRedis(builder.Configuration);
builder.Services.AddPubSubSweeper();

builder.AddPubSubObservability();

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase;
    options.SerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
});

builder.Services.AddOpenApi();
builder.Services.AddProblemDetails();

builder.Services.AddHealthChecks()
    .AddDbContextCheck<BrokerDbContext>("broker-database");

AddAuthentication(builder);

WebApplication app = builder.Build();

// The broker owns its schema, so it applies its own migrations on startup: a fresh database
// otherwise leaves the service running but unable to answer anything. Set
// Broker:ApplyMigrationsOnStartup to false where a deployment pipeline owns migrations instead,
// which also lets the runtime identity drop its DDL permission.
if (builder.Configuration.GetValue("Broker:ApplyMigrationsOnStartup", defaultValue: true))
{
    using IServiceScope migrationScope = app.Services.CreateScope();
    BrokerDbContext database = migrationScope.ServiceProvider.GetRequiredService<BrokerDbContext>();
    await database.Database.MigrateAsync();
}

// Every broker error already carries the right status code and a message written for the caller;
// this turns them into Problem Details rather than letting them surface as an opaque 500.
app.UseExceptionHandler(handler => handler.Run(WriteProblemAsync));

app.UseAuthentication();
app.UseAuthorization();

app.MapOpenApi();

app.MapMessageEndpoints();
app.MapSessionEndpoints();
app.MapAdminEndpoints();

// Liveness answers "is the process up"; readiness additionally answers "can it reach its
// database". Keeping them apart stops a transient database blip from having the orchestrator
// restart a perfectly healthy process.
app.MapHealthChecks("/health/live", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = _ => false,
}).AllowAnonymous();

app.MapHealthChecks("/health/ready").AllowAnonymous();

await app.RunAsync();

static void AddAuthentication(WebApplicationBuilder builder)
{
    // Entra ID is the intended production identity provider, and managed identity means no
    // secrets are stored for the database or cache either. Authentication is only switched off
    // when explicitly configured, so it cannot be disabled by omission.
    bool authenticationDisabled =
        builder.Configuration.GetValue("Broker:DisableAuthentication", defaultValue: false);

    if (authenticationDisabled)
    {
        builder.Services.AddAuthentication(BrokerAuthentication.AnonymousScheme)
            .AddScheme<Microsoft.AspNetCore.Authentication.AuthenticationSchemeOptions, AnonymousAuthenticationHandler>(
                BrokerAuthentication.AnonymousScheme, _ => { });

        builder.Services.AddAuthorizationBuilder()
            .AddPolicy(BrokerPolicies.Publish, policy => policy.RequireAssertion(_ => true))
            .AddPolicy(BrokerPolicies.Subscribe, policy => policy.RequireAssertion(_ => true))
            .AddPolicy(BrokerPolicies.Admin, policy => policy.RequireAssertion(_ => true));

        return;
    }

    builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
        .AddJwtBearer(options =>
        {
            options.Authority = builder.Configuration["Broker:Authority"];
            options.Audience = builder.Configuration["Broker:Audience"];
            options.TokenValidationParameters.ValidateIssuer = true;
            options.TokenValidationParameters.ValidateAudience = true;
            options.TokenValidationParameters.ValidateLifetime = true;
        });

    // A capability is granted by either a delegated scope or an application role, so the same
    // policy covers a user-facing app and a daemon using client credentials.
    builder.Services.AddAuthorizationBuilder()
        .AddPolicy(BrokerPolicies.Publish, policy =>
            policy.RequireAssertion(context => context.User.HasScopeOrRole(BrokerPolicies.Publish)))
        .AddPolicy(BrokerPolicies.Subscribe, policy =>
            policy.RequireAssertion(context => context.User.HasScopeOrRole(BrokerPolicies.Subscribe)))
        .AddPolicy(BrokerPolicies.Admin, policy =>
            policy.RequireAssertion(context => context.User.HasScopeOrRole(BrokerPolicies.Admin)));
}

static async Task WriteProblemAsync(HttpContext context)
{
    Exception? error = context.Features.Get<IExceptionHandlerFeature>()?.Error;

    (int status, string title) = error switch
    {
        EntityNotFoundException => (StatusCodes.Status404NotFound, "Entity not found"),
        EntityAlreadyExistsException => (StatusCodes.Status409Conflict, "Entity already exists"),
        MessageLockLostException => (StatusCodes.Status409Conflict, "Message lock lost"),
        SessionLockLostException => (StatusCodes.Status409Conflict, "Session lock lost"),
        FilterSyntaxException => (StatusCodes.Status400BadRequest, "Invalid filter expression"),
        InvalidOperationForStateException => (StatusCodes.Status409Conflict, "Invalid for current state"),
        ArgumentException => (StatusCodes.Status400BadRequest, "Invalid request"),
        FormatException => (StatusCodes.Status400BadRequest, "Malformed request"),
        BrokerUnavailableException => (StatusCodes.Status503ServiceUnavailable, "Broker unavailable"),
        _ => (StatusCodes.Status500InternalServerError, "Unexpected error"),
    };

    context.Response.StatusCode = status;

    // The message is echoed for errors the caller can act on. An unexpected fault is not echoed,
    // because its text may describe internals the caller has no business seeing.
    string detail = status == StatusCodes.Status500InternalServerError
        ? "An unexpected error occurred while processing the request."
        : error?.Message ?? title;

    await Results.Problem(detail, statusCode: status, title: title).ExecuteAsync(context);
}

/// <summary>Exposes the generated entry point to the integration tests.</summary>
public partial class Program;
