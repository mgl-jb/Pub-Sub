# 8. Target .NET 10 LTS

Status: Accepted

## Context

The system needs a runtime version, and messaging infrastructure is long-lived: it is deployed once
and then depended on by everything else, so an unsupported runtime becomes everyone's problem
rather than just its owner's.

## Decision

Target .NET 10, the current LTS release. Pin package versions centrally through
`Directory.Packages.props`, and pin the SDK through `global.json`.

## Consequences

Long-term support means security patches without a framework migration during the window when this
is least convenient to touch.

The concrete alternative was .NET 11, which is in preview: ASP.NET Core 11 has no stable release,
and building messaging infrastructure on a preview runtime trades a real support guarantee for
features this system does not need.

Central package management means one place to see and change every version, and
`CentralPackageTransitivePinningEnabled` closes the gap where a transitive dependency drifts to an
unreviewed version.

`global.json` pins the SDK feature band with `rollForward: latestFeature`, so CI and a developer
machine build with compatible tooling rather than whatever each happens to have installed.

Warnings are errors across the solution. This is stricter than most projects choose, and it earned
its place during the build: the analysers caught a real bug where a conditional expression silently
widened every integer arithmetic result to `decimal`, which no test would have noticed and which
would have quietly changed the type of every value a rule action assigned.

## Alternatives considered

**.NET 11 preview.** Newest features. Rejected: no stable ASP.NET Core release, and no support
guarantee for infrastructure meant to outlast several application release cycles.

**.NET 8 LTS.** Still supported and widely deployed. Rejected because .NET 10 is the current LTS,
so choosing 8 would start the project a full support cycle behind for no benefit.
