# Sufficit Statistics

Statistics collection and routing for the Sufficit platform, extracted from
`sufficit-standard` so that services (like `sufficit-ai`) can consume metrics
without a source dependency on the whole Standard library.

Follows the same model as `sufficit-events` / `sufficit-efdata`: a small
versioned NuGet package (`Sufficit.Statistics`), namespace preserved.

## What lives here

- **`StatisticsGeneralController`** — the event-bus metric sink: consumes `Metric`
  events (`IEventHandler<Metric>` via `Sufficit.Events`), caches them and
  batches to the configured output provider.
- **Output providers** — `VictoriaMetricsProvider`, `InfluxDbMetricsProvider`,
  `CompositeMetricsProvider` (implements `IMetricsProvider` from `Sufficit.EFData`).
- **`EventBusMetricsExtensions`** — `PublishMetricAsync` helpers on `IEventBus`.
- **DI extensions** — `AddSufficitStatistics()`, provider-specific registrations
  and configuration bindings.

The metric data model (`Metric`, `MetricsSearchParameters`, `IMetricsProvider`)
and the EF persistence live in **`Sufficit.EFData`**; this package only routes
and delivers.

Also hosts the self-contained tracking primitives from `Sufficit.Logging`
(`ITracking`, `LogStopWatch` and extensions) used by the AI transcription and
translation legacy endpoints — pure BCL, no other dependencies.

## Usage

```csharp
// after AddSufficitEvents(...)
services.AddSufficitStatistics(configuration);
```

## Build

```
dotnet build src/Sufficit.Statistics.csproj -c Release
```

Pushes to `main` build, pack and publish the package (requires the
`NUGET_API_KEY` repository secret; the publish step is skipped while the secret
is absent).
