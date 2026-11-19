# .nuget-feed — committed folder feed

`Pomelo.EntityFrameworkCore.MySql.10.0.0.nupkg` — upstream build
(PomeloFoundation, commit `86bbaa2e`, PR #2019). Required by
`Sufficit.EFData >= 1.26.914` (floated by the net10.0 asset of this
package) while nuget.org still carries no 10.x (nearest there: 9.0.0),
which made every cold-cache restore fail with NU1102.

The netstandard2.0 target uses the Sufficit EF Core 3.1 provider
`Pomelo.EntityFrameworkCore.MySql.3.2.8.1` from commit `d3e94fab6a46`.
This build allows MySqlConnector 2.6.1 and is also vendored by Standard and EFData.
SHA-256 `7e6961c13d723cc00a22ebd821e8221388843eb3f4467bd9800744d62cb1dd17`.
