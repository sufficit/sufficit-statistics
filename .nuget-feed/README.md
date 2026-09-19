# .nuget-feed — committed folder feed

`Pomelo.EntityFrameworkCore.MySql.10.0.0.nupkg` — upstream build
(PomeloFoundation, commit `86bbaa2e`, PR #2019). Required by
`Sufficit.EFData >= 1.26.914` (floated by the net10.0 asset of this
package) while nuget.org still carries no 10.x (nearest there: 9.0.0),
which made every cold-cache restore fail with NU1102.

TEMPORARY: when Pomelo ships 10.x on nuget.org (issue #2007), delete
this folder and the `pomelo-10-feed` mapping in `../nuget.config`.
