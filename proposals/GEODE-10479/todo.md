# GEODE-10479 — current status and remaining work

Status reconciled September 13, 2026 against develop `8295390505a7` and public Jira/PR discussions.
This branch proposes an updated plan; it does not change Jira status or establish community approval.
The parent remains open. Historical warning counts are not a current build baseline.

| Child | Landed work | Remaining work |
|---|---|---|
| [10531](GEODE-10531.md) | None from PR #7971 yet | Review the revised compatibility exception and regression test; agree revised issue scope |
| [10532](GEODE-10532.md) | PR #7967; Jira Resolved | Complete for the HTTP status replacement |
| [10533](GEODE-10533.md) | PRs #8000 and #8001 | Record a disposition for the remaining `IndexType` deprecation; reconcile Jira |
| [10534](GEODE-10534.md) | PR #7983 | Resolve or explicitly defer the Swagger path-matching API; reconcile Jira |

## Next steps

- [ ] Agree 10531's compatibility exception or an explicit retirement of the existing behavior.
- [ ] Resolve or record follow-up scope for the gfsh and Swagger residuals with their contributors.
- [ ] Produce a fresh warning inventory with the global suppressions temporarily disabled.
- [ ] Agree justified, narrowly scoped exceptions and their retirement conditions.
- [ ] Enable removal/deprecation warnings incrementally; remove global suppressions.
- [ ] Set `options.deprecation = true` and enforce agreed warnings in CI.
- [ ] Validate the clean build, relevant tests, documentation and performance acceptance criteria.
- [ ] Close 10479 only after meeting its criteria or agreeing explicit amendments and follow-ups.

The SecurityManager change is independent of SBOM (GEODE-10481) and was never required for
10532–10534 to merge. See [plan](plan.md), [scope](spec.md) and [parent issue](issue.md).
