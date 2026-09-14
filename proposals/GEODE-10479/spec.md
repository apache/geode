# GEODE-10479 — scope and warning policy proposal

The goal is to re-enable compiler deprecation/removal warnings while preserving agreed Geode
behavior. Merging individual API replacements alone does not complete this work.

## Existing acceptance criteria

The [public issue](https://issues.apache.org/jira/browse/GEODE-10479) requires removing global
`-Xlint:-removal` and `-Xlint:-deprecation` suppressions, enabling `options.deprecation`, a clean
build without those warnings, CI gates, updated documentation and no performance regression.
These outcomes have not been demonstrated. This branch does not amend the public issue.

## Proposed exceptions

The parent plan allows documenting justified exceptions. Propose exceptions at the smallest
source scope, with the reason, tracking issue and condition for removal; do not use a global
suppression to hide unrelated warnings. Review each exception with the community.

For [10531](GEODE-10531.md), retaining the target executable permission check preserves a
configured legacy SecurityManager policy that ProcessBuilder's shell check does not preserve.
The proposed local suppression remains until an explicit compatibility decision retires or
replaces that behavior. It is not an assertion that SecurityManager is a modern security API.

## Validation

Record the source revision, build JDK, runtime JDK and warning flags. A focused Java 17/21
process test cannot establish a full-project warning baseline, Windows behavior or JMX
integration. Use tests appropriate to each changed API and the parent's eventual closure scope.

See [plan.md](plan.md) for sequencing and [todo.md](todo.md) for actual delivery status.
