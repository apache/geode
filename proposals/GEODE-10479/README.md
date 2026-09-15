# GEODE-10479: remaining warning cleanup and compatibility decision

Status reconciled September 13, 2026 against develop `8295390505a7` and public
Jira/PR discussions. This proposal does not change Jira status or establish
community approval. Consult the linked issues for subsequent decisions.

## Work already delivered

| Issue | Merged work | Remaining scope |
|---|---|---|
| [GEODE-10532](https://issues.apache.org/jira/browse/GEODE-10532) | HTTP status replacement, [#7967](https://github.com/apache/geode/pull/7967); Jira resolved | Complete for this bounded ticket |
| [GEODE-10533](https://issues.apache.org/jira/browse/GEODE-10533) | gfsh replacements, [#8000](https://github.com/apache/geode/pull/8000) and [#8001](https://github.com/apache/geode/pull/8001) | Contributor identified remaining `IndexType` deprecation and suggested a separate ticket; agree and record its disposition |
| [GEODE-10534](https://issues.apache.org/jira/browse/GEODE-10534) | Support-module replacements, [#7983](https://github.com/apache/geode/pull/7983) | `SwaggerConfig` retains `setMatchOptionalTrailingSeparator(true)`, with replacement deferred until Spring 6.2+ |

The remaining gfsh and Swagger work should be coordinated with their contributors.
These merges did not depend on GEODE-10531. Their former implementation plans are
superseded by the merged code and linked discussions.

## GEODE-10531: proposed compatibility exception

[GEODE-10531](https://issues.apache.org/jira/browse/GEODE-10531) and
[#7971](https://github.com/apache/geode/pull/7971) address `OSProcess.bgexec`.
The issue's original claim that SecurityManager was removed in Java 21 is
incorrect: [Java 21 retains the API, deprecated for removal](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/SecurityManager.html).
This is a warning and compatibility decision, not the claimed missing-API blocker.

`bgexec` checks permission for the requested executable, then launches it through
a shell. [ProcessBuilder checks its first command](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/lang/ProcessBuilder.html#start()),
which here is the shell. Removing the explicit target check therefore changes
configured SecurityManager policy; OS permissions do not preserve that policy.

The revised implementation retains the check at its original location and scopes
`@SuppressWarnings("removal")` to the local variable declaration. The proposed
exception preserves existing behavior while allowing that statement to compile
with removal warnings enabled. Revisit it when legacy SecurityManager behavior
is explicitly retired or replaced.

The community must still agree the exception and revise 10531's acceptance
criteria, which currently require complete removal. Proposed criteria are:

- Preserve denial of the requested executable before shell launch and normal
  launch without a SecurityManager.
- Compile this compatibility statement without removal warnings on Java 17/21;
  keep the suppression local and document its reason and retirement condition.
- Cover both behaviors in a separate JVM, keeping the test policy out of other
  tests. See [OSProcessSecurityIntegrationTest](../../geode-logging/src/integrationTest/java/org/apache/geode/logging/internal/OSProcessSecurityIntegrationTest.java).
- Record the agreed policy in the issue and PR description before merge.

This change does not enable globally suppressed warnings or complete 10479.

## What remains before closing GEODE-10479

The [parent issue](https://issues.apache.org/jira/browse/GEODE-10479) remains open.
After agreeing 10531's disposition and the two residual API items:

1. Produce a fresh warning inventory on current source, recording the revision,
   JDK, Gradle version and commands. Historical counts are not a current baseline.
2. Resolve warnings incrementally and record any agreed, narrowly scoped
   compatibility/dependency exceptions with their retirement conditions.
3. Remove global `-Xlint:-removal` and `-Xlint:-deprecation` suppressions, set
   `options.deprecation = true`, and enforce the agreed warning policy in CI.
4. Supply the parent's clean-build, test, documentation and performance evidence,
   or agree explicit acceptance-criteria amendments with recorded follow-up scope.

Focused process tests do not establish a full-project warning baseline or complete
platform/JMX compatibility. SBOM remains a separate initiative, GEODE-10481.
