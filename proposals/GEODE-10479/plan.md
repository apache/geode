# GEODE-10479 — remaining implementation plan

This revised proposal replaces the original generated implementation prompts. They are not
additional project requirements. The public issue remains the authority for acceptance criteria;
changes to those criteria require community agreement.

1. Review [10531](GEODE-10531.md): preserve subprocess policy with a narrow removal-warning
   exception, or explicitly agree retirement. Correct the Java 21 premise and stale completion claims.
2. Coordinate with the authors of [10533](GEODE-10533.md) and [10534](GEODE-10534.md) on
   the remaining `IndexType` and Swagger API uses. Do not redo merged replacements.
3. On current develop, temporarily enable removal/deprecation warnings and capture a clean-build
   inventory with revision, JDK, Gradle version and commands. Historical counts are obsolete.
4. Resolve warnings module by module, documenting only agreed compatibility/dependency exceptions.
5. Remove global suppressions, set `options.deprecation = true`, and validate CI rejection of new
   warnings. Keep the work coordinated with the separate Gradle/Java migration.
6. Complete the parent's test, documentation and performance evidence before closure, or agree
   explicit follow-up scope where an acceptance criterion changes.

See [todo.md](todo.md) for current delivery status. No dashboards, new RFC infrastructure,
blanket API rewrites or additional tracking systems are required by this proposal.
