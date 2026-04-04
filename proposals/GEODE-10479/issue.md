h3. Current State

Following the successful Java 17 migration (GEODE-10465), all deprecation and removal warnings have been suppressed to ensure build stability during the transition. The current suppression configuration includes:

*In warnings.gradle:* 

tasks.withType(JavaCompile) \{ options.compilerArgs << '-Xlint:-unchecked' << "-Werror" << '-Xlint:-deprecation' << '-Xlint:-removal' options.deprecation = false }

*In geode-java.gradle:* 

options.compilerArgs.addAll([ '-Xlint:-removal', '-Xlint:-deprecation' ])
h3. Problem Statement

The current suppression of all deprecation warnings creates technical debt and prevents the codebase from:
 * Leveraging modern Java 17+ APIs and features
 * Identifying potentially broken code due to API removals
 * Maintaining code quality standards
 * Preparing for future Java version upgrades

h3. Proposed Solution
h4. Phase 1: Assessment and Categorization ( <1 week )
 * *Baseline Analysis*
 ** Remove warning suppressions temporarily on a test branch
 ** Generate comprehensive report of all deprecation and removal warnings
 ** Categorize warnings by:
 *** {*}Critical{*}: API removal warnings (will break in future Java versions)
 *** {*}High Priority{*}: Security-related deprecated APIs
 *** {*}Medium Priority{*}: Performance-impacting deprecated APIs
 *** {*}Low Priority{*}: General deprecated APIs with modern alternatives
 * *Module-by-Module Impact Assessment*
 ** Identify modules with highest warning concentration
 ** Document external dependency deprecations vs. internal code issues
 ** Create priority matrix for remediation effort

h4. Phase 2: Incremental Warning Re-enablement ( <1 week)
 * *Start with Removal Warnings* (Week 1-2) Re-enable only removal warnings first (highest priority): options.compilerArgs << '-Xlint:removal'
 ** Address API removal issues that will break in future Java versions
 ** Replace removed APIs with modern alternatives
 ** Focus on critical functionality first
 * *Enable Deprecation Warnings by Module* (Week 3-6) Enable deprecation warnings module by module: if (project.name in ['geode-core', 'geode-common']) \{ options.deprecation = true options.compilerArgs << '-Xlint:deprecation' }
 ** Start with core modules with fewer dependencies
 ** Gradually expand to more complex modules

h4. Phase 3: API Modernization (1-2 weeks)
 * *Security API Updates*
 ** Replace deprecated security manager APIs
 ** Update SSL/TLS configuration APIs
 ** Modernize authentication mechanisms
 * *Collections and Concurrency*
 ** Replace deprecated collection methods
 ** Update concurrent API usage
 ** Leverage Java 17 concurrency improvements
 * *I/O and Networking*
 ** Replace deprecated networking APIs
 ** Update file I/O operations
 ** Leverage NIO.2 improvements
 * *Reflection and Introspection*
 ** Update reflection API usage for module system compatibility
 ** Replace deprecated introspection methods
 ** Add proper module exports where needed

h4. Phase 4: Full Warning Compliance (2 weeks)
 * *Remove All Suppressions* Final configuration with all warnings enabled: tasks.withType(JavaCompile) \{ options.compilerArgs << '-Xlint:unchecked' << "-Werror" << '-Xlint:deprecation' << '-Xlint:removal' options.deprecation = true }
 * *Establish Warning Gates*
 ** Configure CI/CD to fail on new deprecation warnings
 ** Add checkstyle rules to prevent deprecated API introduction
 ** Document approved exceptions with justification

h3. Acceptance Criteria
 *  All '-Xlint:-removal' suppressions removed and underlying issues resolved
 *  All '-Xlint:-deprecation' suppressions removed and underlying issues resolved
 *  'options.deprecation = false' changed to 'options.deprecation = true'
 *  Zero deprecation warnings in clean build
 *  Zero removal warnings in clean build
 *  CI/CD pipeline fails on new deprecation/removal warnings
 *  Documentation updated with modern API usage patterns
 *  Performance benchmarks show no regression from API changes

h3. Implementation Strategy
 # *Create Feature Branch* 
 # {*}Incremental PRs{*}: Submit changes module by module for easier review
 # {*}Parallel Development{*}: Allow normal development to continue while cleanup progresses
 # {*}Testing Strategy{*}: Ensure all existing tests pass after each modernization change
 # {*}Rollback Plan{*}: Maintain ability to temporarily suppress warnings if blocking issues discovered

h3. Estimated Effort
 * {*}Total Effort{*}: 2-3 weeks
 * {*}Team Size{*}: 2-3 developers
 * {*}Risk Level{*}: Medium (phased approach minimizes disruption)

h3. Benefits
 * {*}Code Quality{*}: Modern, maintainable codebase using current Java 17 APIs
 * {*}Future Compatibility{*}: Preparation for Java 18+ upgrades
 * {*}Performance{*}: Potential improvements from modern API usage
 * {*}Security{*}: Updated security APIs and practices
 * {*}Developer Experience{*}: Cleaner build output and better IDE warnings

h3. Dependencies
 * Requires completion of GEODE-10465 (Java 17 migration)
 * May require coordination with external dependency updates
 * Should align with any planned Gradle or build system upgrades

h3. Success Metrics
 * Zero suppressed deprecation warnings
 * Build time maintained or improved
 * Test suite execution time maintained or improved
 * No functional regressions in existing features
 * Documentation updated with modern patterns

 

 
