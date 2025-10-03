# Apache Geode Java 17 Deprecation Warning Removal - Technical Specification

## Overview

This specification details the complete removal of deprecation and removal warning suppressions from the Apache Geode codebase following the Java 17 migration (GEODE-10465). The goal is to modernize the codebase to leverage Java 17+ APIs while maintaining zero warnings and ensuring no functional regressions.

## Background

Following the Java 17 migration, all deprecation and removal warnings were suppressed to ensure build stability. Current suppressions include:
- `warnings.gradle`: `-Xlint:-unchecked`, `-Xlint:-deprecation`, `-Xlint:-removal`, `options.deprecation = false`
- `geode-java.gradle`: `-Xlint:-removal`, `-Xlint:-deprecation`

## Success Criteria

- **Zero tolerance**: All modules must achieve zero warnings (both deprecation and removal)
- **No functional regressions**: All existing tests must pass
- **Performance maintained**: No degradation in build/test/runtime performance
- **Community approval**: All API changes approved through RFC process

## Implementation Strategy

### Phase 1: Assessment and Categorization (Week 1)

#### Baseline Analysis Tooling
**Approach**: Hybrid custom script + existing tooling integration

**Custom Gradle Task Implementation**:
```gradle
task generateWarningReport {
    doLast {
        // Temporarily remove suppressions
        // Capture compiler warnings to structured JSON/CSV
        // Categorize by: warning type, module, severity, API category
        // Generate dashboard-style reports
    }
}
```

**Integration Points**:
- SonarQube for ongoing tracking and trend analysis
- Gradle's built-in reporting for CI integration
- Automated categorization based on warning patterns

**Warning Categories**:
- **Critical**: API removal warnings (will break in future Java versions)
- **High Priority**: Security-related deprecated APIs
- **Medium Priority**: Performance-impacting deprecated APIs  
- **Low Priority**: General deprecated APIs with modern alternatives

#### Module Prioritization Strategy
**"Fewest Warnings First" Approach**:
1. Generate warning count per module
2. Sort modules by ascending warning count
3. Process modules in order: lowest count → highest count
4. Build momentum with quick wins before tackling complex modules

### Phase 2: Incremental Warning Re-enablement (Weeks 2-4)

#### Warning Enablement Strategy
**Hybrid Approach with Interface Contracts**:

1. **Global Removal Warnings**: Re-enable removal warnings globally first
   ```gradle
   options.compilerArgs << '-Xlint:removal'
   ```

2. **Graduated Deprecation Enablement**: Enable deprecation warnings module-by-module after dependencies are clean
   ```gradle
   if (project.name in processedModules) {
       options.deprecation = true
       options.compilerArgs << '-Xlint:deprecation'
   }
   ```

3. **Interface Contracts**: 
   - Public APIs between modules must be deprecation-free
   - Internal implementation can temporarily use `@SuppressWarnings("deprecation")` with TODO comments
   - All suppressions must link to tracking tickets

#### Module Completion Criteria
**Zero Warning Requirement**: A module is considered complete only when it has zero warnings of any type.

**Exception Handling Process**:
- **Third-party dependency issues**: Evaluate case-by-case
  - Propose community upgrade if possible
  - Document suppressions with justification if upgrade not feasible
  - Consider replacing dependencies if breaking changes acceptable
- **Legacy compatibility requirements**: Isolate into separate compatibility modules
- **Platform-specific constraints**: Document and track for future resolution

### Phase 3: API Modernization (Weeks 3-5)

#### Community Proposal Process
**RFC Structure**: Module-by-module RFCs organized by functional areas within each module

**RFC Organization**:
- **Location**: `@proposals/GEODE-XXXXX-module-name-deprecation-removal/`
- **Format**: Standard RFC format
- **Scope**: One RFC per module (following "fewest warnings first" order)
- **Content Structure**: Organized by functional areas within each module:
  - Security API Updates
  - Collections and Concurrency
  - I/O and Networking  
  - Reflection and Introspection

**Required RFC Content**:
- Before/after code examples for each API change
- Performance impact analysis
- Migration guide for downstream users
- Rollback procedures if issues discovered
- Justification for chosen replacement APIs

#### API Replacement Decision Criteria
**Community Review Required**: All deprecated API replacements must go through community proposal and review process.

**Selection Criteria** (when multiple alternatives exist):
1. Future-proofing (stability in future Java versions)
2. API consistency (fits with existing Geode patterns)
3. Performance characteristics
4. Community standards (what other Apache projects use)
5. Migration effort (least disruptive change)

### Phase 4: Full Warning Compliance (Week 6)

#### Final Configuration
```gradle
tasks.withType(JavaCompile) {
    options.compilerArgs << '-Xlint:unchecked' << "-Werror" << '-Xlint:deprecation' << '-Xlint:removal'
    options.deprecation = true
}
```

#### CI/CD Warning Gates
**Implementation Timeline**: After all deprecation work is complete (no allowlist/baseline system during transition)

**Final CI Configuration**:
- Fail builds on any new deprecation warnings
- Fail builds on any removal warnings
- Add checkstyle rules to prevent deprecated API introduction

## Testing Strategy

### Comprehensive Testing Approach
**Multi-layered Testing**:
1. **Existing test suite**: All unit + integration tests must pass
2. **Enhanced performance benchmarks**: Catch performance impacts from API changes
3. **Compatibility testing**: Ensure no breaks to existing client applications
4. **Staged rollout testing**: Development → staging → production-like environments

**API Category-Specific Testing**:
- Security APIs: Additional security validation tests
- Collections: Performance and behavior consistency tests
- I/O: Compatibility and performance tests
- Concurrency: Thread safety and performance tests

### Performance Monitoring
**Measurement Points**: Only at major milestones
- End of each phase
- Module completion milestones
- Final completion

**Metrics Tracked**:
- Build times
- Test execution times  
- Runtime performance benchmarks
- Memory usage patterns

## Rollback Strategy

### Rollback Triggers
**All of the following scenarios**:
- Performance regressions exceeding 5% in benchmarks
- Test failures that can't be resolved within 48 hours
- Community objections during RFC review process
- Production issues discovered after deployment

### Rollback Mechanism
**Git Revert**: Clean revert of entire module's changes
- Simple and reliable
- Maintains clean git history
- Allows for quick restoration of working state

## Communication and Coordination

### Responsibility
**Single Point of Responsibility**: All modernization work will be performed by the assignee to ensure consistency and coordination.

### Community Communication
**Bi-weekly Updates**: 
- Send progress reports to dev and user mailing lists
- Include: completed modules, current work, upcoming milestones, any blockers

### Documentation
**Progress Tracking**:
- Maintain wiki pages with module status (in progress, completed, upcoming)
- Document established modern API patterns for future reference
- Track all community RFC approvals and decisions

## Timeline and Milestones

### Major Milestones
- **Week 1**: Complete baseline analysis and module prioritization
- **Week 2**: Complete removal warning remediation globally  
- **Week 4**: Complete first 50% of modules (by warning count)
- **Week 5**: Complete all module modernization
- **Week 6**: Full warning compliance and CI/CD gates enabled

### Success Metrics
- Zero suppressed deprecation warnings
- Zero suppressed removal warnings  
- All tests passing
- Performance maintained or improved
- Community RFC approval for all API changes
- Documentation updated with modern patterns

## Dependencies and Prerequisites
- Completion of GEODE-10465 (Java 17 migration)
- Community engagement for RFC review process
- Coordination with any planned Gradle or build system upgrades

## Risk Mitigation
- Phased approach minimizes disruption
- Module-by-module processing allows parallel development to continue
- Comprehensive testing at each milestone
- Clear rollback procedures for any issues
- Community involvement ensures architectural decisions are sound
