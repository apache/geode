# GEODE-10479: Java 17 Deprecation Warning Removal - Implementation Plan

## Project Overview

This project implements the complete removal of deprecation and removal warning suppressions from the Apache Geode codebase following the Java 17 migration (GEODE-10465). The goal is to modernize the codebase to leverage Java 17+ APIs while maintaining zero warnings and ensuring no functional regressions.

**CRITICAL UPDATE**: Analysis revealed Java 21 blocking issues that require immediate attention before any Java 21 migration can proceed.

## Success Criteria

- **Zero tolerance**: All modules must achieve zero warnings (both deprecation and removal)
- **No functional regressions**: All existing tests must pass
- **Performance maintained**: No degradation in build/test/runtime performance
- **Community approval**: All API changes approved through RFC process
- **Java 21 compatibility**: All removal warnings that block Java 21 must be resolved

## Warning Baseline Summary

**Total Warnings Identified**: 41 warnings across 6 modules
- **Critical (Java 21 blockers)**: 2 warnings in geode-logging
- **High Priority (Library blockers)**: 3 warnings in geode-gfsh
- **Medium Priority (Technical debt)**: 36 warnings across 5 modules

**Module Breakdown**:
- geode-logging: 2 warnings (CRITICAL - SecurityManager APIs removed in Java 21)
- geode-gfsh: 26 warnings (3 removal + 18 deprecation + 5 unchecked)
- geode-management: 10 warnings (Apache HttpClient 5 + Commons Lang)
- geode-serialization: 1 warning (Reflection API)
- geode-deployment-legacy: 1 warning (Proxy API)
- geode-web-api: 1 warning (Spring Framework PathPatternParser)

## Implementation Strategy

### Phase 0: Critical Java 21 Blockers (Days 1-2)

**URGENT**: These issues must be resolved before any Java 21 migration attempts.

#### 0.1 Fix SecurityManager API Removal (GEODE-10531)
**Objective**: Remove SecurityManager APIs that were removed in Java 21

**Critical Issue**:
- File: `geode-logging/src/main/java/org/apache/geode/logging/internal/OSProcess.java:200`
- APIs: `java.lang.SecurityManager`, `System.getSecurityManager()`
- Status: **REMOVED in Java 21** (not just deprecated)
- Impact: **Blocks Java 21 compilation**

**Implementation**:
- Remove all SecurityManager references from OSProcess.java
- Replace security checks with modern alternatives (context-specific permissions, Java Security API, or application-level security)
- Ensure functionality is preserved after removal

**Deliverables**:
- Updated OSProcess.java with SecurityManager APIs removed
- Comprehensive testing to ensure security functionality preserved
- Documentation of replacement security approach

**Acceptance Criteria**:
- Code compiles successfully in Java 21
- All existing tests pass
- Security functionality maintained or improved
- Zero removal warnings in geode-logging module

### Phase 1: Assessment and Categorization (Week 1)

#### 1.1 Create Custom Gradle Warning Analysis Task
**Objective**: Build tooling to systematically analyze and categorize warnings

**Implementation**:
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

**Deliverables**:
- Custom Gradle task for warning analysis
- JSON/CSV output format for structured data
- Automated categorization logic
- Dashboard-style reporting

#### 1.2 Integrate with SonarQube and CI Reporting
**Objective**: Establish ongoing tracking and trend analysis

**Implementation**:
- SonarQube integration for trend analysis
- Gradle's built-in reporting for CI integration
- Automated categorization based on warning patterns

#### 1.3 Generate Baseline Warning Report
**Objective**: Create comprehensive baseline of all warnings

**Warning Categories**:
- **Critical**: API removal warnings (will break in future Java versions)
- **High Priority**: Security-related deprecated APIs
- **Medium Priority**: Performance-impacting deprecated APIs
- **Low Priority**: General deprecated APIs with modern alternatives

#### 1.4 Implement Module Prioritization Strategy
**Objective**: Establish processing order prioritizing blockers first, then "fewest warnings first"

**Updated Prioritization Strategy**:
1. **Critical blockers first**: Java 21 blocking issues (completed in Phase 0)
2. **High-priority removal warnings**: Spring Framework and other library blockers
3. **Fewest warnings first**: Remaining modules by ascending warning count
4. **Complex modules last**: geode-gfsh (26 warnings) processed after building momentum

**Processing Order**:
1. ✅ geode-logging (2 warnings - CRITICAL, completed in Phase 0)
2. geode-serialization (1 warning - Reflection API)
3. geode-deployment-legacy (1 warning - Proxy API)
4. geode-web-api (1 warning - Spring Framework)
5. geode-management (10 warnings - HttpClient + Commons Lang)
6. geode-gfsh (26 warnings - Multiple libraries and patterns)

### Phase 2: High-Priority Removal Warnings (Week 2)

#### 2.0 Fix Spring Framework Removal Warnings (GEODE-10532)
**Objective**: Replace deprecated Spring APIs that will be removed in Spring Framework 7.0

**Critical Issue**:
- File: `geode-gfsh/src/main/java/org/apache/geode/management/internal/web/http/support/HttpRequester.java`
- Lines: 113, 115, 117
- API: `ClientHttpResponse.getRawStatusCode()` → `getStatusCode().value()`
- Timeline: Removal in Spring Framework 7.0 (estimated 2026)

**Implementation**:
```java
// Before:
int statusCode = response.getRawStatusCode();

// After:
int statusCode = response.getStatusCode().value();
```

**Deliverables**:
- Updated HttpRequester.java with modern Spring APIs
- GFSH HTTP status code testing
- Validation of error handling paths

### Phase 3: Incremental Warning Re-enablement (Weeks 2-4)

#### 3.1 Enable Global Removal Warnings
**Objective**: Re-enable removal warnings globally first (highest priority)

**Implementation**:
```gradle
// In warnings.gradle
tasks.withType(JavaCompile) {
    options.compilerArgs << '-Xlint:removal'  // Remove the minus sign
    // Keep other suppressions temporarily
}
```

#### 3.2 Fix Remaining API Removal Issues
**Objective**: Address API removal warnings that will break in future Java versions

**Focus Areas**:
- Replace removed APIs with modern alternatives
- Focus on critical functionality first
- Ensure no breaking changes to public APIs

#### 3.3 Implement Graduated Deprecation Enablement
**Objective**: Enable deprecation warnings module-by-module

**Implementation**:
```gradle
// Module-by-module enablement
if (project.name in processedModules) {
    options.deprecation = true
    options.compilerArgs << '-Xlint:deprecation'
}
```

**Interface Contracts**:
- Public APIs between modules must be deprecation-free
- Internal implementation can temporarily use `@SuppressWarnings("deprecation")` with TODO comments
- All suppressions must link to tracking tickets

#### 3.4 Process Modules in Priority Order
**Objective**: Systematically process each module to zero warnings

**Module Completion Criteria**:
- Zero warnings of any type
- All tests passing
- Performance maintained
- Documentation updated

**Specific Module Processing**:

**3.4.1 geode-serialization (1 warning)**
- Issue: `AccessibleObject.isAccessible()` → `canAccess(Object obj)`
- Effort: 2-3 hours
- Risk: Medium (requires target object reference)

**3.4.2 geode-deployment-legacy (1 warning)**
- Issue: `Proxy.getProxyClass()` → `Proxy.newProxyInstance()`
- Effort: 2-3 hours
- Risk: Medium (dynamic proxy generation)

**3.4.3 geode-web-api (1 warning)**
- Issue: Spring `PathPatternParser.setMatchOptionalTrailingSeparator()`
- Effort: 2-3 hours
- Risk: Low (configuration change)

**3.4.4 geode-management (10 warnings)**
- Issues: Apache HttpClient 5 SSL APIs (9) + Commons Lang StringUtils (1)
- Effort: 6-8 hours
- Risk: Medium (SSL connection testing required)

**3.4.5 geode-gfsh (23 remaining warnings after GEODE-10532)**
- Issues:
  - Apache Commons Lang StringUtils (7 warnings): startsWith(), containsIgnoreCase(), equals(), removeStart()
  - Java Reflection API (4 warnings): Class.newInstance() in RegionFunctionArgs.java, CreateAsyncEventQueueFunction.java, UserFunctionExecution.java
  - Unchecked Stream operations (5 warnings): ShowMissingDiskStoreCommand.java:87-90
  - Micrometer StringUtils (3 warnings): StopServerCommand.java:17 - internal utility class
  - Geode Query API IndexType (1 warning): GfshParser.java:708
  - Proxy.getProxyClass() (3 warnings): deprecated Java 9 API
- Effort: 1-2 days
- Risk: Medium (multiple API patterns, requires comprehensive testing)

### Phase 4: Third-Party Library API Modernization (Weeks 3-5)

#### 4.1 Apache HttpClient 5 SSL API Migration
**Objective**: Modernize SSL configuration in geode-management

**Deprecated APIs**:
- `SSLConnectionSocketFactory` → `SSLConnectionSocketFactoryBuilder`
- `setSSLSocketFactory()` → `setTlsSocketStrategy()`

**Implementation**:
- Update SSL socket factory creation patterns
- Migrate to modern TLS configuration approach
- Comprehensive SSL/TLS connection testing

#### 4.2 Apache Commons Lang Migration
**Objective**: Replace deprecated StringUtils methods across modules

**Common Patterns**:
```java
// StringUtils.startsWith() → String.startsWith()
StringUtils.startsWith(str, prefix) → str.startsWith(prefix)

// StringUtils.containsIgnoreCase() → toLowerCase().contains()
StringUtils.containsIgnoreCase(str, search) → str.toLowerCase().contains(search.toLowerCase())

// StringUtils.equals() → Objects.equals()
StringUtils.equals(cs1, cs2) → Objects.equals(cs1, cs2)

// StringUtils.removeStart() → substring logic
StringUtils.removeStart(str, prefix) → str.startsWith(prefix) ? str.substring(prefix.length()) : str
```

#### 4.3 Java Reflection API Modernization
**Objective**: Update deprecated reflection patterns

**Migration Patterns**:
```java
// Class.newInstance() → getDeclaredConstructor().newInstance()
Object instance = clazz.newInstance();
→
Object instance = clazz.getDeclaredConstructor().newInstance();

// AccessibleObject.isAccessible() → canAccess(Object)
if (field.isAccessible()) { ... }
→
if (field.canAccess(targetObject)) { ... }

// Proxy.getProxyClass() → Proxy.newProxyInstance()
Class<?> proxyClass = Proxy.getProxyClass(loader, interfaces);
Object proxy = proxyClass.getConstructor(InvocationHandler.class).newInstance(handler);
→
Object proxy = Proxy.newProxyInstance(loader, interfaces, handler);
```

#### 4.4 Spring Framework PathPatternParser Migration (geode-web-api)
**Objective**: Update deprecated Spring Framework 6.x path pattern configuration

**Deprecated API**:
- `PathPatternParser.setMatchOptionalTrailingSeparator(boolean)` (deprecated in Spring 6.x)

**Implementation**:
- Consult Spring Framework 6.x migration guide for replacement configuration
- Update Swagger/OpenAPI path pattern matching configuration
- Test trailing separator handling behavior
- Verify Swagger/OpenAPI URL path matching

**Files Affected**:
- `geode-web-api/src/main/java/org/apache/geode/rest/internal/web/swagger/config/SwaggerConfig.java:62`

#### 4.5 Micrometer StringUtils Migration (geode-gfsh)
**Objective**: Replace internal Micrometer utility class usage

**Issue**: `io.micrometer.core.instrument.util.StringUtils` is an internal utility class not meant for public use

**Implementation**:
- Replace with `org.springframework.util.StringUtils` or Java standard library methods
- Update StopServerCommand.java:17
- Test string operations

#### 4.6 Geode Query API Migration (geode-gfsh)
**Objective**: Update deprecated Geode Query API usage

**Deprecated API**:
- `org.apache.geode.cache.query.IndexType` (deprecated)

**Implementation**:
- Consult Geode API documentation for replacement
- Update GfshParser.java:708
- Test query functionality and index creation

#### 4.7 Establish RFC Process for API Changes
**Objective**: Set up community proposal process for API changes

**RFC Structure**:
- Location: `@proposals/GEODE-XXXXX-module-name-deprecation-removal/`
- Format: Standard RFC format
- Scope: One RFC per module (following "fewest warnings first" order)
- Content organized by functional areas within each module

**Required RFC Content**:
- Before/after code examples for each API change
- Performance impact analysis
- Migration guide for downstream users
- Rollback procedures if issues discovered
- Justification for chosen replacement APIs

### Phase 5: Full Warning Compliance (Week 6)

#### 5.1 Remove All Warning Suppressions
**Objective**: Final configuration with all warnings enabled

**Final Configuration**:
```gradle
tasks.withType(JavaCompile) {
    options.compilerArgs << '-Xlint:unchecked' << "-Werror" << '-Xlint:deprecation' << '-Xlint:removal'
    options.deprecation = true
}
```

#### 5.2 Establish CI/CD Warning Gates
**Objective**: Prevent regression of deprecated API usage

**Implementation**:
- Fail builds on any new deprecation warnings
- Fail builds on any removal warnings
- Add checkstyle rules to prevent deprecated API introduction

#### 5.3 Comprehensive Testing and Validation
**Objective**: Ensure no functional or performance regressions

**Testing Strategy**:
- All unit + integration tests must pass
- Enhanced performance benchmarks
- Compatibility testing
- Staged rollout testing

#### 5.4 Documentation and Communication
**Objective**: Document modern patterns and communicate progress

**Deliverables**:
- Updated documentation with modern API usage patterns
- Migration guides for downstream users
- Bi-weekly progress reports to community
- Wiki pages with module status tracking

## Risk Mitigation

- **Phased approach** minimizes disruption
- **Module-by-module processing** allows parallel development to continue
- **Comprehensive testing** at each milestone
- **Clear rollback procedures** for any issues
- **Community involvement** ensures architectural decisions are sound

## Testing Strategy

### Comprehensive Testing Approach
**Multi-layered Testing**:
1. **Existing test suite**: All unit + integration tests must pass
2. **Enhanced performance benchmarks**: Catch performance impacts from API changes
3. **Compatibility testing**: Ensure no breaks to existing client applications
4. **Staged rollout testing**: Development → staging → production-like environments

**Library-Specific Testing Requirements**:
- **SSL/TLS Testing**: Apache HttpClient 5 SSL configuration validation
- **Reflection Testing**: Class instantiation, field access, proxy creation
- **String Operations**: Null handling, edge cases for Commons Lang replacements
- **Stream Operations**: Type safety validation for unchecked generics
- **HTTP Status Codes**: Spring Framework status code handling (success, error, edge cases)
- **Security Testing**: SecurityManager replacement functionality validation

## Timeline and Milestones

- **Days 1-2**: Complete critical Java 21 blockers (Phase 0)
- **Week 1**: Complete baseline analysis and module prioritization
- **Week 2**: Complete high-priority removal warnings (Spring Framework)
- **Weeks 2-4**: Complete incremental warning re-enablement
- **Weeks 3-5**: Complete third-party library API modernization
- **Week 6**: Full warning compliance and CI/CD gates enabled

## Dependencies and Prerequisites

- Completion of GEODE-10465 (Java 17 migration)
- Community engagement for RFC review process
- Coordination with any planned Gradle or build system upgrades

---

# Implementation Prompts for Code Generation LLM

## Phase 1 Implementation Prompts

### Prompt 1.1: Create Custom Gradle Warning Analysis Task

```
Create a custom Gradle task called `generateWarningReport` that performs comprehensive warning analysis for the Apache Geode project. The task should:

1. **Temporarily disable warning suppressions**: Modify compiler arguments to remove `-Xlint:-deprecation`, `-Xlint:-removal`, and set `options.deprecation = true`

2. **Capture warnings systematically**: Execute compilation and capture all compiler warnings to structured data format

3. **Categorize warnings**: Implement categorization logic for:
   - Critical: API removal warnings (will break in future Java versions)
   - High Priority: Security-related deprecated APIs
   - Medium Priority: Performance-impacting deprecated APIs
   - Low Priority: General deprecated APIs with modern alternatives

4. **Generate structured output**: Create JSON/CSV reports with fields:
   - Module name
   - Warning type (deprecation/removal)
   - Warning category (Critical/High/Medium/Low)
   - Source file and line number
   - Warning message
   - Suggested replacement (where applicable)

5. **Create dashboard reports**: Generate HTML dashboard showing:
   - Warning count by module
   - Warning distribution by category
   - Trend analysis capabilities

Requirements:
- Use test-driven development approach
- Write comprehensive unit tests for categorization logic
- Ensure task can run without breaking existing build
- Include error handling for compilation failures
- Make output format extensible for future enhancements

Test the implementation by running against a subset of modules first, then validate against the full codebase.
```

### Prompt 1.2: Integrate with SonarQube and CI Reporting

```
Extend the warning analysis system created in the previous step to integrate with SonarQube and CI reporting systems. Building on the existing `generateWarningReport` task:

1. **SonarQube Integration**:
   - Create SonarQube custom rules for deprecation/removal warnings
   - Map warning categories to SonarQube severity levels
   - Generate SonarQube-compatible XML reports
   - Set up trend tracking for warning counts over time

2. **CI Integration**:
   - Integrate with Gradle's built-in reporting mechanisms
   - Create JUnit-style XML reports for CI consumption
   - Add build status indicators based on warning thresholds
   - Generate artifacts for CI dashboard display

3. **Automated Categorization Enhancement**:
   - Implement pattern-based categorization using regex patterns
   - Create configuration file for categorization rules
   - Add machine learning-style classification for unknown warnings
   - Include confidence scores for automated categorizations

4. **Reporting Enhancements**:
   - Add email reporting capabilities for stakeholders
   - Create diff reports showing warning changes between builds
   - Implement warning suppression tracking
   - Add performance metrics for analysis execution

Requirements:
- Maintain backward compatibility with existing reporting
- Write integration tests with mock SonarQube server
- Ensure CI integration works with common CI systems (Jenkins, GitHub Actions)
- Include comprehensive error handling and logging
- Document configuration options and setup procedures

Test the integration with a sample CI pipeline and validate SonarQube rule functionality.
```

### Prompt 1.3: Generate Baseline Warning Report

```
Using the warning analysis infrastructure built in previous steps, generate a comprehensive baseline warning report for the entire Apache Geode codebase:

1. **Execute Full Codebase Analysis**:
   - Run `generateWarningReport` task against all Geode modules
   - Handle compilation failures gracefully with partial reporting
   - Capture both deprecation and removal warnings
   - Include third-party dependency warnings analysis

2. **Create Comprehensive Baseline Report**:
   - Generate module-by-module warning breakdown
   - Create summary statistics (total warnings, by category, by type)
   - Identify top 10 most problematic modules
   - Document external dependency vs internal code warning split

3. **Warning Impact Assessment**:
   - Analyze critical path modules (geode-core, geode-common, etc.)
   - Identify warnings that affect public APIs
   - Document warnings that impact security or performance
   - Create dependency graph showing warning propagation between modules

4. **Baseline Documentation**:
   - Create detailed CSV/JSON exports for tracking
   - Generate executive summary report
   - Document methodology and assumptions
   - Include recommendations for prioritization

5. **Validation and Quality Assurance**:
   - Cross-validate warning counts with manual spot checks
   - Verify categorization accuracy on sample warnings
   - Ensure report completeness across all modules
   - Document any analysis limitations or exclusions

Requirements:
- Generate reproducible results with consistent methodology
- Include timestamp and environment information in reports
- Create both technical and executive-level summaries
- Ensure reports are version-controllable and diffable
- Include confidence metrics for automated categorizations

Expected deliverables:
- Baseline warning report (JSON/CSV/HTML formats)
- Executive summary with key findings
- Module prioritization recommendations
- Validation report confirming analysis accuracy

Test by comparing results with manual analysis of a subset of modules.
```

### Prompt 1.4: Implement Module Prioritization Strategy

```
Implement the "fewest warnings first" module prioritization strategy using the baseline warning data from the previous step:

1. **Module Warning Analysis**:
   - Parse baseline warning report data
   - Calculate warning counts per module (total, by category, by type)
   - Identify module dependencies and warning propagation
   - Account for module complexity and criticality factors

2. **Prioritization Algorithm**:
   - Sort modules by ascending total warning count
   - Apply weighting factors for:
     - Module criticality (core vs peripheral)
     - Warning severity (removal vs deprecation)
     - Dependency impact (how many other modules depend on this)
     - Development team familiarity
   - Create processing order that maximizes early wins

3. **Dependency-Aware Ordering**:
   - Ensure dependency modules are processed before dependents
   - Identify circular dependencies that need special handling
   - Create parallel processing opportunities where possible
   - Account for interface contract requirements between modules

4. **Processing Strategy Documentation**:
   - Generate ordered module list with rationale
   - Create processing timeline estimates
   - Document special cases and exceptions
   - Include rollback procedures for each module

5. **Validation and Optimization**:
   - Simulate processing order to identify bottlenecks
   - Validate dependency ordering correctness
   - Create alternative orderings for different scenarios
   - Include progress tracking mechanisms

Requirements:
- Create configurable prioritization algorithm
- Generate multiple prioritization strategies for comparison
- Include detailed rationale for ordering decisions
- Ensure processing order respects module dependencies
- Create tooling for progress tracking during implementation

Expected deliverables:
- Ordered module processing list
- Prioritization algorithm implementation
- Processing timeline and resource estimates
- Progress tracking dashboard
- Alternative prioritization strategies

Test the prioritization by running a simulation of the first few modules in the processing order.
```

## Phase 2 Implementation Prompts

### Prompt 2.1: Enable Global Removal Warnings

```
Implement the first step of incremental warning re-enablement by globally enabling removal warnings across the Apache Geode codebase:

1. **Modify Build Configuration**:
   - Update `build-tools/scripts/src/main/groovy/warnings.gradle` to remove `-Xlint:-removal` suppression
   - Update `build-tools/scripts/src/main/groovy/geode-java.gradle` to remove `-Xlint:-removal` from compiler args
   - Ensure `-Xlint:removal` is explicitly enabled
   - Keep other warning suppressions temporarily in place

2. **Create Removal Warning Baseline**:
   - Execute build with removal warnings enabled
   - Capture all removal warnings across all modules
   - Categorize removal warnings by severity and impact
   - Document APIs that will break in future Java versions

3. **Implement Temporary Suppression Strategy**:
   - Add `@SuppressWarnings("removal")` annotations where immediate fixes aren't feasible
   - Link each suppression to a tracking ticket
   - Document rationale for each temporary suppression
   - Create TODO comments with target resolution dates

4. **Build Stability Validation**:
   - Ensure build completes successfully with removal warnings enabled
   - Validate that existing tests continue to pass
   - Confirm no functional regressions introduced
   - Test build performance impact

5. **Documentation and Communication**:
   - Document the change in build configuration
   - Create migration guide for developers
   - Update CI/CD documentation
   - Communicate change to development team

Requirements:
- Maintain build stability throughout the change
- Use test-driven approach for build configuration changes
- Include rollback procedures if issues arise
- Ensure change is backward compatible with existing development workflows
- Create comprehensive testing of the build configuration change

Expected deliverables:
- Updated build configuration files
- Removal warning baseline report
- Temporary suppression documentation
- Build validation test results
- Developer migration guide

Test by running full build and test suite to ensure no regressions.
```

### Prompt 2.2: Fix Critical API Removal Issues

```
Address the critical API removal warnings identified in the previous step, focusing on APIs that will break in future Java versions:

1. **Critical Removal Warning Analysis**:
   - Review removal warning baseline from previous step
   - Identify APIs marked for removal in Java 18+
   - Prioritize by impact on core functionality
   - Research modern replacement APIs for each deprecated API

2. **API Replacement Implementation**:
   - Replace removed Security Manager APIs with modern alternatives
   - Update deprecated reflection APIs for module system compatibility
   - Replace removed networking APIs with NIO.2 equivalents
   - Update deprecated concurrency APIs with modern alternatives

3. **Backward Compatibility Strategy**:
   - Ensure public API changes maintain backward compatibility
   - Create adapter patterns where direct replacement isn't possible
   - Document any breaking changes with migration paths
   - Implement feature flags for gradual rollout if needed

4. **Testing and Validation**:
   - Create comprehensive test suite for each API replacement
   - Validate functional equivalence with original APIs
   - Test performance characteristics of new implementations
   - Ensure security properties are maintained or improved

5. **Documentation and Migration**:
   - Document each API replacement with before/after examples
   - Create migration guide for downstream users
   - Update internal documentation and code comments
   - Provide rollback procedures for each change

Requirements:
- Use test-driven development for all API replacements
- Maintain or improve performance characteristics
- Ensure security properties are preserved
- Create comprehensive regression tests
- Document all changes thoroughly

Expected deliverables:
- Updated code with modern API usage
- Comprehensive test suite for changes
- Performance benchmark results
- Security validation report
- Migration documentation

Test each API replacement individually before integrating, and run full test suite to validate no regressions.
```

### Prompt 2.3: Implement Graduated Deprecation Enablement

```
Create a module-by-module deprecation warning enablement system with interface contracts:

1. **Build System Enhancement**:
   - Modify build configuration to support per-module deprecation warning control
   - Create `processedModules` list in build configuration
   - Implement conditional logic: `if (project.name in processedModules) { options.deprecation = true }`
   - Ensure system works with Gradle's incremental compilation

2. **Interface Contract Framework**:
   - Define interface contract requirements between modules
   - Implement validation that public APIs between modules are deprecation-free
   - Create automated checking for interface contract violations
   - Document contract requirements and enforcement mechanisms

3. **Temporary Suppression Management**:
   - Create system for managing `@SuppressWarnings("deprecation")` annotations
   - Implement TODO comment linking to tracking tickets
   - Create reporting for all temporary suppressions
   - Establish review process for suppression approvals

4. **Module Enablement Workflow**:
   - Create step-by-step process for enabling deprecation warnings in a module
   - Implement validation checks before adding module to `processedModules`
   - Create rollback procedures if issues are discovered
   - Document testing requirements for each module

5. **Progress Tracking and Reporting**:
   - Create dashboard showing module processing status
   - Implement progress metrics and timeline tracking
   - Generate reports on interface contract compliance
   - Create alerts for contract violations

Requirements:
- Ensure system doesn't break existing development workflows
- Create comprehensive testing for the enablement system
- Implement proper error handling and rollback capabilities
- Document the process thoroughly for team adoption
- Ensure system scales to all Geode modules

Expected deliverables:
- Enhanced build system with module-by-module control
- Interface contract validation framework
- Temporary suppression management system
- Module enablement workflow documentation
- Progress tracking dashboard

Test the system by enabling deprecation warnings on the first few modules in the priority order.
```

### Prompt 2.4: Process Modules in Priority Order

```
Systematically process each module to achieve zero warnings following the priority order established in Phase 1:

1. **Module Processing Framework**:
   - Implement automated module processing workflow
   - Create checklist for each module completion
   - Establish entry and exit criteria for module processing
   - Create parallel processing capabilities where dependencies allow

2. **Per-Module Warning Resolution**:
   - Enable deprecation warnings for target module
   - Analyze and categorize all warnings in the module
   - Implement fixes for each warning category:
     - Replace deprecated APIs with modern alternatives
     - Update deprecated patterns with current best practices
     - Resolve interface contract violations
   - Validate zero warnings achieved

3. **Testing and Validation Per Module**:
   - Run full test suite for processed module
   - Execute performance benchmarks to ensure no regressions
   - Validate interface contracts with dependent modules
   - Test integration with other processed modules

4. **Documentation and Knowledge Transfer**:
   - Document modern API patterns established in each module
   - Create migration examples for common deprecation patterns
   - Update module-specific documentation
   - Share learnings with development team

5. **Progress Management**:
   - Update progress tracking dashboard after each module
   - Generate status reports for stakeholders
   - Identify and escalate blockers quickly
   - Maintain timeline and resource allocation

Requirements:
- Achieve zero warnings for each processed module
- Maintain all existing functionality
- Ensure no performance regressions
- Create reusable patterns for similar warnings across modules
- Document all changes comprehensively

Expected deliverables per module:
- Zero-warning module with all tests passing
- Updated documentation with modern patterns
- Performance validation results
- Interface contract compliance confirmation
- Progress report and lessons learned

Process modules in the established priority order, completing each module fully before moving to the next.
```

## Phase 3 Implementation Prompts

### Prompt 3.1: Establish RFC Process for API Changes

```
Set up a comprehensive RFC (Request for Comments) process for managing API changes during the deprecation removal project:

1. **RFC Framework Setup**:
   - Create RFC template following Apache Geode community standards
   - Establish RFC directory structure: `@proposals/GEODE-XXXXX-module-name-deprecation-removal/`
   - Define RFC lifecycle: Draft → Community Review → Approval → Implementation
   - Create review criteria and approval thresholds

2. **RFC Template and Guidelines**:
   - Standard sections: Summary, Motivation, Detailed Design, Alternatives, Risks
   - Required content for deprecation removal RFCs:
     - Before/after code examples for each API change
     - Performance impact analysis with benchmarks
     - Migration guide for downstream users
     - Rollback procedures if issues discovered
     - Justification for chosen replacement APIs

3. **Community Engagement Process**:
   - Define stakeholder groups and notification procedures
   - Establish review timeline (minimum review period, escalation procedures)
   - Create feedback collection and resolution mechanisms
   - Document decision-making process and authority

4. **RFC Management Tooling**:
   - Create RFC status tracking system
   - Implement automated notifications for RFC lifecycle events
   - Generate RFC summary reports for project tracking
   - Create templates for common deprecation patterns

5. **Integration with Development Process**:
   - Link RFCs to implementation tasks and pull requests
   - Establish gates preventing implementation without approved RFC
   - Create validation that implementation matches approved RFC
   - Document exceptions and emergency procedures

Requirements:
- Follow Apache Geode community governance standards
- Ensure process is lightweight but thorough
- Create clear guidelines for RFC authors
- Establish efficient review and approval workflows
- Include mechanisms for handling disagreements

Expected deliverables:
- RFC process documentation
- RFC templates and examples
- Community engagement guidelines
- RFC management tooling
- Integration with development workflow

Test the process by creating a sample RFC for a simple API change and walking it through the full lifecycle.
```

### Prompt 3.2: Security API Modernization

```
Modernize security-related APIs across the Apache Geode codebase, focusing on deprecated security manager APIs, SSL/TLS configuration, and authentication mechanisms:

1. **Security API Assessment**:
   - Identify all deprecated security-related APIs in use
   - Analyze Security Manager API usage and replacement strategies
   - Review SSL/TLS configuration APIs for modern alternatives
   - Assess authentication mechanism implementations

2. **Security Manager API Replacement**:
   - Replace deprecated SecurityManager APIs with modern alternatives
   - Implement policy-based security where applicable
   - Update permission checking mechanisms
   - Ensure backward compatibility for existing security configurations

3. **SSL/TLS Configuration Modernization**:
   - Update deprecated SSL context creation APIs
   - Modernize certificate handling and validation
   - Implement modern cipher suite selection
   - Update protocol version handling for security best practices

4. **Authentication Mechanism Updates**:
   - Modernize JAAS (Java Authentication and Authorization Service) usage
   - Update credential handling APIs
   - Implement modern token-based authentication where appropriate
   - Ensure secure credential storage and transmission

5. **Security Validation and Testing**:
   - Create comprehensive security test suite for all changes
   - Validate that security properties are maintained or improved
   - Test against common security vulnerabilities
   - Perform security review of all changes

Requirements:
- Maintain or improve security posture
- Ensure backward compatibility for security configurations
- Follow security best practices for all implementations
- Create comprehensive security testing
- Document security implications of all changes

Expected deliverables:
- Updated security API implementations
- Comprehensive security test suite
- Security impact analysis
- Migration guide for security configurations
- Security review documentation

Create RFC for security changes and obtain community approval before implementation.
```

### Prompt 3.3: Collections and Concurrency Modernization

```
Modernize collections and concurrency APIs throughout the Apache Geode codebase to leverage Java 17 improvements:

1. **Collections API Assessment**:
   - Identify deprecated collection methods and patterns
   - Analyze usage of legacy collection APIs (Vector, Hashtable, etc.)
   - Review custom collection implementations for modernization opportunities
   - Assess thread-safety requirements for collection usage

2. **Collections Modernization**:
   - Replace deprecated collection methods with modern alternatives
   - Update legacy collections (Vector → ArrayList, Hashtable → ConcurrentHashMap)
   - Leverage Java 17 collection factory methods and improvements
   - Implement immutable collections where appropriate
   - Update collection iteration patterns to use modern approaches

3. **Concurrency API Updates**:
   - Replace deprecated concurrency utilities with modern alternatives
   - Update thread pool implementations to use modern ExecutorService patterns
   - Leverage Java 17 concurrency improvements (CompletableFuture enhancements, etc.)
   - Modernize synchronization patterns and lock usage
   - Update atomic operations to use modern APIs

4. **Performance Optimization**:
   - Benchmark collection and concurrency changes for performance impact
   - Identify opportunities for performance improvements with modern APIs
   - Optimize memory usage patterns with modern collection implementations
   - Validate thread safety and performance under load

5. **Testing and Validation**:
   - Create comprehensive test suite for collection and concurrency changes
   - Implement performance regression tests
   - Test thread safety and concurrent access patterns
   - Validate behavior consistency with original implementations

Requirements:
- Maintain or improve performance characteristics
- Ensure thread safety is preserved or improved
- Create comprehensive testing for all changes
- Document performance implications
- Ensure backward compatibility for public APIs

Expected deliverables:
- Modernized collections and concurrency implementations
- Performance benchmark results
- Thread safety validation
- Comprehensive test suite
- Performance impact documentation

Create RFC documenting collections and concurrency changes with performance analysis.
```

### Prompt 3.4: I/O and Networking Modernization

```
Modernize I/O and networking APIs across the Apache Geode codebase to leverage NIO.2 and modern networking improvements:

1. **I/O API Assessment**:
   - Identify deprecated I/O APIs and patterns in use
   - Analyze file I/O operations for NIO.2 migration opportunities
   - Review stream and reader/writer usage for modernization
   - Assess character encoding handling and updates needed

2. **File I/O Modernization**:
   - Replace deprecated File APIs with Path and Files APIs
   - Update file operations to use NIO.2 for better performance
   - Implement modern file watching and monitoring where applicable
   - Update file permission and attribute handling
   - Modernize temporary file and directory creation

3. **Networking API Updates**:
   - Replace deprecated networking APIs with modern alternatives
   - Update socket implementations to use NIO channels where appropriate
   - Modernize URL and URI handling
   - Update HTTP client usage to modern implementations
   - Implement modern SSL/TLS handling for network connections

4. **Stream and Buffer Management**:
   - Update stream handling to use modern patterns
   - Implement try-with-resources for proper resource management
   - Modernize buffer management and memory-mapped file usage
   - Update serialization and deserialization patterns

5. **Compatibility and Performance Testing**:
   - Test I/O operations for performance improvements
   - Validate compatibility with existing file formats and protocols
   - Test network operations under various conditions
   - Ensure proper resource cleanup and memory management

Requirements:
- Maintain compatibility with existing file formats and network protocols
- Improve performance where possible with modern APIs
- Ensure proper resource management and cleanup
- Create comprehensive testing for all I/O and networking changes
- Document migration paths for any breaking changes

Expected deliverables:
- Modernized I/O and networking implementations
- Performance comparison results
- Compatibility validation
- Resource management improvements
- Migration documentation

Create RFC for I/O and networking changes with compatibility and performance analysis.
```

### Prompt 3.5: Reflection and Introspection Modernization

```
Modernize reflection and introspection APIs for Java 17 module system compatibility and modern best practices:

1. **Reflection API Assessment**:
   - Identify deprecated reflection APIs and patterns
   - Analyze module system compatibility requirements
   - Review introspection usage for modernization opportunities
   - Assess security implications of reflection usage

2. **Module System Compatibility**:
   - Update reflection usage for module system compatibility
   - Add proper module exports where needed for reflection access
   - Implement module-aware reflection patterns
   - Update class loading and discovery mechanisms
   - Handle module boundaries and access restrictions properly

3. **Reflection API Modernization**:
   - Replace deprecated reflection methods with modern alternatives
   - Update method and field access patterns
   - Modernize annotation processing and discovery
   - Implement proper exception handling for reflection operations
   - Update dynamic proxy creation and usage

4. **Introspection Updates**:
   - Replace deprecated introspection methods
   - Update bean introspection patterns
   - Modernize property discovery and access
   - Implement modern metadata handling
   - Update serialization introspection

5. **Security and Performance Considerations**:
   - Ensure reflection usage follows security best practices
   - Optimize reflection performance where possible
   - Implement proper access control for reflection operations
   - Cache reflection metadata where appropriate
   - Document security implications of reflection changes

Requirements:
- Ensure module system compatibility
- Maintain security properties of reflection usage
- Optimize performance where possible
- Create comprehensive testing for reflection operations
- Document module export requirements

Expected deliverables:
- Module system compatible reflection implementations
- Updated introspection patterns
- Security validation for reflection usage
- Performance optimization results
- Module export documentation

Create RFC for reflection and introspection changes with module system compatibility analysis.
```

## Phase 4 Implementation Prompts

### Prompt 4.1: Remove All Warning Suppressions

```
Complete the final step of warning suppression removal by updating all build configuration files to enable all warnings:

1. **Build Configuration Updates**:
   - Update `build-tools/scripts/src/main/groovy/warnings.gradle`:
     - Remove `-Xlint:-unchecked`, `-Xlint:-deprecation`, `-Xlint:-removal`
     - Add `-Xlint:unchecked`, `-Xlint:deprecation`, `-Xlint:removal`
     - Set `options.deprecation = true`
   - Update `build-tools/scripts/src/main/groovy/geode-java.gradle`:
     - Remove `-Xlint:-removal`, `-Xlint:-deprecation` from compiler args
   - Ensure consistent configuration across all build files

2. **Final Warning Validation**:
   - Execute full build with all warnings enabled
   - Validate zero warnings across all modules
   - Confirm no warning suppressions remain in code
   - Test build performance with all warnings enabled

3. **Build System Testing**:
   - Test incremental compilation with warnings enabled
   - Validate IDE integration with new warning configuration
   - Test parallel build execution
   - Confirm build caching works correctly

4. **Rollback Preparation**:
   - Document exact changes made to build configuration
   - Create rollback scripts for quick reversion if needed
   - Test rollback procedures
   - Document emergency procedures

5. **Final Validation**:
   - Run complete test suite with new configuration
   - Execute performance benchmarks
   - Validate all CI/CD pipelines work correctly
   - Confirm no functional regressions

Requirements:
- Achieve zero warnings across entire codebase
- Maintain build performance
- Ensure all tests pass
- Create comprehensive rollback procedures
- Document all configuration changes

Expected deliverables:
- Updated build configuration files
- Zero warning validation report
- Build performance analysis
- Rollback procedures documentation
- Final validation results

Test thoroughly before committing changes to ensure no regressions.
```

### Prompt 4.2: Establish CI/CD Warning Gates

```
Implement comprehensive CI/CD warning gates to prevent regression of deprecated API usage:

1. **CI/CD Pipeline Configuration**:
   - Update CI/CD configuration to fail builds on any deprecation warnings
   - Configure build to fail on any removal warnings
   - Implement warning threshold enforcement (zero tolerance)
   - Add warning detection to all build stages (compile, test, package)

2. **Checkstyle Integration**:
   - Create custom checkstyle rules to prevent deprecated API introduction
   - Configure checkstyle to detect common deprecated patterns
   - Implement rules for specific deprecated APIs identified during the project
   - Add checkstyle validation to CI/CD pipeline

3. **Automated Warning Detection**:
   - Implement automated parsing of compiler output for warnings
   - Create warning classification and reporting
   - Add warning trend tracking over time
   - Implement alerts for warning threshold violations

4. **Developer Workflow Integration**:
   - Configure IDE integration for warning detection
   - Create pre-commit hooks to catch warnings early
   - Implement pull request validation for warnings
   - Add warning status to code review process

5. **Monitoring and Alerting**:
   - Create dashboards for warning status monitoring
   - Implement alerts for warning gate failures
   - Add warning metrics to project health monitoring
   - Create reporting for warning trends and patterns

Requirements:
- Zero tolerance for new deprecation or removal warnings
- Fast feedback to developers on warning violations
- Comprehensive coverage across all build stages
- Integration with existing development workflows
- Reliable detection and reporting of warnings

Expected deliverables:
- Updated CI/CD pipeline configuration
- Custom checkstyle rules for deprecated API prevention
- Automated warning detection system
- Developer workflow integration
- Monitoring and alerting system

Test the warning gates with intentional deprecated API usage to ensure detection works correctly.
```

### Prompt 4.3: Comprehensive Testing and Validation

```
Execute comprehensive testing and validation to ensure the deprecation warning removal project meets all success criteria:

1. **Full Test Suite Execution**:
   - Run complete unit test suite across all modules
   - Execute integration tests for all components
   - Run distributed tests to validate distributed functionality
   - Execute performance tests to ensure no regressions
   - Run compatibility tests with different Java versions

2. **Performance Benchmark Validation**:
   - Execute comprehensive performance benchmarks
   - Compare results with baseline performance metrics
   - Validate build time performance
   - Test runtime performance of modernized APIs
   - Measure memory usage patterns

3. **Compatibility Testing**:
   - Test backward compatibility with existing client applications
   - Validate API compatibility for downstream users
   - Test interoperability with different Geode versions
   - Validate configuration compatibility
   - Test upgrade/downgrade scenarios

4. **Security Validation**:
   - Execute security test suite
   - Validate security properties of modernized APIs
   - Test authentication and authorization functionality
   - Validate SSL/TLS and encryption functionality
   - Perform security review of all changes

5. **Final Validation Report**:
   - Compile comprehensive validation results
   - Document any issues found and resolutions
   - Create performance comparison analysis
   - Generate compatibility validation report
   - Document security validation results

Requirements:
- All tests must pass with zero warnings
- Performance must be maintained or improved
- Compatibility must be preserved
- Security properties must be maintained
- Comprehensive documentation of validation results

Expected deliverables:
- Complete test execution results
- Performance benchmark comparison
- Compatibility validation report
- Security validation documentation
- Final project validation report

Execute validation in staged environments (development → staging → production-like) to ensure comprehensive coverage.
```

### Prompt 4.4: Documentation and Communication

```
Complete the project with comprehensive documentation updates and community communication:

1. **Documentation Updates**:
   - Update all technical documentation to reflect modern API usage patterns
   - Create comprehensive migration guide for downstream users
   - Document new build configuration and warning policies
   - Update developer guides with modern patterns established
   - Create troubleshooting guide for common migration issues

2. **Migration Guide Creation**:
   - Document all API changes with before/after examples
   - Create step-by-step migration instructions for each deprecated API
   - Include performance implications of API changes
   - Document any breaking changes and workarounds
   - Provide automated migration tools where possible

3. **Community Communication**:
   - Send final project completion announcement to dev and user mailing lists
   - Create blog post documenting project outcomes and lessons learned
   - Update project wiki with final status and results
   - Present results at community meetings or conferences
   - Create FAQ document for common questions

4. **Knowledge Transfer**:
   - Document established modern API patterns for future reference
   - Create coding standards updates reflecting modern practices
   - Document lessons learned and best practices
   - Create training materials for development team
   - Establish ongoing maintenance procedures

5. **Project Closure**:
   - Create final project report with metrics and outcomes
   - Document project timeline and milestone achievements
   - Archive project artifacts and documentation
   - Transfer ongoing maintenance responsibilities
   - Create post-project review and retrospective

Requirements:
- Comprehensive documentation of all changes
- Clear migration paths for all deprecated APIs
- Effective communication to all stakeholders
- Knowledge transfer for ongoing maintenance
- Complete project closure documentation

Expected deliverables:
- Updated technical documentation
- Comprehensive migration guide
- Community communication materials
- Knowledge transfer documentation
- Final project report

Ensure all documentation is accessible and maintainable for future reference.
```

---

# Summary

This implementation plan provides a comprehensive, test-driven approach to removing Java 17 deprecation warnings from Apache Geode. The plan is structured in four phases with detailed prompts for each step, ensuring:

1. **Systematic Approach**: Each phase builds on the previous one with clear dependencies
2. **Risk Mitigation**: Phased implementation with comprehensive testing at each step
3. **Community Involvement**: RFC process ensures architectural decisions are sound
4. **Maintainability**: Comprehensive documentation and knowledge transfer
5. **Quality Assurance**: Test-driven development with performance and security validation

The prompts are designed to be self-contained while building on each other, providing clear guidance for implementation while maintaining flexibility for specific technical decisions during execution.
