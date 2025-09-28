# GEODE-10481 SBOM Implementation Plan

## Overview
This plan breaks down the SBOM implementation into small, manageable pull requests that build incrementally without breaking existing functionality. Each step is designed to be safely testable and provides value while building toward the complete solution.

## High-Level Strategy
1. **Foundation First**: Start with basic plugin integration and validation
2. **Context-Aware Logic**: Add smart generation detection
3. **Core Module Integration**: Implement SBOM generation for individual modules
4. **Assembly Integration**: Add aggregated SBOM generation
5. **CI/CD Integration**: Add GitHub Actions workflows
6. **ASF Compliance**: Add signing and compliance features
7. **Security Integration**: Add vulnerability scanning
8. **Documentation & Polish**: Complete the implementation

## Risk Mitigation Strategies

### Build Safety
- All changes are feature-flagged and disabled by default initially
- Context detection ensures no impact on developer local builds
- Comprehensive rollback procedures for each PR
- Extensive testing at each step

### Performance Protection
- Parallel execution to minimize build time impact
- Gradle caching to optimize repeated builds
- Performance benchmarking at each phase
- <3% build time impact target maintained

### Compatibility Assurance
- Gradle 8.5+ compatibility validation from start
- Java 21+ readiness verification
- Plugin version compatibility testing
- Future migration path planning

## Implementation Prompts for Code Generation

## PR 1: Plugin Foundation & Compatibility Validation

### Context
This is the first PR in implementing SBOM generation for Apache Geode (GEODE-10481). We need to establish the foundation by adding the CycloneDX plugin and compatibility validation without affecting existing builds.

### Prompt for PR 1

```
You are implementing SBOM generation for Apache Geode using the CycloneDX Gradle plugin. This is PR 1 of a 15-PR implementation plan.

REQUIREMENTS:
1. Add CycloneDX Gradle plugin to the root build.gradle file (version 3.0.0-alpha-1)
2. Apply the plugin as "apply false" initially to avoid affecting existing builds
3. Create a validateGradleCompatibility task that checks current Gradle and Java versions
4. Add basic plugin configuration structure (but keep it disabled by default)
5. Create unit tests to verify the compatibility validation logic
6. Ensure zero impact on existing build processes

CURRENT STATE:
- Apache Geode project with 30+ modules
- Gradle 7.3.3 with existing plugin configuration
- Java 8 primary, but planning for Java 21+ compatibility
- Existing build.gradle has plugins block starting at line 18

IMPLEMENTATION APPROACH:
1. Add the CycloneDX plugin to the plugins block with "apply false"
2. Create validateGradleCompatibility task in the root build.gradle
3. Add basic configuration structure that will be used in later PRs
4. Write comprehensive tests for the compatibility validation
5. Verify that existing builds are unaffected

TESTING REQUIREMENTS:
- Unit tests for validateGradleCompatibility task
- Integration test to verify plugin loads without errors
- Verification that existing gradle tasks still work
- Performance test to ensure no build time impact

ACCEPTANCE CRITERIA:
- CycloneDX plugin is available but not active
- validateGradleCompatibility task provides useful version information
- All existing tests pass without modification
- No performance impact on existing builds
- Clear logging about compatibility status

Please implement this step with comprehensive testing and ensure it's safe to merge without affecting any existing functionality.
```

## PR 2: Context Detection Logic

### Context
Building on PR 1, we now need to implement the smart context detection that determines when SBOM generation should occur (CI, release, explicit).

### Prompt for PR 2

```
You are continuing the SBOM implementation for Apache Geode. This is PR 2, building on the foundation from PR 1.

REQUIREMENTS:
1. Implement context-aware SBOM generation detection logic
2. Add shouldGenerateSbom logic that detects CI, release, and explicit SBOM contexts
3. Add configuration properties to gradle.properties for SBOM optimization
4. Create comprehensive unit tests for all context scenarios
5. Ensure the logic is isolated and doesn't affect builds when SBOM is disabled

CONTEXT DETECTION LOGIC NEEDED:
- isCI: Detect when running in CI environment (System.getenv("CI") == "true")
- isRelease: Detect release builds (task names contain "release", "distribution", "assemble")
- isExplicitSbom: Detect when generateSbom task is explicitly requested
- shouldGenerateSbom: Combine all contexts (isCI || isRelease || isExplicitSbom)

GRADLE.PROPERTIES ADDITIONS:
- cyclonedx.skip.generation=false
- cyclonedx.parallel.execution=true
- org.gradle.caching=true (if not already present)
- org.gradle.parallel=true (if not already present)

TESTING REQUIREMENTS:
- Unit tests for each context detection scenario
- Mock environment variable testing for CI detection
- Mock gradle task parameter testing for release detection
- Integration tests for shouldGenerateSbom logic
- Verification that logic works correctly in all combinations

IMPLEMENTATION APPROACH:
1. Add context detection variables to root build.gradle
2. Implement shouldGenerateSbom logic with proper boolean combinations
3. Add gradle.properties optimizations
4. Create comprehensive test suite for all scenarios
5. Add logging to help debug context detection

ACCEPTANCE CRITERIA:
- Context detection works correctly in all scenarios (dev, CI, release, explicit)
- Logic is well-tested with >95% coverage
- No impact on existing builds when SBOM is not requested
- Clear logging shows which context triggered SBOM generation
- Performance optimizations are properly configured

Build on the foundation from PR 1 and ensure this logic will be used by subsequent PRs for conditional SBOM generation.
```

## PR 3: Basic SBOM Generation for Single Module

### Context
With the foundation and context detection in place, we now implement actual SBOM generation for a single module to validate the approach.

### Prompt for PR 3

```
You are continuing the SBOM implementation for Apache Geode. This is PR 3, building on PR 1 (plugin foundation) and PR 2 (context detection).

REQUIREMENTS:
1. Enable SBOM generation for geode-common module only (as a test case)
2. Configure basic CycloneDX settings for the module
3. Implement SBOM format validation and content verification
4. Add integration tests to verify SBOM accuracy
5. Measure and document performance impact

MODULE CONFIGURATION NEEDED:
- Apply CycloneDX plugin to geode-common module
- Configure cyclonedxBom task with proper settings:
  - includeConfigs: ["runtimeClasspath", "compileClasspath"]
  - skipConfigs: ["testRuntimeClasspath", "testCompileClasspath"]
  - projectType: "library"
  - schemaVersion: "1.4"
  - outputFormat: "json"
  - includeLicenseText: true

TESTING REQUIREMENTS:
- Integration test that generates SBOM for geode-common
- Validation that SBOM contains expected dependencies
- Format validation using CycloneDX schema
- Content verification (component names, versions, licenses)
- Performance measurement (build time impact)
- Verification that SBOM is only generated in appropriate contexts

IMPLEMENTATION APPROACH:
1. Add CycloneDX plugin configuration to geode-common/build.gradle
2. Use shouldGenerateSbom logic from PR 2 to control generation
3. Configure SBOM output location and naming
4. Create comprehensive integration tests
5. Add SBOM content validation utilities
6. Measure and document performance impact

ACCEPTANCE CRITERIA:
- SBOM is generated for geode-common when shouldGenerateSbom is true
- SBOM contains accurate dependency information
- SBOM format validates against CycloneDX schema
- Performance impact is <1% for single module
- Integration tests verify SBOM content accuracy
- No SBOM generation when shouldGenerateSbom is false

VALIDATION REQUIREMENTS:
- Verify SBOM contains all runtime and compile dependencies
- Check that test dependencies are excluded
- Validate license information is included where available
- Ensure component versions match actual dependency versions
- Confirm SBOM serial number and metadata are present

This PR establishes the pattern that will be used for all modules in subsequent PRs.
```

## PR 4: Multi-Module SBOM Configuration

### Context
Expanding from single module to all non-assembly modules, implementing coordinated SBOM generation.

### Prompt for PR 4

```
You are continuing the SBOM implementation for Apache Geode. This is PR 4, building on the successful single-module implementation from PR 3.

REQUIREMENTS:
1. Apply SBOM configuration to all non-assembly modules (30+ modules)
2. Implement generateSbom coordinating task for all modules
3. Add module-specific configuration handling
4. Create comprehensive multi-module integration tests
5. Perform performance benchmarking across all modules

MULTI-MODULE CONFIGURATION:
- Apply the pattern from PR 3 to all subprojects except 'geode-assembly'
- Use configure(subprojects.findAll { it.name != 'geode-assembly' }) pattern
- Ensure each module gets appropriate cyclonedxBom configuration
- Create generateSbom task that coordinates all module SBOM generation

TASK COORDINATION:
- generateSbom task should depend on all module cyclonedxBom tasks
- Proper task ordering and dependency management
- Parallel execution where possible
- Clear progress reporting during generation

TESTING REQUIREMENTS:
- Integration test that generates SBOMs for all modules
- Validation that each module produces valid SBOM
- Cross-module dependency verification
- Performance benchmarking for full multi-module generation
- Memory usage monitoring during bulk generation

IMPLEMENTATION APPROACH:
1. Extract SBOM configuration into reusable pattern
2. Apply configuration to all eligible subprojects
3. Create coordinating generateSbom task
4. Add comprehensive multi-module testing
5. Implement performance monitoring and optimization
6. Add proper error handling and reporting

PERFORMANCE REQUIREMENTS:
- Total build time impact <3% when SBOM generation is enabled
- Parallel execution to minimize sequential overhead
- Proper Gradle task caching to avoid redundant work
- Memory-efficient processing for large module count

ACCEPTANCE CRITERIA:
- All 30+ modules generate valid SBOMs when requested
- generateSbom task successfully coordinates all module generation
- Performance impact stays within acceptable limits
- Integration tests verify multi-module SBOM accuracy
- Error handling provides clear feedback for any failures
- Task dependencies are properly configured

This PR scales the approach to the full project while maintaining performance and reliability.
```

## PR 5: Assembly Module Integration

### Context
Adding SBOM generation to the geode-assembly module with ASF compliance metadata and distribution packaging.

### Prompt for PR 5

```
You are continuing the SBOM implementation for Apache Geode. This is PR 5, focusing on the critical geode-assembly module integration.

REQUIREMENTS:
1. Configure SBOM generation for geode-assembly module with application-specific settings
2. Add ASF compliance metadata (supplier, manufacturer information)
3. Implement generateDistributionSbom task for packaging
4. Add assembly-specific SBOM validation tests
5. Integrate with existing distribution packaging process

ASSEMBLY-SPECIFIC CONFIGURATION:
- projectType: "application" (different from library modules)
- includeConfigs: ["runtimeClasspath"] (assembly runtime dependencies)
- ASF metadata:
  - supplier: "Apache Software Foundation"
  - manufacturer: "Apache Geode Community"
  - appropriate URLs and contact information
- outputName: "apache-geode-${project.version}"

DISTRIBUTION INTEGRATION:
- generateDistributionSbom task copies SBOM to distributions/sbom directory
- Integration with existing distributionArchives task
- Proper task dependencies to ensure SBOM is included in distributions
- Maintain compatibility with existing assembly process

TESTING REQUIREMENTS:
- Integration test for assembly SBOM generation
- Validation of ASF compliance metadata
- Verification that SBOM is included in distribution packages
- Content validation for assembly-level dependencies
- Distribution packaging integration testing

IMPLEMENTATION APPROACH:
1. Add CycloneDX configuration to geode-assembly/build.gradle
2. Configure application-specific SBOM settings
3. Add ASF compliance metadata
4. Create generateDistributionSbom task
5. Integrate with existing distribution tasks
6. Add comprehensive testing for assembly SBOM

ASF COMPLIANCE REQUIREMENTS:
- Supplier information must identify Apache Software Foundation
- Manufacturer information must identify Apache Geode Community
- Include appropriate project URLs
- Ensure metadata follows ASF standards
- Add timestamp for deterministic generation

ACCEPTANCE CRITERIA:
- geode-assembly generates application-type SBOM
- ASF compliance metadata is correctly included
- SBOM is packaged with distribution artifacts
- Integration with existing assembly process is seamless
- Assembly SBOM contains aggregated dependency information
- Distribution packages include SBOM artifacts

This PR completes the core SBOM generation functionality across all modules.
```

## PR 6: Performance Optimization & Caching

### Context
Optimizing SBOM generation performance and implementing proper Gradle caching.

### Prompt for PR 6

```
You are continuing the SBOM implementation for Apache Geode. This is PR 6, focusing on performance optimization and caching.

REQUIREMENTS:
1. Enable parallel execution for SBOM generation across modules
2. Implement proper Gradle build caching for SBOM tasks
3. Add performance monitoring and benchmarking
4. Optimize for <3% total build time impact
5. Add performance regression testing

PERFORMANCE OPTIMIZATIONS:
- Enable parallel execution in gradle.properties
- Configure Gradle task caching for cyclonedxBom tasks
- Optimize task dependencies to allow maximum parallelization
- Add task output caching to avoid redundant SBOM generation
- Implement incremental build support

CACHING STRATEGY:
- Mark cyclonedxBom tasks as cacheable
- Define proper task inputs and outputs for cache key generation
- Ensure SBOM generation is skipped when dependencies haven't changed
- Add cache validation to ensure correctness
- Configure remote cache compatibility if applicable

MONITORING AND BENCHMARKING:
- Add build time measurement for SBOM generation
- Create performance regression tests
- Implement memory usage monitoring
- Add reporting for cache hit/miss rates
- Create performance dashboard/reporting

IMPLEMENTATION APPROACH:
1. Configure parallel execution settings
2. Add proper task caching annotations
3. Optimize task dependency graph
4. Implement performance monitoring
5. Add regression testing
6. Create performance reporting

TESTING REQUIREMENTS:
- Performance regression tests comparing before/after
- Cache effectiveness validation
- Parallel execution correctness verification
- Memory usage profiling
- Build time impact measurement across different scenarios

ACCEPTANCE CRITERIA:
- SBOM generation runs in parallel across modules
- Gradle caching reduces redundant SBOM generation
- Total build time impact is <3% when SBOM is enabled
- Performance regression tests prevent future slowdowns
- Cache hit rates are >80% for unchanged dependencies
- Memory usage remains within acceptable limits

OPTIMIZATION TARGETS:
- Single module SBOM generation: <30 seconds
- Full multi-module generation: <5 minutes
- Cache hit scenario: <10 seconds total
- Memory usage: <500MB additional heap
- Parallel efficiency: >70% of theoretical maximum

This PR ensures the SBOM implementation is production-ready from a performance perspective.
```

## PR 7: Basic GitHub Actions Integration

### Context
Integrating SBOM generation into existing GitHub Actions workflows.

### Prompt for PR 7

```
You are continuing the SBOM implementation for Apache Geode. This is PR 7, integrating SBOM generation into CI/CD workflows.

REQUIREMENTS:
1. Update existing .github/workflows/gradle.yml to include generateSbom
2. Add conditional SBOM generation in CI environment
3. Implement SBOM artifact upload for CI builds
4. Ensure backward compatibility with existing workflow
5. Add proper error handling and fallback options

WORKFLOW MODIFICATIONS:
- Update the existing gradle build step to include generateSbom
- Modify the arguments line to add generateSbom task
- Ensure SBOM generation only occurs in CI context (using context detection from PR 2)
- Add artifact upload step for generated SBOMs
- Maintain all existing functionality

CURRENT WORKFLOW INTEGRATION:
- Existing step: "Run 'build install javadoc spotlessCheck rat checkPom resolveDependencies pmdMain' with Gradle"
- Updated step: Add generateSbom to the arguments
- Add new step for SBOM artifact upload
- Ensure proper step dependencies and error handling

ARTIFACT MANAGEMENT:
- Upload SBOM artifacts with retention policy (90 days)
- Use descriptive artifact names with build metadata
- Include all module SBOMs in artifact package
- Add artifact size and content validation

IMPLEMENTATION APPROACH:
1. Modify existing gradle.yml workflow file
2. Update Gradle arguments to include generateSbom
3. Add SBOM artifact collection and upload step
4. Add proper error handling and conditional execution
5. Test workflow modifications thoroughly
6. Ensure no impact on existing workflow functionality

TESTING REQUIREMENTS:
- Workflow execution testing in CI environment
- Artifact upload and download verification
- Error handling validation
- Performance impact measurement on CI builds
- Backward compatibility verification

ACCEPTANCE CRITERIA:
- SBOM generation occurs automatically in CI builds
- SBOM artifacts are uploaded and accessible
- Existing workflow functionality is preserved
- Error handling prevents workflow failures
- CI build time impact is minimal
- Artifacts contain all expected SBOM files

SAFETY MEASURES:
- Conditional execution to prevent failures if SBOM generation fails
- Proper error messages and logging
- Fallback options if artifact upload fails
- Monitoring for workflow success rates
- Easy rollback capability

This PR integrates SBOM generation into the existing CI pipeline safely and effectively.
```

## PR 8: Dedicated SBOM Workflow

### Context
Creating a comprehensive SBOM-specific workflow for validation and security scanning.

### Prompt for PR 8

```
You are continuing the SBOM implementation for Apache Geode. This is PR 8, creating a dedicated SBOM workflow for comprehensive processing.

REQUIREMENTS:
1. Create new .github/workflows/sbom.yml workflow
2. Implement SBOM format validation in CI
3. Add basic security scanning integration
4. Create comprehensive SBOM processing pipeline
5. Add proper workflow triggers and permissions

WORKFLOW STRUCTURE:
- Trigger on push to develop/main, pull requests, releases, and manual dispatch
- Two main jobs: generate-sbom and validate-sbom
- Proper job dependencies and artifact passing
- Security permissions for vulnerability reporting

SBOM GENERATION JOB:
- Set up Java 8 environment (matching existing workflows)
- Run generateSbom task with proper arguments
- Collect all generated SBOM files
- Upload artifacts with proper naming and retention
- Add build metadata and versioning

SBOM VALIDATION JOB:
- Download SBOM artifacts from generation job
- Validate SBOM format compliance (CycloneDX schema)
- Run basic vulnerability scanning (Anchore/Grype)
- Generate SARIF reports for GitHub Security
- Upload security findings

IMPLEMENTATION APPROACH:
1. Create new sbom.yml workflow file
2. Define proper triggers and permissions
3. Implement generate-sbom job
4. Implement validate-sbom job
5. Add artifact management between jobs
6. Configure security scanning and reporting

SECURITY INTEGRATION:
- Use Anchore SBOM Action for validation
- Implement Grype or Trivy for vulnerability scanning
- Generate SARIF format reports
- Upload to GitHub Security tab
- Add proper security permissions

TESTING REQUIREMENTS:
- Workflow execution testing
- SBOM validation accuracy verification
- Security scanning functionality testing
- Artifact management validation
- Error handling and recovery testing

ACCEPTANCE CRITERIA:
- Dedicated SBOM workflow executes successfully
- SBOM format validation catches format issues
- Security scanning identifies vulnerabilities
- SARIF reports appear in GitHub Security tab
- Workflow provides clear feedback on SBOM quality
- Proper artifact management between jobs

WORKFLOW TRIGGERS:
- Push to develop and main branches
- Pull requests to develop
- Release events
- Manual workflow dispatch
- Scheduled runs for security monitoring

This PR creates a comprehensive SBOM processing pipeline for quality assurance and security monitoring.
```

## PR 9: Release Workflow Integration

### Context
Integrating SBOM generation into release processes and packaging.

### Prompt for PR 9

```
You are continuing the SBOM implementation for Apache Geode. This is PR 9, integrating SBOM generation into release workflows.

REQUIREMENTS:
1. Create release.yml workflow for SBOM-enabled releases
2. Add SBOM packaging for release artifacts
3. Implement release candidate SBOM generation
4. Update existing release scripts for SBOM integration
5. Ensure SBOM artifacts are included in all release packages

RELEASE WORKFLOW FEATURES:
- Manual workflow dispatch with release version and RC inputs
- Build release with SBOM generation enabled
- Package SBOM artifacts with release distributions
- Create GitHub releases with SBOM attachments
- Validate SBOM compliance before release

RELEASE SCRIPT INTEGRATION:
- Update dev-tools/release/prepare_rc.sh to include SBOM generation
- Add SBOM validation to release preparation process
- Package signed SBOM artifacts with release distributions
- Add SBOM verification to release checklist

IMPLEMENTATION APPROACH:
1. Create new release.yml workflow
2. Add SBOM generation to release build process
3. Implement SBOM packaging and signing
4. Update existing release scripts
5. Add release validation steps
6. Test complete release process

PACKAGING REQUIREMENTS:
- Include SBOM files in distribution archives
- Create separate SBOM artifact packages
- Add SBOM checksums and signatures
- Maintain release artifact naming conventions
- Ensure SBOM artifacts follow ASF release standards

ACCEPTANCE CRITERIA:
- Release workflow generates SBOMs for all components
- SBOM artifacts are packaged with release distributions
- Release scripts include SBOM generation and validation
- GitHub releases include SBOM attachments
- Release process maintains existing functionality
- SBOM artifacts follow ASF release standards

This PR completes the integration of SBOM generation into the complete release pipeline.
```

## Summary of Implementation Approach

The implementation plan provides 12 carefully structured pull requests organized into 6 logical phases that build incrementally:

### Phase 1: Foundation & Infrastructure (PRs 1-2)
- Establishes plugin infrastructure safely
- Implements context-aware generation logic

### Phase 2: Core SBOM Generation (PRs 3-5)
- Validates approach with single module
- Scales to all modules
- Adds assembly integration

### Phase 3: Performance & Production Readiness (PR 6)
- Optimizes performance and implements caching

### Phase 4: CI/CD Integration (PRs 7-9)
- Integrates with existing workflows
- Adds dedicated SBOM processing
- Completes release integration

### Phase 5: Compliance & Security (PRs 10-11)
- Adds ASF compliance and signing
- Implements security scanning and format validation

### Phase 6: Documentation & Finalization (PR 12)
- Adds comprehensive documentation
- Implements full testing suite
- Addresses community feedback and final polish

Each PR is designed to be:
- **Independently testable** - Can be validated in isolation
- **Safely reversible** - Can be rolled back without impact
- **Incrementally valuable** - Provides benefit even if later PRs are delayed
- **Performance conscious** - Maintains <3% build time impact
- **Community friendly** - Includes proper documentation and testing

The phases provide logical groupings of related work while maintaining appropriate granularity for review and implementation. The prompts provide detailed implementation guidance while maintaining flexibility for the implementing developer to make appropriate technical decisions within the established framework.
