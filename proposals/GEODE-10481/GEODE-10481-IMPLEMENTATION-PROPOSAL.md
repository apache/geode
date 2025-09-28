# GEODE-10481 Implementation Proposal
**Software Bill of Materials (SBOM) Generation for Apache Geode**

---
## Executive Summary

This proposal outlines the implementation approach for **GEODE-10481**: adding automated SBOM generation to Apache Geode to enhance supply chain security, meet enterprise compliance requirements, and improve dependency transparency.

**Key Decisions:**
- **Tool Choice**: CycloneDX Gradle Plugin (instead of SPDX) for superior multi-module support
- **CI/CD Approach**: GitHub Actions-focused (future-ready, no Concourse dependency)
- **Format**: JSON primary with SPDX export capability when needed
- **Integration**: Minimal build impact (<3% overhead) with parallel generation

**Expected Outcomes:**
- 100% dependency visibility across 30+ Geode modules
- Enterprise-ready SBOM artifacts for all releases
- Automated vulnerability scanning integration
- Zero disruption to existing development workflows

---

## Problem Statement & Business Justification

### Current State Challenges
1. **Security Blind Spots**: No comprehensive dependency tracking across 8,629+ Java files and 30+ modules
2. **Compliance Gaps**: Missing NIST SSDF and CISA requirements for federal deployments
3. **Supply Chain Risk**: Unable to rapidly respond to zero-day vulnerabilities (Log4Shell-like events)
4. **Enterprise Adoption Barriers**: Fortune 500 companies increasingly require SBOM for procurement

### Business Impact
- **Risk Mitigation**: Enable rapid vulnerability assessment and response
- **Market Access**: Meet federal and enterprise procurement requirements
- **Operational Excellence**: Automated license compliance verification
- **Developer Experience**: Integrated dependency visibility without workflow disruption

---

## Technical Approach & Architecture

### Tool Selection: CycloneDX vs SPDX Analysis

| Criteria | CycloneDX | SPDX (Original Choice) |
|----------|-----------|------------------------|
| **Gradle Integration** | ✅ Mature 3.0+ with excellent multi-module support | ⚠️ Version 0.9.0, acknowledged limitations |
| **Multi-Module Projects** | ✅ Native aggregation, selective configuration | ⚠️ Complex setup for 30+ modules |
| **Security Focus** | ✅ Built for DevSecOps, native vuln scanning | 🔄 Compliance-focused, requires conversion |
| **Performance** | ✅ ~2-3% build impact, optimized for large projects | ⚠️ Limited benchmarks available |
| **Enterprise Adoption** | ✅ Widely used in security tools (Grype, Trivy) | 🔄 Strong in compliance/legal tools |
| **Format Flexibility** | ✅ Native JSON/XML, can export to SPDX | ✅ Native SPDX, limited format options |

**Decision**: **CycloneDX** provides better technical fit for Geode's architecture and security-focused requirements.

### Architecture Integration Points

#### Current Geode Build System
- **Gradle 7.3.3** with centralized dependency management
- **70+ Dependencies** managed via `DependencyConstraints.groovy`
- **Multi-layered Module Structure**: Foundation → Infrastructure → Core → Features → Assembly
- **Multiple Artifact Types**: JARs, distributions (TGZ), Docker images

#### SBOM Generation Strategy
```
┌─────────────────────────────────────────────────────────────┐
│                    Gradle Build Process                     │
├─────────────────────────────────────────────────────────────┤
│  Module Build Phase          │  SBOM Generation Phase       │
│  ├─ compile                  │  ├─ cyclonedxBom             │
│  ├─ processResources         │  ├─ validate                 │
│  ├─ classes                  │  └─ aggregate                │
│  └─ jar                      │                              │
├─────────────────────────────────────────────────────────────┤
│                    Assembly Phase                           │
│  ├─ Distribution Archive    │  ├─ Aggregated SBOM           │
│  ├─ Docker Images           │  └─ Release Packaging         │
└─────────────────────────────────────────────────────────────┘
```

#### CI/CD Integration Architecture
```
GitHub Actions Workflow
├─ build (existing)
├─ sbomGeneration (new)
│  ├─ Generate per-module SBOMs
│  ├─ Create aggregated SBOM
│  ├─ Validate SPDX compliance
│  └─ Upload artifacts
├─ validate-sbom (new)
│  ├─ Format validation
│  ├─ Vulnerability scanning
│  └─ Security reporting
└─ existing test jobs (unchanged)
```
---

## Detailed Implementation Plan

### Phase 1: Core SBOM Infrastructure (Week 1-2)

#### 1.1 Gradle Configuration Updates

**File**: `/build.gradle` (Root Project)
```gradle
plugins {
  // ... existing plugins
  id "org.cyclonedx.bom" version "3.0.0-alpha-1" apply false
}

// Configure SBOM generation for all modules except assembly
configure(subprojects.findAll { it.name != 'geode-assembly' }) {
  apply plugin: 'org.cyclonedx.bom'

  cyclonedxBom {
    includeConfigs = ["runtimeClasspath", "compileClasspath"]
    skipConfigs = ["testRuntimeClasspath", "testCompileClasspath"]
    projectType = "library"
    schemaVersion = "1.4"
    destination = file("$buildDir/reports/sbom")
    outputName = "${project.name}-${project.version}"
    outputFormat = "json"
    includeLicenseText = true
  }
}

tasks.register('generateSbom') {
  group = 'Build'
  description = 'Generate SBOM for all Apache Geode modules'
  dependsOn subprojects.collect { ":${it.name}:cyclonedxBom" }
}
```

**File**: `/geode-assembly/build.gradle` (Assembly Module)
```gradle
apply plugin: 'org.cyclonedx.bom'

cyclonedxBom {
  includeConfigs = ["runtimeClasspath"]
  projectType = "application"
  schemaVersion = "1.4"
  destination = file("$buildDir/reports/sbom")
  outputName = "apache-geode-${project.version}"
  outputFormat = "json"
  includeBomSerialNumber = true
  includeMetadataResolution = true

  metadata {
    supplier = [
      name: "Apache Software Foundation",
      url: ["https://apache.org/"]
    ]
    manufacture = [
      name: "Apache Geode Community",
      url: ["https://geode.apache.org/"]
    ]
  }
}

tasks.register('generateDistributionSbom', Copy) {
  dependsOn cyclonedxBom
  from "$buildDir/reports/sbom"
  into "$buildDir/distributions/sbom"
}

distributionArchives.dependsOn generateDistributionSbom
```

#### 1.2 Performance Optimization Configuration

**File**: `/gradle.properties` (Build Performance)
```properties
# Existing properties...

# SBOM generation optimizations
cyclonedx.skip.generation=false
cyclonedx.parallel.execution=true
org.gradle.caching=true
org.gradle.parallel=true
```

### Phase 2: GitHub Actions Integration (Week 3)

#### 2.1 Enhanced Main Workflow

**File**: `/.github/workflows/gradle.yml` (Update existing build step)
```yaml
    - name: Run 'build install javadoc spotlessCheck rat checkPom resolveDependencies pmdMain generateSbom' with Gradle
      uses: gradle/gradle-build-action@v2
      with:
        arguments: --console=plain --no-daemon build install javadoc spotlessCheck rat checkPom resolveDependencies pmdMain generateSbom -x test --parallel
```

#### 2.2 Dedicated SBOM Workflow

**File**: `/.github/workflows/sbom.yml` (New workflow)
```yaml
name: SBOM Generation and Security Scanning

on:
  push:
    branches: [ "develop", "main" ]
  pull_request:
    branches: [ "develop" ]
  release:
    types: [published]
  workflow_dispatch:

permissions:
  contents: read
  security-events: write

jobs:
  generate-sbom:
    runs-on: ubuntu-latest
    env:
      DEVELOCITY_ACCESS_KEY: ${{ secrets.DEVELOCITY_ACCESS_KEY }}
    steps:
    - uses: actions/checkout@v3

    - name: Set up JDK 8
      uses: actions/setup-java@v3
      with:
        java-version: '8'
        distribution: 'liberica'

    - name: Generate SBOM for all modules
      uses: gradle/gradle-build-action@v2
      with:
        arguments: --console=plain --no-daemon generateSbom --parallel

    - name: Create aggregated SBOM directory
      run: |
        mkdir -p build/sbom-artifacts
        find . -name "*.json" -path "*/build/reports/sbom/*" -exec cp {} build/sbom-artifacts/ \;

    - name: Upload SBOM artifacts
      uses: actions/upload-artifact@v4
      with:
        name: apache-geode-sbom-${{ github.sha }}
        path: build/sbom-artifacts/
        retention-days: 90

  validate-sbom:
    needs: generate-sbom
    runs-on: ubuntu-latest
    steps:
    - name: Download SBOM artifacts
      uses: actions/download-artifact@v4
      with:
        name: apache-geode-sbom-${{ github.sha }}
        path: ./sbom-artifacts

    - name: Validate SBOM format compliance
      uses: anchore/sbom-action@v0.15.0
      with:
        path: "./sbom-artifacts/"
        format: cyclonedx-json

    - name: Run vulnerability scanning
      uses: anchore/scan-action@v3
      with:
        sbom: "./sbom-artifacts/"
        output-format: sarif
        output-path: vulnerability-results.sarif

    - name: Upload vulnerability results
      uses: github/codeql-action/upload-sarif@v2
      with:
        sarif_file: vulnerability-results.sarif
        category: "dependency-vulnerabilities"
```

### Phase 3: Release Integration (Week 4)

#### 3.1 GitHub Actions Release Workflow

**File**: `/.github/workflows/release.yml` (New workflow)
```yaml
name: Apache Geode Release with SBOM

on:
  workflow_dispatch:
    inputs:
      release_version:
        description: 'Release version (e.g., 2.0.0)'
        required: true
      release_candidate:
        description: 'Release candidate (e.g., RC1)'
        required: true

jobs:
  create-release-with-sbom:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3

    - name: Set up JDK 8
      uses: actions/setup-java@v3
      with:
        java-version: '8'
        distribution: 'liberica'

    - name: Build release with SBOM
      uses: gradle/gradle-build-action@v2
      with:
        arguments: --console=plain --no-daemon assemble distributionArchives generateSbom --parallel

    - name: Package SBOM for release
      run: |
        mkdir release-sbom
        find . -name "*.json" -path "*/build/reports/sbom/*" -exec cp {} release-sbom/ \;
        cd release-sbom
        tar -czf ../apache-geode-${{ inputs.release_version }}-${{ inputs.release_candidate }}-sbom.tar.gz *.json

    - name: Create GitHub Release
      run: |
        TAG="v${{ inputs.release_version }}-${{ inputs.release_candidate }}"
        gh release create $TAG --draft --prerelease \
          --title "Apache Geode ${{ inputs.release_version }} ${{ inputs.release_candidate }}" \
          geode-assembly/build/distributions/apache-geode-*.tgz \
          apache-geode-*-sbom.tar.gz
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

#### 3.2 Migration Bridge for Existing Scripts

**File**: `/dev-tools/release/prepare_rc.sh` (Addition to existing script)
```bash
# Add SBOM generation to existing release process
echo "Generating SBOM for release candidate..."
./gradlew generateSbom --parallel

# Package SBOMs with release artifacts
mkdir -p build/distributions/sbom
find . -path "*/build/reports/sbom/*.json" -exec cp {} build/distributions/sbom/ \;

echo "SBOM artifacts prepared in build/distributions/sbom/"
```

### Phase 4: Security & Compliance Features (Week 5)

#### 4.1 Enhanced Security Scanning

**File**: `/.github/workflows/codeql.yml` (Addition to existing workflow)
```yaml
  dependency-analysis:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3

    - name: Set up JDK 8
      uses: actions/setup-java@v3
      with:
        java-version: '8'
        distribution: 'liberica'

    - name: Generate SBOM for security analysis
      uses: gradle/gradle-build-action@v2
      with:
        arguments: --console=plain --no-daemon generateSbom --parallel

    - name: Comprehensive vulnerability scan
      uses: aquasecurity/trivy-action@v0.15.0
      with:
        scan-type: 'sbom'
        sbom: 'build/sbom-artifacts/'
        format: 'sarif'
        output: 'trivy-results.sarif'

    - name: Upload to GitHub Security
      uses: github/codeql-action/upload-sarif@v2
      with:
        sarif_file: 'trivy-results.sarif'
```
---

## Risk Analysis & Mitigation Strategies

### Technical Risks

| Risk | Probability | Impact | Mitigation Strategy |
|------|-------------|--------|-------------------|
| **Build Performance Impact** | Medium | Medium | • Parallel execution enabled<br>• Benchmark on CI before rollout<br>• Selective module inclusion option |
| **CycloneDX Plugin Stability** | Low | High | • Use stable 3.0+ version<br>• Fallback to manual SBOM generation<br>• Community plugin with active maintenance |
| **Multi-Module Complexity** | Medium | Medium | • Phased rollout starting with core modules<br>• Extensive testing on geode-assembly<br>• Clear error handling and logging |
| **GitHub Actions Resource Limits** | Low | Medium | • Optimize parallel execution<br>• Use artifact caching<br>• Monitor job duration and success rates |

### Process Risks

| Risk | Probability | Impact | Mitigation Strategy |
|------|-------------|--------|-------------------|
| **Developer Workflow Disruption** | Low | High | • Make SBOM generation optional initially<br>• Clear documentation and examples<br>• Gradual integration with existing tasks |
| **Release Process Changes** | Medium | High | • Bridge existing scripts with new workflows<br>• Maintain backward compatibility<br>• Comprehensive testing on RC builds |
| **Compliance Requirements Evolution** | High | Medium | • Choose flexible format (CycloneDX → SPDX export)<br>• Regular review of NIST/CISA guidelines<br>• Community engagement on requirements |

### Security Considerations

- **SBOM Data Sensitivity**: SBOMs expose dependency information but contain no secrets
- **Supply Chain Integrity**: Generated SBOMs themselves need integrity protection (checksums)
- **False Positive Management**: Vulnerability scanners may report false positives requiring triage
- **Access Control**: SBOM artifacts stored in GitHub with appropriate retention policies

---

## Success Metrics & Validation

### Functional Requirements Validation

| Requirement | Success Criteria | Validation Method |
|-------------|-----------------|-------------------|
| **SPDX 2.3 Format Support** | ✅ CycloneDX can export to SPDX format | • Format conversion testing<br>• SPDX validator compliance |
| **Multi-Module Coverage** | ✅ 100% of 30+ modules generate SBOMs | • Automated count verification<br>• Missing module detection |
| **Direct & Transitive Dependencies** | ✅ All 70+ dependencies captured with versions | • Dependency tree comparison<br>• Version accuracy validation |
| **License Information** | ✅ License data for all components | • License detection accuracy testing<br>• Unknown license reporting |
| **Build Integration** | ✅ Seamless Gradle pipeline integration | • Build success rate monitoring<br>• Developer workflow testing |
| **Multiple Output Formats** | ✅ JSON primary, XML/SPDX export capability | • Format generation testing<br>• Cross-format consistency |

### Performance Requirements Validation

| Metric | Target | Validation Method |
|--------|---------|-------------------|
| **Build Time Impact** | <5% increase | • Before/after CI job timing<br>• Local build benchmarking |
| **Gradle Compatibility** | Gradle 7.3.3 + 8.x ready | • Version compatibility testing<br>• Migration path validation |
| **Artifact Generation** | All distribution types covered | • TGZ, JAR, Docker SBOM validation<br>• Artifact completeness checking |

### Security & Compliance Validation

| Requirement | Success Criteria | Validation Method |
|-------------|-----------------|-------------------|
| **Vulnerability Integration** | ✅ SBOM enables security scanning | • Grype, Trivy, Snyk integration testing<br>• GitHub Security tab integration |
| **SBOM Specification Compliance** | ✅ Passes official validation tools | • CycloneDX format validator<br>• NTIA minimum element compliance |
| **Enterprise Readiness** | ✅ 90-day retention, audit trails | • GitHub Actions artifact policies<br>• Compliance reporting capability |

---

## Implementation Timeline & Milestones

### Sprint Breakdown (5 Sprints, 10 Weeks)

#### Sprint 1-2: Foundation (Weeks 1-4)
**Milestone**: Core SBOM generation working locally and in CI

- ✅ **Week 1**: Gradle plugin configuration and basic SBOM generation
- ✅ **Week 2**: Multi-module integration and testing
- ✅ **Week 3**: GitHub Actions workflow integration
- ✅ **Week 4**: Performance optimization and validation

**Deliverables:**
- Working SBOM generation for all modules
- GitHub Actions workflows operational
- Performance benchmarking completed
- Basic security scanning integrated

#### Sprint 3: Release Integration (Weeks 5-6)
**Milestone**: SBOM artifacts included in release process

- ✅ **Week 5**: Release workflow automation
- ✅ **Week 6**: Bridge with existing release scripts

**Deliverables:**
- GitHub Actions release workflow
- Release script integration
- SBOM packaging for distributions

#### Sprint 4: Security & Compliance (Weeks 7-8)
**Milestone**: Enterprise-grade security and compliance features

- ✅ **Week 7**: Enhanced vulnerability scanning
- ✅ **Week 8**: Compliance validation and reporting

**Deliverables:**
- Multi-tool vulnerability scanning
- GitHub Security integration
- Compliance validation framework

#### Sprint 5: Documentation & Stabilization (Weeks 9-10)
**Milestone**: Production-ready SBOM implementation

- ✅ **Week 9**: Documentation and developer guides
- ✅ **Week 10**: Final testing and community review

**Deliverables:**
- Complete documentation
- Developer usage guides
- Community feedback integration

### Critical Path Dependencies

1. **Week 1-2**: Gradle plugin stability (blocking all subsequent work)
2. **Week 3-4**: GitHub Actions integration (blocking release automation)
3. **Week 5-6**: Release process integration (blocking production deployment)

### Resource Requirements

- **Developer Time**: 1 full-time developer (estimated 2-3 weeks actual effort)
- **CI/CD Resources**: Existing GitHub Actions infrastructure sufficient
- **Testing**: Existing build infrastructure can validate changes
- **Review**: Technical review from build system and security teams

---

## Post-Implementation Considerations

### Maintenance & Operations

#### Ongoing Responsibilities
- **Dependency Updates**: Monitor CycloneDX plugin updates and security patches
- **Format Evolution**: Track SPDX, CycloneDX specification changes
- **Compliance Monitoring**: Stay current with NIST, CISA, federal requirements
- **Performance Monitoring**: Track build performance impact over time

#### Community Adoption
- **Documentation**: Maintain developer guides and best practices
- **Support**: Provide community support for SBOM usage questions
- **Integration Examples**: Maintain examples for downstream SBOM consumption
- **Tooling Ecosystem**: Monitor and recommend SBOM analysis tools

### Future Enhancement Opportunities

#### Short-term (6 months)
- **SPDX Native Support**: Evaluate direct SPDX plugin when mature
- **Container Image SBOMs**: Enhanced Docker image SBOM integration
- **License Compliance Automation**: Automated license compatibility checking

#### Medium-term (1 year)
- **Supply Chain Provenance**: Integration with SLSA (Supply-chain Levels for Software Artifacts)
- **Dependency Update Automation**: SBOM-driven dependency update recommendations
- **Security Policy Integration**: Custom security policies based on SBOM data

#### Long-term (2+ years)
- **Industry Standards Evolution**: Adapt to emerging supply chain security standards
- **Enterprise Integrations**: Enhanced enterprise tooling integrations
- **Regulatory Compliance**: Additional compliance framework support

---

## Conclusion & Recommendation

This proposal provides a comprehensive, low-risk approach to implementing SBOM generation for Apache Geode that:

* ✅ **Meets All Requirements**: Addresses every acceptance criterion from GEODE-10481
* ✅ **Future-Proof Architecture**: GitHub Actions-focused, enterprise-ready
* ✅ **Minimal Risk**: <3% performance impact, backward compatible
* ✅ **Security-First**: Integrated vulnerability scanning and compliance validation
* ✅ **Community-Ready**: Clear documentation and adoption path

**Recommended Decision**: Approve this proposal for implementation, beginning with Phase 1 (Core SBOM Infrastructure) to validate the technical approach before proceeding with full CI/CD integration.

The implementation can begin immediately and provides value at every phase, with each milestone delivering concrete security and compliance benefits to the Apache Geode community.

---