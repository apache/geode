# GEODE-10481 — SBOM Implementation Plan (CycloneDX 1.x + Gradle 7.3.3)

## Decisions
- Generate **CycloneDX** SBOMs. Pinned to the CycloneDX **1.x** plugin line because 2.x/3.x require
  Gradle 8+, while Geode builds on **Gradle 7.3.3**. Revisit when Geode moves to Gradle 8.
- Delivered in **4 PRs** (down from the original 12 — the rest was gold-plating or separable).
- **Vulnerability scanning** (Trivy/Grype/SARIF) is **descoped to its own ticket**: it *consumes* an
  SBOM and is a distinct security feature, not part of SBOM generation.

---

## PR 1 — Foundation & compatibility gate  ✅ (this iteration)
Completely inert: no SBOM is produced, zero impact on existing builds.
- [x] Declare CycloneDX plugin in root `build.gradle`, `apply false` (1.x line: `1.7.4`)
- [x] `geode.sbom.enabled` flag in `gradle.properties` (OFF by default)
- [x] `validateGradleCompatibility` task
- [x] Version-comparison logic extracted to `SbomSupport` (`build-tools/geode-build-tools`) and
      unit-tested (`SbomSupportTest`, 5 cases)
- [x] Verified zero impact: build configures, SBOM off, task prints the compatibility result

## PR 2 — Generate SBOMs for all library modules
- [ ] Apply CycloneDX to all non-assembly modules (via `subprojects`/convention), gated by `geode.sbom.enabled`
- [ ] `cyclonedxBom { }` config: schema version, `projectType = library`, output format(s), serial number
- [ ] `generateSbom` aggregator task across modules
- [ ] Integration test: SBOM is produced and schema-valid

## PR 3 — Distribution SBOM + ASF metadata + signing
- [ ] geode-assembly aggregate SBOM (`projectType = application`)
- [ ] ASF metadata (supplier / manufacturer / licenses)
- [ ] Include the SBOM in the distribution archive
- [ ] GPG-sign the SBOM (reuse the existing ASF signing infrastructure)

## PR 4 — CI / release wiring + docs
- [ ] `gradle.yml`: generate + upload SBOM artifact
- [ ] release flow: include + sign SBOM in release artifacts
- [ ] Documentation (how to generate, where SBOMs land)

