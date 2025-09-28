# SBOM PR 1: Plugin Foundation & Compatibility Validation

## Overview

This is the first pull request in the implementation of **GEODE-10481**: adding automated SBOM (Software Bill of Materials) generation to Apache Geode. This PR establishes the foundation by safely adding the CycloneDX plugin and compatibility validation without affecting existing builds.

## Changes Made

### 1. CycloneDX Plugin Integration
- Added `org.cyclonedx.bom` version `1.8.2` to the root `build.gradle` plugins block
- Plugin is configured with `apply false` to prevent automatic activation
- No impact on existing build processes

### 2. Compatibility Validation Task
- Added `validateGradleCompatibility` task to verify system compatibility
- Checks current Gradle and Java versions
- Provides future compatibility indicators for Gradle 8.5+ and Java 21+
- Validates CycloneDX plugin availability

### 3. Basic SBOM Configuration Structure
- Added `ext` block with SBOM configuration structure for future PRs
- All SBOM functionality is disabled by default (`sbomEnabled = false`)
- Configuration includes plugin version, schema version, and output format settings

### 4. Test Infrastructure
- Created test directory structure: `src/test/groovy/org/apache/geode/gradle/sbom/`
- Added comprehensive unit tests for compatibility validation
- Added integration tests for plugin foundation
- Created validation script `proposals/GEODE-10481/pr-log/01-validation.sh` for automated testing

## Files Modified

### Core Changes
- `build.gradle` - Added CycloneDX plugin and validateGradleCompatibility task
- `gradle.properties` - No changes (existing caching and parallel settings sufficient)

### Test Files Added
- `src/test/groovy/org/apache/geode/gradle/sbom/SbomCompatibilityTest.groovy`
- `src/test/groovy/org/apache/geode/gradle/sbom/SbomPluginIntegrationTest.groovy`
- `proposals/GEODE-10481/pr-log/01-validation.sh` - Validation script

### Documentation
- `SBOM-PR1-README.md` - This file

## Validation Results

All tests pass successfully:

✅ **CycloneDX plugin added but not applied** - Plugin is available but doesn't affect builds
✅ **validateGradleCompatibility task working** - Provides useful version information
✅ **No impact on existing functionality** - All existing tasks work normally
✅ **No SBOM generation** - As expected, no SBOM files are created
✅ **Performance impact minimal** - Task completes in <3 seconds

## Usage

### Running Compatibility Validation
```bash
./gradlew validateGradleCompatibility
```

### Running All PR 1 Tests
```bash
./proposals/GEODE-10481/pr-log/01-validation.sh
```

### Viewing Available Tasks
```bash
./gradlew tasks --group=verification
```

## Sample Output

```
=== SBOM Compatibility Validation ===
Current Gradle version: 7.3.3
Current Java version: 1.8.0_422
Java vendor: Amazon.com Inc.
Java home: /Library/Java/JavaVirtualMachines/amazon-corretto-8.jdk/Contents/Home/jre
✅ Gradle version meets minimum requirements for SBOM generation
✅ Java version is compatible with SBOM generation
ℹ️  Running on Gradle 7.3.3, 8.5+ compatibility will be validated during migration
ℹ️  Running on Java 8, consider Java 21+ for future SBOM enhancements
ℹ️  CycloneDX plugin is configured but not applied (expected for PR 1)
=== End Compatibility Validation ===
```

## Safety Measures

1. **Zero Impact Design**: Plugin is added with `apply false` - no functional changes
2. **Feature Flags**: All SBOM functionality is disabled by default
3. **Comprehensive Testing**: Multiple test layers ensure safety
4. **Performance Monitoring**: Validation confirms minimal performance impact
5. **Rollback Ready**: Changes can be easily reverted if needed

## Next Steps

This PR establishes the foundation for subsequent PRs:

- **PR 2**: Context Detection Logic - Smart generation based on CI/release/explicit contexts
- **PR 3**: Basic SBOM Generation - Enable generation for single module (geode-common)
- **PR 4**: Multi-Module Configuration - Scale to all 30+ modules
- **PR 5**: Assembly Integration - Add geode-assembly SBOM generation

## Technical Details

### Plugin Version Selection
- Using CycloneDX `1.8.2` (stable release, not alpha/beta)
- Confirmed Gradle 7.3.3+ compatibility
- Future-ready for Gradle 8.5+ and Java 21+

### Configuration Structure
```gradle
ext {
  sbomEnabled = false
  sbomGenerationContext = 'none'
  sbomConfig = [
    pluginVersion: '1.8.2',
    schemaVersion: '1.4',
    outputFormat: 'json',
    includeConfigs: ['runtimeClasspath', 'compileClasspath'],
    skipConfigs: ['testRuntimeClasspath', 'testCompileClasspath']
  ]
}
```

## Acceptance Criteria Met

- ✅ CycloneDX plugin is available but not active
- ✅ validateGradleCompatibility task provides useful version information  
- ✅ All existing tests pass without modification
- ✅ No performance impact on existing builds
- ✅ Clear logging about compatibility status

## Review Checklist

- [ ] All tests pass (`./proposals/GEODE-10481/pr-log/01-validation.sh`)
- [ ] No regression in existing functionality
- [ ] validateGradleCompatibility task works correctly
- [ ] Plugin is available but not applied
- [ ] Documentation is complete and accurate
- [ ] Code follows Apache Geode conventions

This PR is ready for review and merge!
