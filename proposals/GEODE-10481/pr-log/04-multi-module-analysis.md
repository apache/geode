# GEODE-10481 PR 4: Multi-Module SBOM Analysis

## Module Classification for SBOM Generation

### Modules to INCLUDE in SBOM Generation (30+ modules)

#### Core Geode Modules
- `:geode-common` ✅ (already implemented in PR 3)
- `:geode-unsafe`
- `:geode-junit`
- `:geode-dunit`
- `:geode-logging`
- `:geode-jmh`
- `:geode-membership`
- `:geode-serialization`
- `:geode-tcp-server`
- `:geode-core`
- `:geode-log4j`
- `:geode-web`
- `:geode-web-api`
- `:geode-web-management`
- `:geode-management`
- `:geode-gfsh`
- `:geode-pulse`
- `:geode-rebalancer`
- `:geode-lucene`
- `:geode-old-client-support`
- `:geode-wan`
- `:geode-cq`
- `:geode-memcached`
- `:geode-connectors`
- `:geode-http-service`
- `:geode-concurrency-test`
- `:geode-server-all`

#### Extension Modules
- `:extensions:geode-modules`
- `:extensions:geode-modules-session`
- `:extensions:geode-modules-session-internal`
- `:extensions:geode-modules-tomcat7`
- `:extensions:geode-modules-tomcat8`
- `:extensions:geode-modules-tomcat9`

#### Deployment Modules
- `:geode-deployment`
- `:geode-deployment:geode-deployment-legacy`

#### BOM Modules
- `:boms:geode-client-bom`
- `:boms:geode-all-bom`

### Modules to EXCLUDE from SBOM Generation

#### Assembly Modules (as per requirements)
- `:geode-assembly` ❌ (explicitly excluded per PR 4 requirements)

#### Test-Only Modules
- `:geode-assembly:geode-assembly-test` ❌ (test module)
- `:geode-pulse:geode-pulse-test` ❌ (test module)
- `:geode-lucene:geode-lucene-test` ❌ (test module)
- `:extensions:geode-modules-test` ❌ (test module)
- `:extensions:session-testing-war` ❌ (test module)

#### Assembly-Related Modules
- `:extensions:geode-modules-assembly` ❌ (assembly module)

#### Old Version Modules (compatibility/testing only)
- `:geode-old-versions` and all sub-versions ❌ (compatibility testing only)

#### Static Analysis Modules
- `:static-analysis` ❌ (build tooling)
- `:static-analysis:pmd-rules` ❌ (build tooling)

#### Parent/Container Modules
- `:boms` ❌ (parent module)
- `:extensions` ❌ (parent module)

## Summary

**Total Eligible Modules: 35**

**Module Categories:**
- Core Geode Modules: 27
- Extension Modules: 6  
- Deployment Modules: 2
- BOM Modules: 2

**Excluded Categories:**
- Assembly modules: 2
- Test-only modules: 5
- Old version modules: 20+
- Static analysis modules: 2
- Parent modules: 2

## Implementation Strategy

The multi-module configuration will use:
```gradle
configure(subprojects.findAll { 
  it.name != 'geode-assembly' && 
  !it.name.endsWith('-test') &&
  !it.path.startsWith(':geode-old-versions') &&
  !it.path.startsWith(':static-analysis') &&
  it.name != 'geode-modules-assembly' &&
  it.name != 'session-testing-war' &&
  !['boms', 'extensions'].contains(it.name)
}) {
  // SBOM configuration
}
```

This ensures we apply SBOM configuration to all production modules while excluding test, assembly, and tooling modules.
