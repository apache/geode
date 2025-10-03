# GEODE-10479: Java 17 Deprecation Warning Removal - TODO List

## Project Status: Ready for Implementation ✅

**Last Updated**: 2025-12-11
**Current Phase**: Ready to Start Phase 0 - Critical Java 21 Blockers
**Next Phase**: Phase 0 - Critical Java 21 Blockers (URGENT)

## ⚠️ CRITICAL UPDATE
**Java 21 Blocking Issues Discovered**: Analysis revealed 2 critical warnings in geode-logging that use APIs **removed in Java 21**. These must be fixed before any Java 21 migration attempts.

**Warning Baseline**: 41 total warnings across 6 modules
- **CRITICAL**: 2 warnings (Java 21 blockers)
- **HIGH**: 3 warnings (Spring Framework 7.0 blockers)
- **MEDIUM**: 36 warnings (technical debt)

---

## Phase 0: Critical Java 21 Blockers (Days 1-2) 🚨

### 0.1 Fix SecurityManager API Removal (GEODE-10531)
- [x] **Status**: **URGENT - JAVA 21 BLOCKER**
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 2-4 hours (3 story points)
- [ ] **Dependencies**: None - **HIGHEST PRIORITY**
- [ ] **Issue**: SecurityManager APIs **removed in Java 21**
- [ ] **File**: `geode-logging/src/main/java/org/apache/geode/logging/internal/OSProcess.java:200`
- [ ] **APIs**: `java.lang.SecurityManager`, `System.getSecurityManager()`
- [ ] **Deliverables**:
  - [ ] Remove all SecurityManager references from OSProcess.java
  - [ ] Replace with modern security patterns (context-specific permissions, Java Security API, or application-level security)
  - [ ] Comprehensive testing to ensure security functionality preserved
  - [ ] Documentation of replacement security approach
- [ ] **Acceptance Criteria**:
  - [ ] Code compiles successfully in Java 21
  - [ ] All existing tests pass
  - [ ] Security functionality maintained or improved
  - [ ] Zero removal warnings in geode-logging module
  - [ ] No new security vulnerabilities introduced

---

## Phase 1: High-Priority Removal Warnings (Week 2)

### 1.1 Fix Spring Framework Removal Warnings (GEODE-10532)
- [ ] **Status**: **HIGH PRIORITY - SPRING 7.0 BLOCKER**
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 1-2 hours
- [ ] **Dependencies**: Phase 0 complete
- [ ] **Issue**: `ClientHttpResponse.getRawStatusCode()` removal in Spring Framework 7.0
- [ ] **File**: `geode-gfsh/src/main/java/org/apache/geode/management/internal/web/http/support/HttpRequester.java`
- [ ] **Lines**: 113, 115, 117 (3 occurrences)
- [ ] **Migration**: `response.getRawStatusCode()` → `response.getStatusCode().value()`
- [ ] **Deliverables**:
  - [ ] Replace all 3 occurrences of getRawStatusCode()
  - [ ] GFSH HTTP status code testing (success, error, edge cases)
  - [ ] Validation of error handling paths
- [ ] **Acceptance Criteria**:
  - [ ] All 3 occurrences replaced with getStatusCode().value()
  - [ ] Code compiles without [removal] warnings
  - [ ] All existing GFSH tests pass
  - [ ] HTTP status code handling verified
  - [ ] Exception handling remains consistent

---

## Phase 2: Assessment and Categorization (Week 1)

### 2.1 Create Custom Gradle Warning Analysis Task
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 2-3 days
- [ ] **Dependencies**: None
- [ ] **Deliverables**:
  - [ ] Custom Gradle task implementation
  - [ ] JSON/CSV output format
  - [ ] Automated categorization logic
  - [ ] Dashboard-style reporting
  - [ ] Unit tests for categorization logic
- [ ] **Acceptance Criteria**:
  - [ ] Task runs without breaking existing build
  - [ ] Captures all warning types (deprecation/removal)
  - [ ] Categorizes warnings correctly (Critical/High/Medium/Low)
  - [ ] Generates structured output (JSON/CSV)
  - [ ] Creates HTML dashboard
  - [ ] Includes comprehensive error handling

### 2.2 Integrate with SonarQube and CI Reporting
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 2-3 days
- [ ] **Dependencies**: Task 1.1 complete
- [ ] **Deliverables**:
  - [ ] SonarQube custom rules
  - [ ] CI integration components
  - [ ] Enhanced categorization system
  - [ ] Reporting enhancements
- [ ] **Acceptance Criteria**:
  - [ ] SonarQube integration functional
  - [ ] CI reports generated correctly
  - [ ] Automated categorization working
  - [ ] Trend tracking operational
  - [ ] Integration tests passing

### 2.3 Generate Baseline Warning Report
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 1-2 days
- [ ] **Dependencies**: Tasks 1.1, 1.2 complete
- [ ] **Deliverables**:
  - [ ] Comprehensive baseline report
  - [ ] Module-by-module breakdown
  - [ ] Warning impact assessment
  - [ ] Executive summary
  - [ ] Validation report
- [ ] **Acceptance Criteria**:
  - [ ] All modules analyzed
  - [ ] Warning counts validated
  - [ ] Categories assigned correctly
  - [ ] Dependencies mapped
  - [ ] Report formats complete (JSON/CSV/HTML)

### 2.4 Implement Module Prioritization Strategy
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 1-2 days
- [ ] **Dependencies**: Task 1.3 complete
- [ ] **Deliverables**:
  - [ ] Prioritization algorithm
  - [ ] Ordered module list
  - [ ] Processing timeline
  - [ ] Progress tracking tools
- [ ] **Acceptance Criteria**:
  - [ ] **UPDATED**: Critical blockers first, then "fewest warnings first"
  - [ ] Dependencies respected in ordering
  - [ ] Processing estimates created
  - [ ] **NEW**: Specific module order: geode-serialization (1) → geode-deployment-legacy (1) → geode-web-api (1) → geode-management (10) → geode-gfsh (23 remaining)
  - [ ] Simulation validation complete

---

## Phase 3: Incremental Warning Re-enablement (Weeks 2-4)

### 3.1 Enable Global Removal Warnings
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 1-2 days
- [ ] **Dependencies**: Phase 1 and Phase 2 complete
- [ ] **Deliverables**:
  - [ ] Updated build configuration
  - [ ] Removal warning baseline
  - [ ] Temporary suppression strategy
  - [ ] Build validation results
- [ ] **Acceptance Criteria**:
  - [ ] Build completes with removal warnings enabled
  - [ ] All tests pass
  - [ ] No functional regressions
  - [ ] Suppressions documented and tracked

### 3.2 Fix Remaining API Removal Issues
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 3-5 days
- [ ] **Dependencies**: Task 3.1 complete
- [ ] **Deliverables**:
  - [ ] API replacement implementations
  - [ ] Comprehensive test suite
  - [ ] Performance benchmarks
  - [ ] Migration documentation
- [ ] **Acceptance Criteria**:
  - [ ] All critical removal warnings resolved
  - [ ] Backward compatibility maintained
  - [ ] Performance maintained or improved
  - [ ] Security properties preserved

### 3.3 Implement Graduated Deprecation Enablement
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 2-3 days
- [ ] **Dependencies**: Task 3.2 complete
- [ ] **Deliverables**:
  - [ ] Enhanced build system
  - [ ] Interface contract framework
  - [ ] Suppression management system
  - [ ] Module enablement workflow
- [ ] **Acceptance Criteria**:
  - [ ] Per-module deprecation control working
  - [ ] Interface contracts enforced
  - [ ] Temporary suppressions managed
  - [ ] Progress tracking operational

### 3.4 Process Modules in Priority Order
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 5-10 days (varies by module)
- [ ] **Dependencies**: Task 3.3 complete, module priority list
- [ ] **Deliverables**:
  - [ ] **3.4.1**: geode-serialization (1 warning - Reflection API)
  - [ ] **3.4.2**: geode-deployment-legacy (1 warning - Proxy API)
  - [ ] **3.4.3**: geode-web-api (1 warning - Spring Framework)
  - [ ] **3.4.4**: geode-management (10 warnings - HttpClient + Commons Lang)
  - [ ] **3.4.5**: geode-gfsh (23 remaining warnings - Commons Lang:7, Reflection:4, Micrometer:3, Query API:1, Unchecked:5, Spring:3 handled separately)
  - [ ] Updated documentation per module
  - [ ] Performance validation per module
  - [ ] Progress reports
- [ ] **Acceptance Criteria**:
  - [ ] Each module achieves zero warnings
  - [ ] All tests pass per module
  - [ ] Interface contracts maintained
  - [ ] Documentation updated

---

## Phase 4: Third-Party Library API Modernization (Weeks 3-5)

### 4.1 Apache HttpClient 5 SSL API Migration (geode-management)
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 6-8 hours
- [ ] **Dependencies**: Phase 3 in progress
- [ ] **Issue**: 9 warnings for deprecated SSL APIs
- [ ] **APIs**: `SSLConnectionSocketFactory` → `SSLConnectionSocketFactoryBuilder`, `setSSLSocketFactory()` → `setTlsSocketStrategy()`
- [ ] **Deliverables**:
  - [ ] Updated SSL configuration in RestTemplateClusterManagementServiceTransport.java
  - [ ] SSL/TLS connection testing
  - [ ] Security validation
- [ ] **Acceptance Criteria**:
  - [ ] Modern HttpClient 5 SSL APIs used
  - [ ] SSL/TLS connections verified
  - [ ] Security properties maintained

### 4.2 Apache Commons Lang Migration (Multiple Modules)
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 4-6 hours
- [ ] **Dependencies**: Phase 3 in progress
- [ ] **Issue**: 8 warnings across geode-gfsh (7) and geode-management (1)
- [ ] **Files**: ConnectCommand.java, QueryCommand.java, CreateIndexCommand.java, FixedPartitionAttributesInfo.java, RegionAttributesInfo.java, PartitionAttributesInfo.java, Index.java
- [ ] **Patterns**:
  - StringUtils.startsWith() → String.startsWith()
  - StringUtils.containsIgnoreCase() → str.toLowerCase().contains()
  - StringUtils.equals() → Objects.equals()
  - StringUtils.removeStart() → str.startsWith(prefix) ? str.substring(prefix.length()) : str
- [ ] **Deliverables**:
  - [ ] Replace all StringUtils deprecated methods
  - [ ] String operation testing (null handling, edge cases)
- [ ] **Acceptance Criteria**:
  - [ ] All StringUtils deprecated methods replaced
  - [ ] String operations tested thoroughly
  - [ ] Null handling preserved

### 4.3 Java Reflection API Modernization (Multiple Modules)
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 6-8 hours
- [ ] **Dependencies**: Phase 3 in progress
- [ ] **Issue**: 6 warnings across geode-gfsh (4), geode-serialization (1), geode-deployment-legacy (1)
- [ ] **Files**:
  - geode-gfsh: RegionFunctionArgs.java, CreateAsyncEventQueueFunction.java, UserFunctionExecution.java
  - geode-serialization: DSFIDSerializerImpl.java
  - geode-deployment-legacy: LegacyClasspathServiceImpl.java
- [ ] **Patterns**:
  - `Class.newInstance()` → `getDeclaredConstructor().newInstance()` (4 warnings)
  - `isAccessible()` → `canAccess(targetObject)` (1 warning)
  - `Proxy.getProxyClass()` → `Proxy.newProxyInstance()` (1 warning)
- [ ] **Deliverables**:
  - [ ] Updated reflection API usage
  - [ ] Reflection operation testing
  - [ ] Exception handling updates (InvocationTargetException, NoSuchMethodException)
- [ ] **Acceptance Criteria**:
  - [ ] Modern reflection APIs used
  - [ ] Exception handling properly updated
  - [ ] Reflection operations tested

### 4.4 Establish RFC Process for API Changes
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 1-2 days
- [ ] **Dependencies**: Phase 3 in progress
- [ ] **Deliverables**:
  - [ ] RFC templates and guidelines
  - [ ] Community proposal process
  - [ ] Review workflow
- [ ] **Acceptance Criteria**:
  - [ ] RFC process documented
  - [ ] Templates created
  - [ ] Community engagement plan ready

### 4.5 Micrometer StringUtils Migration (geode-gfsh)
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 1-2 hours
- [ ] **Dependencies**: Phase 3 in progress
- [ ] **Issue**: 3 warnings in StopServerCommand.java:17
- [ ] **API**: `io.micrometer.core.instrument.util.StringUtils` (internal utility, not for public use)
- [ ] **Replacement**: `org.springframework.util.StringUtils` or Java standard library methods
- [ ] **Deliverables**:
  - [ ] Replace Micrometer StringUtils usage
  - [ ] String operation testing
- [ ] **Acceptance Criteria**:
  - [ ] Micrometer StringUtils replaced
  - [ ] String operations tested
  - [ ] No dependency on internal utility classes

### 4.6 Geode Query API Migration (geode-gfsh)
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 2-3 hours
- [ ] **Dependencies**: Phase 3 in progress
- [ ] **Issue**: 1 warning in GfshParser.java:708
- [ ] **API**: `org.apache.geode.cache.query.IndexType` (deprecated)
- [ ] **Action**: Consult Geode API documentation for replacement
- [ ] **Deliverables**:
  - [ ] Replace deprecated IndexType usage
  - [ ] Query functionality testing
- [ ] **Acceptance Criteria**:
  - [ ] Modern Geode Query API used
  - [ ] Query operations tested
  - [ ] Index creation verified

### 4.7 Spring Framework PathPatternParser (geode-web-api)
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 2-3 hours
- [ ] **Dependencies**: Phase 3 in progress
- [ ] **Issue**: 1 warning in SwaggerConfig.java:62
- [ ] **API**: `PathPatternParser.setMatchOptionalTrailingSeparator()` (deprecated in Spring 6.x)
- [ ] **Action**: Update path pattern matching configuration per Spring 6.x migration guide
- [ ] **Deliverables**:
  - [ ] Update Swagger path pattern configuration
  - [ ] Swagger/OpenAPI URL testing
- [ ] **Acceptance Criteria**:
  - [ ] Modern Spring path pattern APIs used
  - [ ] Swagger/OpenAPI path matching verified
  - [ ] Trailing separator handling tested

### 4.8 Type Safety - Unchecked Stream Operations (geode-gfsh)
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 2-3 hours
- [ ] **Dependencies**: Phase 3 in progress
- [ ] **Issue**: 5 unchecked warnings in ShowMissingDiskStoreCommand.java:87-90
- [ ] **Pattern**: Add proper generic type parameters to Stream operations
- [ ] **Deliverables**:
  - [ ] Add generic types to Stream operations
  - [ ] Type safety validation
- [ ] **Acceptance Criteria**:
  - [ ] All unchecked warnings resolved
  - [ ] Type safety improved
  - [ ] Stream operations tested

---

## Phase 5: Full Warning Compliance (Week 6)

### 5.1 Remove All Warning Suppressions
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 1 day
- [ ] **Dependencies**: Phase 4 complete
- [ ] **Deliverables**:
  - [ ] Final build configuration
  - [ ] All suppressions removed
  - [ ] Build validation
- [ ] **Acceptance Criteria**:
  - [ ] All warning suppressions removed
  - [ ] Build completes with zero warnings
  - [ ] All tests pass

### 5.2 Establish CI/CD Warning Gates
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 1-2 days
- [ ] **Dependencies**: Task 5.1 complete
- [ ] **Deliverables**:
  - [ ] CI/CD configuration updates
  - [ ] Warning gate implementation
  - [ ] Checkstyle rules
- [ ] **Acceptance Criteria**:
  - [ ] Build fails on new warnings
  - [ ] Checkstyle prevents deprecated API introduction
  - [ ] CI/CD gates operational

### 5.3 Comprehensive Testing and Validation
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 2-3 days
- [ ] **Dependencies**: Task 5.2 complete
- [ ] **Deliverables**:
  - [ ] Full test suite validation
  - [ ] Performance benchmarks
  - [ ] Compatibility testing
  - [ ] Final validation report
- [ ] **Acceptance Criteria**:
  - [ ] All tests pass
  - [ ] Performance maintained
  - [ ] Compatibility confirmed
  - [ ] Zero warnings across all modules

### 5.4 Documentation and Communication
- [ ] **Status**: Not Started
- [ ] **Assignee**: TBD
- [ ] **Estimated Effort**: 1-2 days
- [ ] **Dependencies**: Task 5.3 complete
- [ ] **Deliverables**:
  - [ ] Updated documentation
  - [ ] Migration guides
  - [ ] Community communication
  - [ ] Wiki updates
- [ ] **Acceptance Criteria**:
  - [ ] Documentation reflects modern patterns
  - [ ] Migration guides complete
  - [ ] Community informed of completion
  - [ ] Wiki status updated

---

## Project Completion Checklist

- [ ] **CRITICAL**: Java 21 blockers resolved (SecurityManager APIs in geode-logging)
- [ ] **HIGH PRIORITY**: Spring Framework 7.0 blockers resolved (getRawStatusCode in geode-gfsh)
- [ ] **Zero Warnings**: All 41 warnings across 6 modules resolved
- [ ] **No Regressions**: All existing tests pass
- [ ] **Performance Maintained**: No degradation in build/test/runtime performance
- [ ] **Library Migrations**: Apache HttpClient 5, Commons Lang, Java Reflection APIs updated
- [ ] **CI/CD Gates**: Warning gates prevent future regressions
- [ ] **Documentation**: Modern patterns documented and communicated

---

## Risk Tracking

### Current Risks
- [ ] **CRITICAL RISK**: Java 21 migration blocked by SecurityManager APIs
  - **Mitigation**: Phase 0 prioritizes these critical blockers
  - **Status**: **URGENT - MUST BE RESOLVED FIRST**

- [ ] **HIGH RISK**: Spring Framework 7.0 compatibility issues
  - **Mitigation**: Phase 1 addresses removal warnings early
  - **Status**: High priority after Phase 0

- [ ] **Risk**: Third-party library API changes causing compatibility issues
  - **Mitigation**: Comprehensive testing for HttpClient 5, Commons Lang migrations
  - **Status**: Monitoring

- [ ] **Risk**: Performance regressions from API changes
  - **Mitigation**: Comprehensive benchmarking at each phase
  - **Status**: Monitoring

### Resolved Risks
- None yet

---

## Notes and Decisions

### Key Decisions Made
1. **UPDATED**: 5-phase approach with critical Java 21 blockers first
2. **UPDATED**: "Critical blockers first" prioritization strategy
3. **NEW**: Specific module processing order based on subtask analysis
4. **NEW**: Third-party library migration patterns identified
5. **Interface Contracts**: Established to manage module dependencies
6. **Community RFC Process**: Required for major API changes

### Open Questions
- None currently

### Lessons Learned
- Will be updated as implementation progresses
