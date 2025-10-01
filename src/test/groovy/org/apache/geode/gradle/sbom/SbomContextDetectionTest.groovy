/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.gradle.sbom

import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import spock.lang.Specification

import java.nio.file.Path

/**
 * Tests for SBOM context detection logic (GEODE-10481 PR 2).
 * 
 * This test class validates the context detection logic that determines when
 * SBOM generation should occur based on CI environment, release builds, and
 * explicit SBOM requests.
 */
class SbomContextDetectionTest extends Specification {

    @TempDir
    Path testProjectDir

    File buildFile
    File settingsFile
    File gradlePropertiesFile

    def setup() {
        buildFile = new File(testProjectDir.toFile(), 'build.gradle')
        settingsFile = new File(testProjectDir.toFile(), 'settings.gradle')
        gradlePropertiesFile = new File(testProjectDir.toFile(), 'gradle.properties')
        
        settingsFile << """
            rootProject.name = 'sbom-context-test'
        """
        
        gradlePropertiesFile << """
            # SBOM Configuration
            cyclonedx.skip.generation=false
            cyclonedx.parallel.execution=true
            org.gradle.caching=true
            org.gradle.parallel=true
        """
    }

    def "CI environment detection works correctly"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', '--info')
                .withPluginClasspath()
                .withEnvironment(['CI': 'true'])
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('CI Environment: true')
        result.output.contains('Should Generate SBOM: true')
        result.output.contains('SBOM generation triggered by: CI environment')
    }

    def "non-CI environment detection works correctly"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', '--info')
                .withPluginClasspath()
                .withEnvironment([:])  // No CI environment variable
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('CI Environment: false')
        result.output.contains('Should Generate SBOM: false')
    }

    def "release build detection works correctly"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', 'assemble', '--info')
                .withPluginClasspath()
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('Release Build: true')
        result.output.contains('Should Generate SBOM: true')
        result.output.contains('SBOM generation triggered by: release build')
    }

    def "explicit SBOM request detection works correctly"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', 'generateSbom', '--info')
                .withPluginClasspath()
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('Explicit SBOM Request: true')
        result.output.contains('Should Generate SBOM: true')
        result.output.contains('SBOM generation triggered by: explicit SBOM request')
    }

    def "multiple context triggers work correctly"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', 'assemble', 'generateSbom', '--info')
                .withPluginClasspath()
                .withEnvironment(['CI': 'true'])
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('CI Environment: true')
        result.output.contains('Release Build: true')
        result.output.contains('Explicit SBOM Request: true')
        result.output.contains('Should Generate SBOM: true')
        result.output.contains('SBOM generation triggered by: CI environment, release build, explicit SBOM request')
    }

    def "no context triggers means no SBOM generation"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', 'clean', '--info')
                .withPluginClasspath()
                .withEnvironment([:])  // No CI environment
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('CI Environment: false')
        result.output.contains('Release Build: false')
        result.output.contains('Explicit SBOM Request: false')
        result.output.contains('Should Generate SBOM: false')
    }

    def "distribution task triggers release build detection"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', 'distribution', '--info')
                .withPluginClasspath()
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('Release Build: true')
        result.output.contains('Should Generate SBOM: true')
    }

    def "cyclonedxBom task triggers explicit SBOM detection"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', 'cyclonedxBom', '--info')
                .withPluginClasspath()
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('Explicit SBOM Request: true')
        result.output.contains('Should Generate SBOM: true')
    }

    def "context flags are properly set in ext block"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', 'assemble', '--info')
                .withPluginClasspath()
                .withEnvironment(['CI': 'true'])
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('sbomEnabled: true')
        result.output.contains('sbomGenerationContext: ci')
        result.output.contains('sbomContextFlags.isCI: true')
        result.output.contains('sbomContextFlags.isRelease: true')
        result.output.contains('sbomContextFlags.shouldGenerateSbom: true')
    }

    def "case insensitive task name matching works"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', 'ASSEMBLE', '--info')
                .withPluginClasspath()
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('Release Build: true')
        result.output.contains('Should Generate SBOM: true')
    }

    def "CI environment variable with different values"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', '--info')
                .withPluginClasspath()
                .withEnvironment(['CI': 'false'])
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('CI Environment: false')
        result.output.contains('Should Generate SBOM: false')
    }

    def "context detection with mixed case task names"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateSbomContext', 'generateSBOM', '--info')
                .withPluginClasspath()
                .build()

        then:
        result.task(':validateSbomContext').outcome == TaskOutcome.SUCCESS
        result.output.contains('Explicit SBOM Request: true')
        result.output.contains('Should Generate SBOM: true')
    }

    def "performance impact is minimal with context detection"() {
        given:
        buildFile << createBuildFileWithContextDetection()

        when:
        def startTime = System.currentTimeMillis()
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('tasks')
                .withPluginClasspath()
                .build()
        def endTime = System.currentTimeMillis()
        def duration = endTime - startTime

        then:
        result.task(':tasks').outcome == TaskOutcome.SUCCESS
        // Ensure task listing completes quickly (should be under 10 seconds for simple project)
        duration < 10000
    }

    private String createBuildFileWithContextDetection() {
        return """
            plugins {
                id 'java'
                id 'org.cyclonedx.bom' version '1.8.2' apply false
            }
            
            // Context Detection Logic - determines when SBOM generation should occur
            def isCI = System.getenv("CI") == "true"
            def isRelease = gradle.startParameter.taskNames.any { taskName ->
              taskName.toLowerCase().contains("release") || 
              taskName.toLowerCase().contains("distribution") || 
              taskName.toLowerCase().contains("assemble")
            }
            def isExplicitSbom = gradle.startParameter.taskNames.any { taskName ->
              taskName.toLowerCase().contains("generatesbom") ||
              taskName.toLowerCase().contains("cyclonedxbom")
            }

            // Combined logic: SBOM generation should occur in CI, release builds, or when explicitly requested
            def shouldGenerateSbom = isCI || isRelease || isExplicitSbom

            // Debug logging for context detection
            logger.lifecycle("=== SBOM Context Detection ===")
            logger.lifecycle("CI Environment: \${isCI}")
            logger.lifecycle("Release Build: \${isRelease}")
            logger.lifecycle("Explicit SBOM Request: \${isExplicitSbom}")
            logger.lifecycle("Should Generate SBOM: \${shouldGenerateSbom}")
            if (shouldGenerateSbom) {
              def reasons = []
              if (isCI) reasons.add("CI environment")
              if (isRelease) reasons.add("release build")
              if (isExplicitSbom) reasons.add("explicit SBOM request")
              logger.lifecycle("SBOM generation triggered by: \${reasons.join(', ')}")
            }
            logger.lifecycle("=== End SBOM Context Detection ===")

            // SBOM configuration structure (now context-aware)
            ext {
              // SBOM generation control flags (now context-aware in PR 2)
              sbomEnabled = shouldGenerateSbom
              sbomGenerationContext = shouldGenerateSbom ? 
                (isCI ? 'ci' : (isRelease ? 'release' : 'explicit')) : 'none'

              // Context detection flags for use by other build scripts
              sbomContextFlags = [
                isCI: isCI,
                isRelease: isRelease,
                isExplicitSbom: isExplicitSbom,
                shouldGenerateSbom: shouldGenerateSbom
              ]

              // SBOM configuration that will be used in later PRs
              sbomConfig = [
                pluginVersion: '1.8.2',
                schemaVersion: '1.4',
                outputFormat: 'json',
                includeConfigs: ['runtimeClasspath', 'compileClasspath'],
                skipConfigs: ['testRuntimeClasspath', 'testCompileClasspath']
              ]
            }
            
            tasks.register('validateSbomContext') {
                group = 'Verification'
                description = 'Validate SBOM context detection logic'
                
                doLast {
                    logger.lifecycle("sbomEnabled: \${project.ext.sbomEnabled}")
                    logger.lifecycle("sbomGenerationContext: \${project.ext.sbomGenerationContext}")
                    logger.lifecycle("sbomContextFlags.isCI: \${project.ext.sbomContextFlags.isCI}")
                    logger.lifecycle("sbomContextFlags.isRelease: \${project.ext.sbomContextFlags.isRelease}")
                    logger.lifecycle("sbomContextFlags.isExplicitSbom: \${project.ext.sbomContextFlags.isExplicitSbom}")
                    logger.lifecycle("sbomContextFlags.shouldGenerateSbom: \${project.ext.sbomContextFlags.shouldGenerateSbom}")
                }
            }
        """
    }
}
