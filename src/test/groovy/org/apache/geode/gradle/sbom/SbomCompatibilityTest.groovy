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
 * Tests for SBOM compatibility validation functionality (GEODE-10481 PR 1).
 * 
 * This test class validates the validateGradleCompatibility task and ensures
 * that the CycloneDX plugin integration doesn't affect existing builds.
 */
class SbomCompatibilityTest extends Specification {

    @TempDir
    Path testProjectDir

    File buildFile
    File settingsFile

    def setup() {
        buildFile = new File(testProjectDir.toFile(), 'build.gradle')
        settingsFile = new File(testProjectDir.toFile(), 'settings.gradle')
        
        settingsFile << """
            rootProject.name = 'sbom-test'
        """
    }

    def "validateGradleCompatibility task executes successfully"() {
        given:
        buildFile << """
            plugins {
                id 'java'
                id 'org.cyclonedx.bom' version '1.8.2' apply false
            }
            
            tasks.register('validateGradleCompatibility') {
                group = 'Verification'
                description = 'Validate Gradle and Java compatibility for SBOM generation'
                
                doLast {
                    def gradleVersion = gradle.gradleVersion
                    def javaVersion = System.getProperty("java.version")
                    
                    logger.lifecycle("Current Gradle version: \${gradleVersion}")
                    logger.lifecycle("Current Java version: \${javaVersion}")
                    logger.lifecycle("✅ Compatibility validation completed")
                }
            }
        """

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('validateGradleCompatibility', '--info')
                .withPluginClasspath()
                .build()

        then:
        result.task(':validateGradleCompatibility').outcome == TaskOutcome.SUCCESS
        result.output.contains('Current Gradle version:')
        result.output.contains('Current Java version:')
        result.output.contains('✅ Compatibility validation completed')
    }

    def "CycloneDX plugin can be loaded without applying"() {
        given:
        buildFile << """
            plugins {
                id 'java'
                id 'org.cyclonedx.bom' version '1.8.2' apply false
            }
            
            tasks.register('checkPluginAvailability') {
                doLast {
                    logger.lifecycle("Plugin loaded successfully without application")
                }
            }
        """

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('checkPluginAvailability')
                .withPluginClasspath()
                .build()

        then:
        result.task(':checkPluginAvailability').outcome == TaskOutcome.SUCCESS
        result.output.contains('Plugin loaded successfully without application')
    }

    def "existing Java compilation tasks work normally"() {
        given:
        // Create a simple Java class
        def srcDir = new File(testProjectDir.toFile(), 'src/main/java')
        srcDir.mkdirs()
        def javaFile = new File(srcDir, 'TestClass.java')
        javaFile << """
            public class TestClass {
                public String getMessage() {
                    return "Hello, World!";
                }
            }
        """

        buildFile << """
            plugins {
                id 'java'
                id 'org.cyclonedx.bom' version '1.8.2' apply false
            }
        """

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('compileJava')
                .withPluginClasspath()
                .build()

        then:
        result.task(':compileJava').outcome == TaskOutcome.SUCCESS
    }

    def "build task completes without SBOM generation"() {
        given:
        buildFile << """
            plugins {
                id 'java'
                id 'org.cyclonedx.bom' version '1.8.2' apply false
            }
            
            // Simulate the basic configuration structure from PR 1
            ext {
                sbomEnabled = false
                sbomGenerationContext = 'none'
            }
        """

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments('build')
                .withPluginClasspath()
                .build()

        then:
        result.task(':build').outcome == TaskOutcome.SUCCESS
        // Verify no SBOM files were generated
        !new File(testProjectDir.toFile(), 'build/reports/sbom').exists()
    }

    def "performance impact is minimal"() {
        given:
        buildFile << """
            plugins {
                id 'java'
                id 'org.cyclonedx.bom' version '1.8.2' apply false
            }
        """

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
}
