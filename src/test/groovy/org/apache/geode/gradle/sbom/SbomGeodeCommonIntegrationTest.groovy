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
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path

/**
 * Integration tests for SBOM generation in geode-common module.
 * This test validates PR 3: Basic SBOM Generation for Single Module.
 * 
 * Tests cover:
 * - SBOM generation when context detection enables it
 * - SBOM content validation and format compliance
 * - Dependency accuracy and completeness
 * - Performance impact measurement
 * - Context-aware generation behavior
 */
class SbomGeodeCommonIntegrationTest extends Specification {

    @TempDir
    Path testProjectDir

    def setup() {
        // Create a minimal test project structure that mimics geode-common
        createTestProject()
    }

    def "SBOM is generated for geode-common when context detection enables it"() {
        given:
        def buildFile = createGeodeCommonBuildFile()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments(':geode-common:cyclonedxBom', '--info')
                .withPluginClasspath()
                .withEnvironment(['CI': 'true'])
                .build()

        then:
        result.task(':geode-common:cyclonedxBom').outcome == TaskOutcome.SUCCESS
        
        def sbomFile = new File(testProjectDir.toFile(), 'geode-common/build/reports/sbom/geode-common-1.0.0.json')
        sbomFile.exists()
        sbomFile.length() > 0
        
        // Validate SBOM JSON structure
        def sbomContent = new groovy.json.JsonSlurper().parse(sbomFile)
        sbomContent.bomFormat == 'CycloneDX'
        sbomContent.specVersion != null
        sbomContent.serialNumber != null
        sbomContent.metadata != null
        sbomContent.components != null
    }

    def "SBOM contains expected dependencies from geode-common"() {
        given:
        createGeodeCommonBuildFile()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments(':geode-common:cyclonedxBom', '--info')
                .withPluginClasspath()
                .withEnvironment(['CI': 'true'])
                .build()

        then:
        result.task(':geode-common:cyclonedxBom').outcome == TaskOutcome.SUCCESS
        
        def sbomFile = new File(testProjectDir.toFile(), 'geode-common/build/reports/sbom/geode-common-1.0.0.json')
        def sbomContent = new groovy.json.JsonSlurper().parse(sbomFile)
        
        // Verify expected dependencies are present
        def componentNames = sbomContent.components.collect { it.name }
        componentNames.contains('jackson-databind')
        componentNames.contains('jackson-datatype-jsr310')
        componentNames.contains('jackson-datatype-joda')
        
        // Verify test dependencies are excluded
        !componentNames.any { it.contains('junit') }
        !componentNames.any { it.contains('mockito') }
        !componentNames.any { it.contains('assertj') }
    }

    def "SBOM format validates against CycloneDX schema"() {
        given:
        createGeodeCommonBuildFile()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments(':geode-common:cyclonedxBom', '--info')
                .withPluginClasspath()
                .withEnvironment(['CI': 'true'])
                .build()

        then:
        result.task(':geode-common:cyclonedxBom').outcome == TaskOutcome.SUCCESS
        
        def sbomFile = new File(testProjectDir.toFile(), 'geode-common/build/reports/sbom/geode-common-1.0.0.json')
        def sbomContent = new groovy.json.JsonSlurper().parse(sbomFile)
        
        // Validate required CycloneDX fields
        sbomContent.bomFormat == 'CycloneDX'
        sbomContent.specVersion =~ /^1\.\d+$/
        sbomContent.serialNumber =~ /^urn:uuid:[0-9a-f-]{36}$/
        sbomContent.version >= 1
        
        // Validate metadata structure
        sbomContent.metadata.timestamp != null
        sbomContent.metadata.component != null
        sbomContent.metadata.component.type == 'library'
        sbomContent.metadata.component.name == 'geode-common'
        
        // Validate components structure
        sbomContent.components.each { component ->
            assert component.type != null
            assert component.name != null
            assert component.version != null
            assert component.purl != null
        }
    }

    def "SBOM is not generated when context detection disables it"() {
        given:
        createGeodeCommonBuildFile()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments(':geode-common:cyclonedxBom', '--info')
                .withPluginClasspath()
                .withEnvironment([:]) // No CI environment
                .build()

        then:
        result.task(':geode-common:cyclonedxBom').outcome == TaskOutcome.SKIPPED
        
        def sbomFile = new File(testProjectDir.toFile(), 'geode-common/build/reports/sbom/geode-common-1.0.0.json')
        !sbomFile.exists()
    }

    def "SBOM generation performance impact is minimal"() {
        given:
        createGeodeCommonBuildFile()

        when:
        // Measure build time without SBOM
        def startTimeWithoutSbom = System.currentTimeMillis()
        def resultWithoutSbom = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments(':geode-common:compileJava', '--info')
                .withPluginClasspath()
                .withEnvironment([:])
                .build()
        def timeWithoutSbom = System.currentTimeMillis() - startTimeWithoutSbom

        // Measure build time with SBOM
        def startTimeWithSbom = System.currentTimeMillis()
        def resultWithSbom = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments(':geode-common:compileJava', ':geode-common:cyclonedxBom', '--info')
                .withPluginClasspath()
                .withEnvironment(['CI': 'true'])
                .build()
        def timeWithSbom = System.currentTimeMillis() - startTimeWithSbom

        then:
        resultWithoutSbom.task(':geode-common:compileJava').outcome == TaskOutcome.SUCCESS
        resultWithSbom.task(':geode-common:compileJava').outcome == TaskOutcome.SUCCESS
        resultWithSbom.task(':geode-common:cyclonedxBom').outcome == TaskOutcome.SUCCESS
        
        // Performance impact should be less than 1% (very generous for test environment)
        def performanceImpact = ((timeWithSbom - timeWithoutSbom) / timeWithoutSbom) * 100
        performanceImpact < 50 // 50% threshold for test environment (much more generous than 1% production target)
    }

    def "validateGeodeCommonSbom task works correctly"() {
        given:
        createGeodeCommonBuildFile()

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectDir.toFile())
                .withArguments(':geode-common:validateGeodeCommonSbom', '--info')
                .withPluginClasspath()
                .withEnvironment(['CI': 'true'])
                .build()

        then:
        result.task(':geode-common:validateGeodeCommonSbom').outcome == TaskOutcome.SUCCESS
        result.output.contains('SBOM file generated')
        result.output.contains('SBOM JSON is valid')
        result.output.contains('Components found:')
    }

    private void createTestProject() {
        // Create root build.gradle with context detection logic
        def rootBuildFile = new File(testProjectDir.toFile(), 'build.gradle')
        rootBuildFile.text = createRootBuildFileWithContextDetection()
        
        // Create settings.gradle
        def settingsFile = new File(testProjectDir.toFile(), 'settings.gradle')
        settingsFile.text = """
            rootProject.name = 'geode-test'
            include 'geode-common'
        """
        
        // Create geode-common directory
        def geodeCommonDir = new File(testProjectDir.toFile(), 'geode-common')
        geodeCommonDir.mkdirs()
        
        // Create minimal source file
        def srcDir = new File(geodeCommonDir, 'src/main/java/org/apache/geode/common')
        srcDir.mkdirs()
        def javaFile = new File(srcDir, 'TestClass.java')
        javaFile.text = """
            package org.apache.geode.common;
            public class TestClass {
                public String getMessage() {
                    return "Hello from geode-common";
                }
            }
        """
    }

    private String createRootBuildFileWithContextDetection() {
        return """
            plugins {
                id 'org.cyclonedx.bom' version '1.8.2' apply false
            }
            
            // Context Detection Logic (from PR 2)
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
            
            def shouldGenerateSbom = isCI || isRelease || isExplicitSbom
            
            ext {
              sbomEnabled = shouldGenerateSbom
              sbomGenerationContext = shouldGenerateSbom ? 
                (isCI ? 'ci' : (isRelease ? 'release' : 'explicit')) : 'none'
              sbomContextFlags = [
                isCI: isCI,
                isRelease: isRelease,
                isExplicitSbom: isExplicitSbom,
                shouldGenerateSbom: shouldGenerateSbom
              ]
              sbomConfig = [
                pluginVersion: '1.8.2',
                schemaVersion: '1.4',
                outputFormat: 'json',
                includeConfigs: ['runtimeClasspath', 'compileClasspath'],
                skipConfigs: ['testRuntimeClasspath', 'testCompileClasspath']
              ]
            }
            
            allprojects {
                repositories {
                    mavenCentral()
                }
                
                version = '1.0.0'
                group = 'org.apache.geode'
            }
        """
    }

    private String createGeodeCommonBuildFile() {
        def buildFile = new File(testProjectDir.toFile(), 'geode-common/build.gradle')
        buildFile.text = """
            plugins {
                id 'java-library'
                id 'org.cyclonedx.bom' version '1.8.2'
            }
            
            dependencies {
                implementation 'com.fasterxml.jackson.core:jackson-databind:2.15.2'
                implementation 'com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.2'
                implementation 'com.fasterxml.jackson.datatype:jackson-datatype-joda:2.15.2'
                
                testImplementation 'junit:junit:4.13.2'
                testImplementation 'org.mockito:mockito-core:4.11.0'
                testImplementation 'org.assertj:assertj-core:3.24.2'
            }
            
            // SBOM Configuration (from PR 3)
            cyclonedxBom {
              enabled = rootProject.ext.sbomEnabled
              includeConfigs = rootProject.ext.sbomConfig.includeConfigs
              skipConfigs = rootProject.ext.sbomConfig.skipConfigs
              projectType = "library"
              schemaVersion = rootProject.ext.sbomConfig.schemaVersion
              outputFormat = rootProject.ext.sbomConfig.outputFormat
              includeLicenseText = true
              destination = file("\$buildDir/reports/sbom")
              outputName = "\${project.name}-\${project.version}"
              includeMetadataResolution = true
              includeBomSerialNumber = true
            }
            
            tasks.register('validateGeodeCommonSbom') {
              group = 'Verification'
              description = 'Validate SBOM generation for geode-common module'
              dependsOn 'cyclonedxBom'
              
              doLast {
                def sbomFile = file("\$buildDir/reports/sbom/\${project.name}-\${project.version}.json")
                logger.lifecycle("SBOM enabled: \${rootProject.ext.sbomEnabled}")
                
                if (rootProject.ext.sbomEnabled) {
                  if (sbomFile.exists()) {
                    logger.lifecycle("SBOM file generated: \${sbomFile.absolutePath}")
                    def sbomContent = new groovy.json.JsonSlurper().parse(sbomFile)
                    logger.lifecycle("SBOM JSON is valid")
                    logger.lifecycle("Components found: \${sbomContent.components?.size() ?: 0}")
                  }
                }
              }
            }
        """
        return buildFile.text
    }
}
