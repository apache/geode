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

/**
 * Integration tests for multi-module SBOM generation (GEODE-10481 PR 4)
 * Tests the coordinated SBOM generation across all eligible Geode modules
 */
class SbomMultiModuleIntegrationTest extends Specification {

    @TempDir
    File testProjectDir

    File buildFile
    File settingsFile
    File gradlePropertiesFile

    def setup() {
        buildFile = new File(testProjectDir, 'build.gradle')
        settingsFile = new File(testProjectDir, 'settings.gradle')
        gradlePropertiesFile = new File(testProjectDir, 'gradle.properties')
        
        // Create test project structure
        setupTestProject()
    }

    def "generateSbom task coordinates all module SBOM generation"() {
        given: "A multi-module project with SBOM configuration"
        
        when: "Running generateSbom task"
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '--info', '--stacktrace')
            .withPluginClasspath()
            .build()

        then: "Task completes successfully"
        result.task(':generateSbom').outcome == TaskOutcome.SUCCESS
        
        and: "All eligible modules generate SBOMs"
        def expectedModules = ['test-core', 'test-common', 'test-web']
        expectedModules.each { moduleName ->
            def sbomFile = new File(testProjectDir, "${moduleName}/build/reports/sbom/${moduleName}-1.0.0.json")
            assert sbomFile.exists() : "SBOM file should exist for module ${moduleName}"
            assert sbomFile.length() > 0 : "SBOM file should not be empty for module ${moduleName}"
        }
        
        and: "Aggregated SBOM directory is created"
        def aggregatedDir = new File(testProjectDir, 'build/sbom-artifacts')
        assert aggregatedDir.exists() : "Aggregated SBOM directory should exist"
        assert aggregatedDir.listFiles().length == expectedModules.size() : "Should contain SBOMs for all modules"
    }

    def "excluded modules do not generate SBOMs"() {
        given: "A multi-module project with excluded modules"
        
        when: "Running generateSbom task"
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '--info')
            .withPluginClasspath()
            .build()

        then: "Excluded modules do not generate SBOMs"
        def excludedModules = ['test-assembly', 'test-module-test']
        excludedModules.each { moduleName ->
            def sbomFile = new File(testProjectDir, "${moduleName}/build/reports/sbom/${moduleName}-1.0.0.json")
            assert !sbomFile.exists() : "SBOM file should not exist for excluded module ${moduleName}"
        }
    }

    def "SBOM generation respects context detection"() {
        given: "A project with context detection disabled"
        
        when: "Running generateSbom without explicit SBOM context"
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('build', '--info')
            .withPluginClasspath()
            .build()

        then: "No SBOMs are generated when context detection disables generation"
        def testModules = ['test-core', 'test-common', 'test-web']
        testModules.each { moduleName ->
            def sbomFile = new File(testProjectDir, "${moduleName}/build/reports/sbom/${moduleName}-1.0.0.json")
            assert !sbomFile.exists() : "SBOM file should not exist when generation is disabled for ${moduleName}"
        }
    }

    def "generated SBOMs contain valid CycloneDX format"() {
        given: "A multi-module project"
        
        when: "Running generateSbom task"
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '--info')
            .withPluginClasspath()
            .build()

        then: "All generated SBOMs are valid JSON with CycloneDX format"
        def expectedModules = ['test-core', 'test-common', 'test-web']
        expectedModules.each { moduleName ->
            def sbomFile = new File(testProjectDir, "${moduleName}/build/reports/sbom/${moduleName}-1.0.0.json")
            def sbomContent = new groovy.json.JsonSlurper().parse(sbomFile)
            
            assert sbomContent.bomFormat == 'CycloneDX' : "Should use CycloneDX format for ${moduleName}"
            assert sbomContent.specVersion == '1.4' : "Should use schema version 1.4 for ${moduleName}"
            assert sbomContent.serialNumber != null : "Should have serial number for ${moduleName}"
            assert sbomContent.metadata != null : "Should have metadata for ${moduleName}"
            assert sbomContent.components != null : "Should have components for ${moduleName}"
        }
    }

    def "performance impact is within acceptable limits"() {
        given: "A multi-module project"
        
        when: "Running build without SBOM generation"
        def startTimeWithoutSbom = System.currentTimeMillis()
        def resultWithoutSbom = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('build', '--info')
            .withPluginClasspath()
            .build()
        def timeWithoutSbom = System.currentTimeMillis() - startTimeWithoutSbom
        
        and: "Running build with SBOM generation"
        def startTimeWithSbom = System.currentTimeMillis()
        def resultWithSbom = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '--info')
            .withPluginClasspath()
            .build()
        def timeWithSbom = System.currentTimeMillis() - startTimeWithSbom
        
        then: "Performance impact is within acceptable limits (< 50% increase for test)"
        def performanceImpact = ((timeWithSbom - timeWithoutSbom) / timeWithoutSbom) * 100
        assert performanceImpact < 50 : "Performance impact should be less than 50% (actual: ${performanceImpact}%)"
    }

    private void setupTestProject() {
        // Create settings.gradle with multiple modules
        settingsFile.text = """
            rootProject.name = 'test-geode'
            
            include 'test-common'
            include 'test-core'
            include 'test-web'
            include 'test-assembly'
            include 'test-module-test'
        """
        
        // Create gradle.properties
        gradlePropertiesFile.text = """
            org.gradle.jvmargs=-Xmx2g -XX:MaxMetaspaceSize=512m
            org.gradle.parallel=true
            org.gradle.caching=true
        """
        
        // Create root build.gradle with multi-module SBOM configuration
        buildFile.text = createRootBuildScript()
        
        // Create subproject build files
        createSubprojectBuildFiles()
    }

    private String createRootBuildScript() {
        return """
            plugins {
                id 'org.cyclonedx.bom' version '1.8.2' apply false
            }
            
            // SBOM Context Detection (simplified for testing)
            def isExplicitSbom = gradle.startParameter.taskNames.any { taskName ->
                taskName.toLowerCase().contains("generatesbom") ||
                taskName.toLowerCase().contains("cyclonedxbom")
            }
            def shouldGenerateSbom = isExplicitSbom
            
            ext {
                sbomEnabled = shouldGenerateSbom
                sbomGenerationContext = shouldGenerateSbom ? 'explicit' : 'none'
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
                group = 'org.apache.geode.test'
            }
            
            // Multi-module SBOM configuration
            def sbomConfiguration = {
                apply plugin: 'org.cyclonedx.bom'
                
                afterEvaluate {
                    tasks.named('cyclonedxBom') {
                        enabled = rootProject.ext.sbomEnabled
                        includeConfigs = rootProject.ext.sbomConfig.includeConfigs
                        skipConfigs = rootProject.ext.sbomConfig.skipConfigs
                        projectType = "library"
                        schemaVersion = rootProject.ext.sbomConfig.schemaVersion
                        outputFormat = rootProject.ext.sbomConfig.outputFormat
                        includeLicenseText = true
                        destination = file("\$buildDir/reports/sbom")
                        outputName = "\${project.name}-\${project.version}"
                        includeBomSerialNumber = true
                        includeMetadataResolution = true
                    }
                }
            }
            
            // Apply to eligible subprojects (exclude assembly and test modules)
            configure(subprojects.findAll { subproject ->
                !subproject.name.endsWith('-test') && 
                subproject.name != 'test-assembly'
            }, sbomConfiguration)
            
            // Coordinating generateSbom task
            tasks.register('generateSbom') {
                group = 'Build'
                description = 'Generate SBOM for all eligible modules'
                
                dependsOn subprojects.findAll { subproject ->
                    !subproject.name.endsWith('-test') && 
                    subproject.name != 'test-assembly'
                }.collect { "\${it.path}:cyclonedxBom" }
                
                doLast {
                    def aggregatedDir = file("\${buildDir}/sbom-artifacts")
                    aggregatedDir.mkdirs()
                    
                    subprojects.each { subproject ->
                        def sbomFile = subproject.file("\${subproject.buildDir}/reports/sbom/\${subproject.name}-\${subproject.version}.json")
                        if (sbomFile.exists()) {
                            copy {
                                from sbomFile
                                into aggregatedDir
                            }
                        }
                    }
                }
            }
        """
    }

    private void createSubprojectBuildFiles() {
        // Create eligible modules
        ['test-common', 'test-core', 'test-web'].each { moduleName ->
            def moduleDir = new File(testProjectDir, moduleName)
            moduleDir.mkdirs()
            
            new File(moduleDir, 'build.gradle').text = """
                plugins {
                    id 'java-library'
                }
                
                dependencies {
                    implementation 'org.slf4j:slf4j-api:1.7.36'
                }
            """
        }
        
        // Create excluded modules
        ['test-assembly', 'test-module-test'].each { moduleName ->
            def moduleDir = new File(testProjectDir, moduleName)
            moduleDir.mkdirs()
            
            new File(moduleDir, 'build.gradle').text = """
                plugins {
                    id 'java'
                }
                
                dependencies {
                    implementation 'junit:junit:4.13.2'
                }
            """
        }
    }
}
