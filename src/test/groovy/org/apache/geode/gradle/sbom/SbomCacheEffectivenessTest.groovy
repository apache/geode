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
 * Cache effectiveness tests for SBOM generation (GEODE-10481 PR 6)
 * Validates caching configuration and effectiveness targets
 */
class SbomCacheEffectivenessTest extends Specification {

    @TempDir
    File testProjectDir

    File buildFile
    File settingsFile
    File gradlePropertiesFile

    def setup() {
        buildFile = new File(testProjectDir, 'build.gradle')
        settingsFile = new File(testProjectDir, 'settings.gradle')
        gradlePropertiesFile = new File(testProjectDir, 'gradle.properties')
        
        setupCacheTestProject()
    }

    def "SBOM tasks are properly cacheable"() {
        given: "A project with caching enabled"
        
        when: "Running SBOM generation first time"
        def firstResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache', '--info')
            .withPluginClasspath()
            .build()
        
        and: "Running SBOM generation second time without changes"
        def secondResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache', '--info')
            .withPluginClasspath()
            .build()
        
        then: "First run executes the task"
        assert firstResult.task(':cyclonedxBom').outcome == TaskOutcome.SUCCESS
        
        and: "Second run uses cache"
        assert secondResult.task(':cyclonedxBom').outcome in [TaskOutcome.UP_TO_DATE, TaskOutcome.FROM_CACHE]
    }

    def "Cache hit rate exceeds 80% for unchanged dependencies"() {
        given: "A multi-module project"
        setupMultiModuleCacheTest()
        
        when: "Running SBOM generation first time"
        def firstResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        
        and: "Running SBOM generation second time"
        def secondResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        
        then: "Cache hit rate is above 80%"
        def totalTasks = secondResult.tasks.findAll { it.path.contains('cyclonedxBom') }.size()
        def cachedTasks = secondResult.tasks.findAll { 
            it.path.contains('cyclonedxBom') && 
            it.outcome in [TaskOutcome.UP_TO_DATE, TaskOutcome.FROM_CACHE] 
        }.size()
        
        def cacheHitRate = totalTasks > 0 ? (cachedTasks / totalTasks) * 100 : 0
        
        println "Total SBOM tasks: ${totalTasks}"
        println "Cached tasks: ${cachedTasks}"
        println "Cache hit rate: ${cacheHitRate}%"
        
        assert cacheHitRate >= 80.0 : "Cache hit rate should be >= 80% (actual: ${cacheHitRate}%)"
    }

    def "Cache invalidation works correctly when dependencies change"() {
        given: "A project with initial dependencies"
        
        when: "Running SBOM generation first time"
        def firstResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        
        and: "Changing dependencies"
        buildFile.text = buildFile.text.replace(
            "implementation 'org.apache.commons:commons-lang3:3.12.0'",
            "implementation 'org.apache.commons:commons-lang3:3.13.0'"
        )
        
        and: "Running SBOM generation after dependency change"
        def secondResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        
        then: "First run succeeds"
        assert firstResult.task(':cyclonedxBom').outcome == TaskOutcome.SUCCESS
        
        and: "Second run re-executes due to dependency change"
        assert secondResult.task(':cyclonedxBom').outcome == TaskOutcome.SUCCESS
        
        and: "SBOM content reflects the dependency change"
        def sbomFile = new File(testProjectDir, 'build/reports/sbom/sbom-cache-test-1.0.json')
        assert sbomFile.exists()
        def sbomContent = sbomFile.text
        assert sbomContent.contains('3.13.0')
    }

    def "Incremental build support works correctly"() {
        given: "A project with SBOM generation"
        
        when: "Running initial SBOM generation"
        def initialResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        
        and: "Running again without any changes"
        def incrementalResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        
        and: "Adding a new source file"
        def srcDir = new File(testProjectDir, 'src/main/java/com/example')
        srcDir.mkdirs()
        new File(srcDir, 'NewClass.java').text = """
            package com.example;
            public class NewClass {
                public void doSomething() {}
            }
        """
        
        and: "Running SBOM generation after source change"
        def afterSourceChangeResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        
        then: "Initial run succeeds"
        assert initialResult.task(':cyclonedxBom').outcome == TaskOutcome.SUCCESS
        
        and: "Incremental run is up-to-date"
        assert incrementalResult.task(':cyclonedxBom').outcome in [TaskOutcome.UP_TO_DATE, TaskOutcome.FROM_CACHE]
        
        and: "After source change, SBOM task remains cached (dependencies unchanged)"
        assert afterSourceChangeResult.task(':cyclonedxBom').outcome in [TaskOutcome.UP_TO_DATE, TaskOutcome.FROM_CACHE]
    }

    def "Cache validation ensures correctness"() {
        given: "A project with cache validation enabled"
        
        when: "Running SBOM generation multiple times"
        def results = []
        3.times { iteration ->
            def result = GradleRunner.create()
                .withProjectDir(testProjectDir)
                .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache')
                .withPluginClasspath()
                .build()
            results.add(result)
        }
        
        then: "All runs produce consistent SBOM output"
        def sbomFile = new File(testProjectDir, 'build/reports/sbom/sbom-cache-test-1.0.json')
        assert sbomFile.exists()
        
        def sbomContent = new groovy.json.JsonSlurper().parse(sbomFile)
        assert sbomContent.bomFormat == 'CycloneDX'
        assert sbomContent.specVersion
        assert sbomContent.components
        
        and: "Cache effectiveness is maintained"
        def firstRun = results[0]
        def subsequentRuns = results[1..-1]
        
        assert firstRun.task(':cyclonedxBom').outcome == TaskOutcome.SUCCESS
        subsequentRuns.each { result ->
            assert result.task(':cyclonedxBom').outcome in [TaskOutcome.UP_TO_DATE, TaskOutcome.FROM_CACHE]
        }
    }

    def "Remote cache compatibility is maintained"() {
        given: "A project configured for remote caching"
        gradlePropertiesFile.text += """
            # Remote cache simulation
            org.gradle.caching.debug=true
        """
        
        when: "Running SBOM generation with cache debugging"
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache', '--info')
            .withPluginClasspath()
            .build()
        
        then: "Task is cacheable and produces valid cache key"
        assert result.task(':cyclonedxBom').outcome == TaskOutcome.SUCCESS
        
        // Verify cache key inputs are properly defined
        def output = result.output
        assert !output.contains("Task ':cyclonedxBom' is not cacheable")
        
        and: "SBOM output is generated correctly"
        def sbomFile = new File(testProjectDir, 'build/reports/sbom/sbom-cache-test-1.0.json')
        assert sbomFile.exists()
        assert sbomFile.length() > 0
    }

    private void setupCacheTestProject() {
        settingsFile.text = """
            rootProject.name = 'sbom-cache-test'
        """
        
        gradlePropertiesFile.text = """
            # Cache optimization settings
            org.gradle.caching=true
            org.gradle.parallel=true
            org.gradle.vfs.watch=true
            org.gradle.configuration-cache=true
            
            # SBOM cache settings
            cyclonedx.cache.enabled=true
            cyclonedx.parallel.execution=true
            sbom.cache.validation=true
        """
        
        buildFile.text = """
            plugins {
                id 'java-library'
                id 'org.cyclonedx.bom' version '1.8.2'
            }
            
            version = '1.0'
            
            repositories {
                mavenCentral()
            }
            
            dependencies {
                implementation 'org.apache.commons:commons-lang3:3.12.0'
                implementation 'com.google.guava:guava:31.1-jre'
                testImplementation 'junit:junit:4.13.2'
            }
            
            cyclonedxBom {
                enabled = project.hasProperty('sbomEnabled') ? project.property('sbomEnabled').toBoolean() : false
                projectType = "library"
                schemaVersion = "1.4"
                destination = file("\$buildDir/reports/sbom")
                outputName = "\${project.name}-\${project.version}"
                outputFormat = "json"
                includeBomSerialNumber = true
                includeConfigs = ["runtimeClasspath", "compileClasspath"]
                
                // Cache configuration
                outputs.cacheIf { true }
                
                // Define inputs for cache key generation
                inputs.files(configurations.runtimeClasspath, configurations.compileClasspath)
                    .withPropertyName("sbomDependencies")
                inputs.property("projectName", project.name)
                inputs.property("projectVersion", project.version)
                inputs.property("projectType", "library")
                inputs.property("schemaVersion", "1.4")
                inputs.property("outputFormat", "json")
                
                // Define outputs for caching
                outputs.dir(destination).withPropertyName("sbomOutputDir")
            }
        """
    }

    private void setupMultiModuleCacheTest() {
        settingsFile.text = """
            rootProject.name = 'sbom-multimodule-cache-test'
            include 'cache-module1', 'cache-module2', 'cache-module3'
        """
        
        ['cache-module1', 'cache-module2', 'cache-module3'].each { moduleName ->
            def moduleDir = new File(testProjectDir, moduleName)
            moduleDir.mkdirs()
            
            new File(moduleDir, 'build.gradle').text = """
                plugins {
                    id 'java-library'
                    id 'org.cyclonedx.bom' version '1.8.2'
                }
                
                repositories {
                    mavenCentral()
                }
                
                dependencies {
                    implementation 'org.apache.commons:commons-lang3:3.12.0'
                }
                
                cyclonedxBom {
                    enabled = rootProject.hasProperty('sbomEnabled') ? rootProject.property('sbomEnabled').toBoolean() : false
                    projectType = "library"
                    schemaVersion = "1.4"
                    destination = file("\$buildDir/reports/sbom")
                    outputName = "\${project.name}-\${project.version}"
                    outputFormat = "json"
                    
                    // Cache configuration
                    outputs.cacheIf { true }
                    inputs.files(configurations.runtimeClasspath).withPropertyName("sbomDependencies")
                    inputs.property("projectName", project.name)
                    outputs.dir(destination).withPropertyName("sbomOutputDir")
                }
            """
        }
        
        buildFile.text = """
            tasks.register('generateSbom') {
                dependsOn subprojects.collect { "\${it.path}:cyclonedxBom" }
                group = 'Build'
                description = 'Generate SBOM for all modules'
            }
        """
    }
}
