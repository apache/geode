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
 * Performance regression tests for SBOM generation (GEODE-10481 PR 6)
 * Ensures performance optimizations meet targets and prevent regressions
 */
class SbomPerformanceRegressionTest extends Specification {

    @TempDir
    File testProjectDir

    File buildFile
    File settingsFile
    File gradlePropertiesFile

    def setup() {
        buildFile = new File(testProjectDir, 'build.gradle')
        settingsFile = new File(testProjectDir, 'settings.gradle')
        gradlePropertiesFile = new File(testProjectDir, 'gradle.properties')
        
        setupPerformanceTestProject()
    }

    def "SBOM generation performance impact is under 3% with caching enabled"() {
        given: "A project with caching enabled"
        
        when: "Running build without SBOM generation (baseline)"
        def baselineResult = runBuildWithTiming(['build', '--build-cache'])
        def baselineTime = baselineResult.executionTime
        
        and: "Running build with SBOM generation"
        def sbomResult = runBuildWithTiming(['build', 'generateSbom', '--build-cache', '-PsbomEnabled=true'])
        def sbomTime = sbomResult.executionTime
        
        then: "Performance impact is under 3%"
        def performanceImpact = ((sbomTime - baselineTime) / baselineTime) * 100
        println "Baseline time: ${baselineTime}ms"
        println "SBOM generation time: ${sbomTime}ms"
        println "Performance impact: ${performanceImpact}%"
        
        assert performanceImpact < 3.0 : "Performance impact should be less than 3% (actual: ${performanceImpact}%)"
        assert sbomResult.result.task(':generateSbom').outcome == TaskOutcome.SUCCESS
    }

    def "Single module SBOM generation completes under 30 seconds"() {
        given: "A single module project"
        
        when: "Generating SBOM for single module"
        def startTime = System.currentTimeMillis()
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        def endTime = System.currentTimeMillis()
        def duration = endTime - startTime
        
        then: "Generation completes under 30 seconds"
        assert duration < 30000 : "Single module SBOM generation should complete under 30 seconds (actual: ${duration}ms)"
        assert result.task(':cyclonedxBom').outcome == TaskOutcome.SUCCESS
    }

    def "Cache hit scenario completes under 10 seconds"() {
        given: "A project with pre-generated SBOM cache"
        
        when: "Running SBOM generation first time"
        def firstRun = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        
        and: "Running SBOM generation second time (cache hit)"
        def startTime = System.currentTimeMillis()
        def secondRun = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('cyclonedxBom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        def endTime = System.currentTimeMillis()
        def cacheHitTime = endTime - startTime
        
        then: "Cache hit scenario completes under 10 seconds"
        assert cacheHitTime < 10000 : "Cache hit scenario should complete under 10 seconds (actual: ${cacheHitTime}ms)"
        assert firstRun.task(':cyclonedxBom').outcome == TaskOutcome.SUCCESS
        // Second run should be UP-TO-DATE or FROM-CACHE
        assert secondRun.task(':cyclonedxBom').outcome in [TaskOutcome.UP_TO_DATE, TaskOutcome.FROM_CACHE]
    }

    def "Memory usage remains under 500MB additional heap"() {
        given: "Memory monitoring setup"
        def runtime = Runtime.getRuntime()
        def initialMemory = runtime.totalMemory() - runtime.freeMemory()
        
        when: "Running SBOM generation"
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        
        def finalMemory = runtime.totalMemory() - runtime.freeMemory()
        def memoryIncrease = finalMemory - initialMemory
        
        then: "Memory usage increase is under 500MB"
        assert memoryIncrease < 500 * 1024 * 1024 : "Memory usage should be under 500MB (actual: ${memoryIncrease / (1024 * 1024)}MB)"
        assert result.task(':generateSbom').outcome == TaskOutcome.SUCCESS
    }

    def "Parallel execution efficiency is above 70%"() {
        given: "A multi-module project with parallel execution enabled"
        setupMultiModuleProject()
        
        when: "Running SBOM generation with parallel execution"
        def startTime = System.currentTimeMillis()
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '-PsbomEnabled=true', '--parallel', '--build-cache')
            .withPluginClasspath()
            .build()
        def endTime = System.currentTimeMillis()
        def parallelTime = endTime - startTime
        
        and: "Running SBOM generation without parallel execution"
        def sequentialStartTime = System.currentTimeMillis()
        def sequentialResult = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '-PsbomEnabled=true', '--build-cache')
            .withPluginClasspath()
            .build()
        def sequentialEndTime = System.currentTimeMillis()
        def sequentialTime = sequentialEndTime - sequentialStartTime
        
        then: "Parallel efficiency is above 70%"
        def efficiency = (sequentialTime - parallelTime) / sequentialTime
        println "Sequential time: ${sequentialTime}ms"
        println "Parallel time: ${parallelTime}ms"
        println "Parallel efficiency: ${efficiency * 100}%"
        
        assert efficiency > 0.7 : "Parallel efficiency should be above 70% (actual: ${efficiency * 100}%)"
        assert result.task(':generateSbom').outcome == TaskOutcome.SUCCESS
        assert sequentialResult.task(':generateSbom').outcome == TaskOutcome.SUCCESS
    }

    def "Build time impact regression detection"() {
        given: "Baseline performance measurements"
        def baselineTimes = []
        
        // Run multiple baseline measurements
        3.times {
            def result = runBuildWithTiming(['build', '--build-cache'])
            baselineTimes.add(result.executionTime)
        }
        def averageBaseline = baselineTimes.sum() / baselineTimes.size()
        
        when: "Running builds with SBOM generation"
        def sbomTimes = []
        3.times {
            def result = runBuildWithTiming(['build', 'generateSbom', '--build-cache', '-PsbomEnabled=true'])
            sbomTimes.add(result.executionTime)
        }
        def averageWithSbom = sbomTimes.sum() / sbomTimes.size()
        
        then: "Performance regression is within acceptable limits"
        def performanceImpact = ((averageWithSbom - averageBaseline) / averageBaseline) * 100
        
        // Store performance data for trend analysis
        def performanceData = [
            timestamp: System.currentTimeMillis(),
            baselineTime: averageBaseline,
            sbomTime: averageWithSbom,
            performanceImpact: performanceImpact
        ]
        
        println "Performance regression test results:"
        println "  Baseline average: ${averageBaseline}ms"
        println "  SBOM average: ${averageWithSbom}ms"
        println "  Performance impact: ${performanceImpact}%"
        
        assert performanceImpact < 5.0 : "Performance regression should be under 5% (actual: ${performanceImpact}%)"
    }

    private def runBuildWithTiming(List<String> arguments) {
        def startTime = System.currentTimeMillis()
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments(arguments)
            .withPluginClasspath()
            .build()
        def endTime = System.currentTimeMillis()
        
        return [
            result: result,
            executionTime: endTime - startTime
        ]
    }

    private void setupPerformanceTestProject() {
        settingsFile.text = """
            rootProject.name = 'sbom-performance-test'
        """
        
        gradlePropertiesFile.text = """
            # Performance optimization settings
            org.gradle.caching=true
            org.gradle.parallel=true
            org.gradle.workers.max=4
            org.gradle.vfs.watch=true
            org.gradle.configuration-cache=true
            
            # SBOM performance settings
            cyclonedx.parallel.execution=true
            cyclonedx.cache.enabled=true
            sbom.performance.monitoring=true
        """
        
        buildFile.text = """
            plugins {
                id 'java-library'
                id 'org.cyclonedx.bom' version '1.8.2'
            }
            
            repositories {
                mavenCentral()
            }
            
            dependencies {
                implementation 'org.apache.commons:commons-lang3:3.12.0'
                implementation 'com.google.guava:guava:31.1-jre'
                testImplementation 'junit:junit:4.13.2'
            }
            
            // SBOM configuration with performance optimizations
            cyclonedxBom {
                enabled = project.hasProperty('sbomEnabled') ? project.property('sbomEnabled').toBoolean() : false
                projectType = "library"
                schemaVersion = "1.4"
                destination = file("\$buildDir/reports/sbom")
                outputName = "\${project.name}-\${project.version}"
                outputFormat = "json"
                includeBomSerialNumber = true
                includeConfigs = ["runtimeClasspath", "compileClasspath"]
                
                // Performance optimizations
                outputs.cacheIf { true }
                inputs.files(configurations.runtimeClasspath, configurations.compileClasspath)
                    .withPropertyName("sbomDependencies")
                inputs.property("projectName", project.name)
                inputs.property("projectVersion", project.version)
                outputs.dir(destination).withPropertyName("sbomOutputDir")
            }
            
            tasks.register('generateSbom') {
                dependsOn cyclonedxBom
                group = 'Build'
                description = 'Generate SBOM for performance testing'
            }
        """
    }

    private void setupMultiModuleProject() {
        settingsFile.text = """
            rootProject.name = 'sbom-multimodule-performance-test'
            include 'module1', 'module2', 'module3'
        """
        
        ['module1', 'module2', 'module3'].each { moduleName ->
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
                    
                    // Performance optimizations
                    outputs.cacheIf { true }
                    inputs.files(configurations.runtimeClasspath).withPropertyName("sbomDependencies")
                    outputs.dir(destination).withPropertyName("sbomOutputDir")
                }
            """
        }
        
        buildFile.text = """
            plugins {
                id 'java-library'
            }
            
            tasks.register('generateSbom') {
                dependsOn subprojects.collect { "\${it.path}:cyclonedxBom" }
                group = 'Build'
                description = 'Generate SBOM for all modules'
            }
        """
    }
}
