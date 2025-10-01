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
import spock.lang.Specification
import spock.lang.TempDir

/**
 * Performance benchmarking tests for multi-module SBOM generation (GEODE-10481 PR 4)
 * Ensures SBOM generation meets performance requirements
 */
class SbomPerformanceBenchmarkTest extends Specification {

    @TempDir
    File testProjectDir

    File buildFile
    File settingsFile
    File gradlePropertiesFile

    def setup() {
        buildFile = new File(testProjectDir, 'build.gradle')
        settingsFile = new File(testProjectDir, 'settings.gradle')
        gradlePropertiesFile = new File(testProjectDir, 'gradle.properties')
        
        setupLargeTestProject()
    }

    def "SBOM generation performance impact is under 3% for large project"() {
        given: "A large multi-module project simulating Geode's structure"
        
        when: "Running build without SBOM generation (baseline)"
        def baselineTimes = []
        3.times {
            def startTime = System.currentTimeMillis()
            def result = GradleRunner.create()
                .withProjectDir(testProjectDir)
                .withArguments('build', '--parallel', '--build-cache')
                .withPluginClasspath()
                .build()
            def duration = System.currentTimeMillis() - startTime
            baselineTimes.add(duration)
        }
        def averageBaseline = baselineTimes.sum() / baselineTimes.size()
        
        and: "Running build with SBOM generation"
        def sbomTimes = []
        3.times {
            def startTime = System.currentTimeMillis()
            def result = GradleRunner.create()
                .withProjectDir(testProjectDir)
                .withArguments('generateSbom', '--parallel', '--build-cache')
                .withPluginClasspath()
                .build()
            def duration = System.currentTimeMillis() - startTime
            sbomTimes.add(duration)
        }
        def averageWithSbom = sbomTimes.sum() / sbomTimes.size()
        
        then: "Performance impact is under 3%"
        def performanceImpact = ((averageWithSbom - averageBaseline) / averageBaseline) * 100
        println "Baseline average: ${averageBaseline}ms"
        println "SBOM generation average: ${averageWithSbom}ms"
        println "Performance impact: ${performanceImpact}%"
        
        assert performanceImpact < 3.0 : "Performance impact should be less than 3% (actual: ${performanceImpact}%)"
    }

    def "memory usage during SBOM generation is reasonable"() {
        given: "A large multi-module project"
        
        when: "Running SBOM generation with memory monitoring"
        def runtime = Runtime.getRuntime()
        def initialMemory = runtime.totalMemory() - runtime.freeMemory()
        
        def result = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '--parallel', '-Xmx1g')
            .withPluginClasspath()
            .build()
        
        def finalMemory = runtime.totalMemory() - runtime.freeMemory()
        def memoryIncrease = finalMemory - initialMemory
        
        then: "Memory usage increase is reasonable (< 500MB for test)"
        def memoryIncreaseMB = memoryIncrease / (1024 * 1024)
        println "Memory increase: ${memoryIncreaseMB}MB"
        
        assert memoryIncreaseMB < 500 : "Memory increase should be less than 500MB (actual: ${memoryIncreaseMB}MB)"
    }

    def "parallel execution reduces total SBOM generation time"() {
        given: "A multi-module project"
        
        when: "Running SBOM generation sequentially"
        def startTimeSequential = System.currentTimeMillis()
        def resultSequential = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '--no-parallel')
            .withPluginClasspath()
            .build()
        def sequentialTime = System.currentTimeMillis() - startTimeSequential
        
        and: "Running SBOM generation in parallel"
        def startTimeParallel = System.currentTimeMillis()
        def resultParallel = GradleRunner.create()
            .withProjectDir(testProjectDir)
            .withArguments('generateSbom', '--parallel')
            .withPluginClasspath()
            .build()
        def parallelTime = System.currentTimeMillis() - startTimeParallel
        
        then: "Parallel execution is faster than sequential"
        def improvement = ((sequentialTime - parallelTime) / sequentialTime) * 100
        println "Sequential time: ${sequentialTime}ms"
        println "Parallel time: ${parallelTime}ms"
        println "Improvement: ${improvement}%"
        
        assert parallelTime < sequentialTime : "Parallel execution should be faster than sequential"
        assert improvement > 10 : "Parallel execution should provide at least 10% improvement (actual: ${improvement}%)"
    }

    def "SBOM generation scales linearly with module count"() {
        given: "Projects with different module counts"
        def moduleCounts = [5, 10, 20]
        def timings = [:]
        
        when: "Testing SBOM generation with different module counts"
        moduleCounts.each { moduleCount ->
            def projectDir = createProjectWithModules(moduleCount)
            
            def startTime = System.currentTimeMillis()
            def result = GradleRunner.create()
                .withProjectDir(projectDir)
                .withArguments('generateSbom', '--parallel')
                .withPluginClasspath()
                .build()
            def duration = System.currentTimeMillis() - startTime
            
            timings[moduleCount] = duration
        }
        
        then: "Scaling is approximately linear"
        def timePerModule5 = timings[5] / 5
        def timePerModule10 = timings[10] / 10
        def timePerModule20 = timings[20] / 20
        
        println "Time per module (5 modules): ${timePerModule5}ms"
        println "Time per module (10 modules): ${timePerModule10}ms"
        println "Time per module (20 modules): ${timePerModule20}ms"
        
        // Allow for some variance in timing, but should be roughly linear
        def variance = Math.abs(timePerModule20 - timePerModule5) / timePerModule5
        assert variance < 0.5 : "Time per module should scale roughly linearly (variance: ${variance})"
    }

    private void setupLargeTestProject() {
        // Create a project structure similar to Geode with 30+ modules
        def modules = []
        
        // Core modules
        (1..15).each { i -> modules.add("geode-core-${i}") }
        
        // Extension modules
        (1..10).each { i -> modules.add("geode-ext-${i}") }
        
        // Web modules
        (1..5).each { i -> modules.add("geode-web-${i}") }
        
        // Utility modules
        (1..5).each { i -> modules.add("geode-util-${i}") }
        
        settingsFile.text = """
            rootProject.name = 'large-test-geode'
            ${modules.collect { "include '${it}'" }.join('\n            ')}
        """
        
        gradlePropertiesFile.text = """
            org.gradle.jvmargs=-Xmx2g -XX:MaxMetaspaceSize=512m
            org.gradle.parallel=true
            org.gradle.caching=true
            org.gradle.configureondemand=true
        """
        
        buildFile.text = createLargeBuildScript()
        
        // Create module build files
        modules.each { moduleName ->
            createModuleBuildFile(moduleName)
        }
    }

    private String createLargeBuildScript() {
        return """
            plugins {
                id 'org.cyclonedx.bom' version '1.8.2' apply false
            }
            
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
            
            configure(subprojects, sbomConfiguration)
            
            tasks.register('generateSbom') {
                group = 'Build'
                description = 'Generate SBOM for all modules'
                
                dependsOn subprojects.collect { "\${it.path}:cyclonedxBom" }
                
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

    private void createModuleBuildFile(String moduleName) {
        def moduleDir = new File(testProjectDir, moduleName)
        moduleDir.mkdirs()
        
        new File(moduleDir, 'build.gradle').text = """
            plugins {
                id 'java-library'
            }
            
            dependencies {
                implementation 'org.slf4j:slf4j-api:1.7.36'
                implementation 'com.fasterxml.jackson.core:jackson-core:2.13.3'
                testImplementation 'junit:junit:4.13.2'
            }
        """
    }

    private File createProjectWithModules(int moduleCount) {
        def projectDir = File.createTempDir("sbom-perf-test-${moduleCount}", "")
        
        def modules = (1..moduleCount).collect { "test-module-${it}" }
        
        new File(projectDir, 'settings.gradle').text = """
            rootProject.name = 'perf-test-${moduleCount}'
            ${modules.collect { "include '${it}'" }.join('\n            ')}
        """
        
        new File(projectDir, 'build.gradle').text = createLargeBuildScript()
        
        modules.each { moduleName ->
            def moduleDir = new File(projectDir, moduleName)
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
        
        return projectDir
    }
}
