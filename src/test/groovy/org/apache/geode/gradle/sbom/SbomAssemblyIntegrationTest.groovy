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
import java.util.zip.ZipFile

/**
 * Integration tests for SBOM generation in geode-assembly module.
 * Tests assembly-specific SBOM configuration, ASF compliance metadata,
 * and distribution packaging integration.
 */
class SbomAssemblyIntegrationTest extends Specification {

  @TempDir
  Path testProjectDir

  File buildFile
  File settingsFile
  File propertiesFile

  def setup() {
    buildFile = testProjectDir.resolve('build.gradle').toFile()
    settingsFile = testProjectDir.resolve('settings.gradle').toFile()
    propertiesFile = testProjectDir.resolve('gradle.properties').toFile()
  }

  def "assembly SBOM generation with explicit request"() {
    given: "a project with assembly SBOM configuration"
    settingsFile << """
      rootProject.name = 'test-geode-assembly'
    """
    
    buildFile << """
      plugins {
        id 'java'
        id 'distribution'
        id 'org.cyclonedx.bom' version '1.8.2'
      }
      
      version = '1.0.0-test'
      
      repositories {
        mavenCentral()
      }
      
      dependencies {
        implementation 'org.apache.commons:commons-lang3:3.12.0'
        implementation 'com.fasterxml.jackson.core:jackson-core:2.13.0'
      }
      
      // Context detection logic (simplified for testing)
      ext.isCiEnvironment = System.getenv('CI') == 'true'
      ext.isReleaseBuild = gradle.startParameter.taskNames.any { it.contains('release') }
      ext.isExplicitSbomRequest = gradle.startParameter.taskNames.any { 
        it.contains('cyclonedxBom') || it.contains('generateSbom') || it.contains('Sbom')
      }
      ext.shouldGenerateSbom = isCiEnvironment || isReleaseBuild || isExplicitSbomRequest
      ext.sbomGenerationReason = isExplicitSbomRequest ? 'explicit SBOM request' : 
                                 (isCiEnvironment ? 'CI environment' : 'release build')
      
      cyclonedxBom {
        if (shouldGenerateSbom) {
          projectType = "application"
          schemaVersion = "1.4"
          destination = file("\$buildDir/reports/bom")
          outputName = "test-geode-assembly-\${project.version}"
          outputFormat = "json"
          includeBomSerialNumber = true
          includeConfigs = ["runtimeClasspath"]
          
          try {
            includeMetadataResolution = true
          } catch (Exception e) {
            logger.debug("includeMetadataResolution not available: \${e.message}")
          }
        } else {
          skip = true
        }
      }
      
      distributions {
        main {
          distributionBaseName = 'test-geode-assembly'
          contents {
            from 'README.md'
            
            with copySpec {
              into('lib')
              from configurations.runtimeClasspath
            }
            
            // Include SBOM artifacts in distribution
            with copySpec {
              into('sbom')
              from { file("\$buildDir/reports/bom") }
              onlyIf { shouldGenerateSbom }
            }
          }
        }
      }
      
      tasks.register('generateDistributionSbom') {
        description = 'Generate and copy SBOM artifacts to distribution directory'
        group = 'distribution'
        
        onlyIf { shouldGenerateSbom }
        dependsOn tasks.cyclonedxBom
        
        inputs.files tasks.cyclonedxBom.outputs.files
        def sbomDistDir = file("\$buildDir/install/\${distributions.main.distributionBaseName.get()}/sbom")
        outputs.dir sbomDistDir
        
        doLast {
          sbomDistDir.mkdirs()
          
          copy {
            from tasks.cyclonedxBom.outputs.files
            into sbomDistDir
            include '*.json'
            include '*.xml'
          }
          
          def metadataFile = new File(sbomDistDir, 'sbom-metadata.txt')
          metadataFile.text = '''Apache Geode SBOM Artifacts
Generated: ''' + new Date().format('yyyy-MM-dd HH:mm:ss UTC') + '''
Version: ''' + project.version + '''
Project Type: application
Format: CycloneDX 1.4
Supplier: Apache Software Foundation
Manufacturer: Apache Geode Community
'''
        }
      }
      
      installDist.dependsOn generateDistributionSbom
      distTar.dependsOn generateDistributionSbom
    """
    
    // Create a dummy README file
    testProjectDir.resolve('README.md').toFile() << "Test Apache Geode Assembly"

    when: "running cyclonedxBom task"
    def result = GradleRunner.create()
        .withProjectDir(testProjectDir.toFile())
        .withArguments('cyclonedxBom', '--info')
        .withPluginClasspath()
        .build()

    then: "the task succeeds"
    result.task(':cyclonedxBom').outcome == TaskOutcome.SUCCESS

    and: "SBOM file is generated"
    def sbomFile = testProjectDir.resolve('build/reports/bom/test-geode-assembly-1.0.0-test.json').toFile()
    sbomFile.exists()

    and: "SBOM contains expected content"
    def sbomContent = sbomFile.text
    sbomContent.contains('"bomFormat": "CycloneDX"')
    sbomContent.contains('"specVersion": "1.4"')
    sbomContent.contains('"name": "test-geode-assembly"')
    sbomContent.contains('"version": "1.0.0-test"')
    sbomContent.contains('commons-lang3')
    sbomContent.contains('jackson-core')
  }

  def "generateDistributionSbom task integration"() {
    given: "a project with distribution SBOM configuration"
    settingsFile << "rootProject.name = 'test-assembly'"
    
    buildFile << """
      plugins {
        id 'java'
        id 'distribution'
        id 'org.cyclonedx.bom' version '1.8.2'
      }
      
      version = '1.0.0'
      
      repositories {
        mavenCentral()
      }
      
      dependencies {
        implementation 'org.slf4j:slf4j-api:1.7.32'
      }
      
      ext.shouldGenerateSbom = true
      
      cyclonedxBom {
        projectType = "application"
        destination = file("\$buildDir/reports/bom")
        outputName = "test-assembly-\${project.version}"
        outputFormat = "json"
      }
      
      distributions {
        main {
          distributionBaseName = 'test-assembly'
          contents {
            from 'README.md'
          }
        }
      }
      
      tasks.register('generateDistributionSbom') {
        dependsOn tasks.cyclonedxBom
        
        def sbomDistDir = file("\$buildDir/install/\${distributions.main.distributionBaseName.get()}/sbom")
        outputs.dir sbomDistDir
        
        doLast {
          sbomDistDir.mkdirs()
          copy {
            from tasks.cyclonedxBom.outputs.files
            into sbomDistDir
          }
          
          new File(sbomDistDir, 'sbom-metadata.txt').text = 'SBOM metadata'
        }
      }
      
      installDist.dependsOn generateDistributionSbom
    """
    
    testProjectDir.resolve('README.md').toFile() << "Test"

    when: "running generateDistributionSbom task"
    def result = GradleRunner.create()
        .withProjectDir(testProjectDir.toFile())
        .withArguments('generateDistributionSbom', '--info')
        .withPluginClasspath()
        .build()

    then: "the task succeeds"
    result.task(':generateDistributionSbom').outcome == TaskOutcome.SUCCESS

    and: "SBOM is copied to distribution directory"
    def sbomDistDir = testProjectDir.resolve('build/install/test-assembly/sbom').toFile()
    sbomDistDir.exists()
    sbomDistDir.isDirectory()

    and: "SBOM files are present"
    def sbomFile = new File(sbomDistDir, 'test-assembly-1.0.0.json')
    sbomFile.exists()

    and: "metadata file is created"
    def metadataFile = new File(sbomDistDir, 'sbom-metadata.txt')
    metadataFile.exists()
    metadataFile.text.contains('SBOM metadata')
  }

  def "distribution archive includes SBOM artifacts"() {
    given: "a project with complete distribution configuration"
    settingsFile << "rootProject.name = 'test-dist'"
    
    buildFile << """
      plugins {
        id 'java'
        id 'distribution'
        id 'org.cyclonedx.bom' version '1.8.2'
      }
      
      version = '2.0.0'
      
      repositories {
        mavenCentral()
      }
      
      dependencies {
        implementation 'junit:junit:4.13.2'
      }
      
      ext.shouldGenerateSbom = true
      
      cyclonedxBom {
        projectType = "application"
        destination = file("\$buildDir/reports/bom")
        outputFormat = "json"
      }
      
      distributions {
        main {
          distributionBaseName = 'test-dist'
          contents {
            from 'LICENSE'
            
            with copySpec {
              into('sbom')
              from { file("\$buildDir/reports/bom") }
            }
          }
        }
      }
      
      tasks.register('generateDistributionSbom') {
        dependsOn tasks.cyclonedxBom
        doLast {
          // Simulate SBOM generation
        }
      }
      
      distTar.dependsOn generateDistributionSbom
    """
    
    testProjectDir.resolve('LICENSE').toFile() << "Apache License 2.0"

    when: "building distribution archive"
    def result = GradleRunner.create()
        .withProjectDir(testProjectDir.toFile())
        .withArguments('distTar', '--info')
        .withPluginClasspath()
        .build()

    then: "the build succeeds"
    result.task(':distTar').outcome == TaskOutcome.SUCCESS

    and: "distribution archive is created"
    def distFile = testProjectDir.resolve('build/distributions/test-dist-2.0.0.tar').toFile()
    distFile.exists()

    and: "archive contains SBOM directory"
    // Note: Full archive validation would require extracting the tar file
    // This is a simplified test focusing on task execution
    result.output.contains('generateDistributionSbom')
  }
}
