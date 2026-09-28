/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.gradle.plugins

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaBasePlugin
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.plugins.ide.eclipse.EclipsePlugin
import org.gradle.plugins.ide.eclipse.model.EclipseModel
import org.gradle.plugins.ide.idea.IdeaPlugin
import org.gradle.plugins.ide.idea.model.IdeaModel

/**
 * Adds a {@code facets} block that declares additional source sets, each extending a parent
 * source set (by default {@code main}). A facet whose name contains {@code Test} also gets a
 * {@link Test} task that runs its tests.
 *
 * <pre>
 * facets {
 *   integrationTest {
 *     includeInCheckLifecycle = false
 *   }
 * }
 * </pre>
 */
class FacetsPlugin implements Plugin<Project> {

  @Override
  void apply(Project project) {
    project.extensions.add('facets', new FacetsExtension(project))
  }
}

class FacetsExtension {
  private final Project project

  FacetsExtension(Project project) {
    this.project = project
  }

  /**
   * Declares a facet named after the method, configured by the closure argument.
   */
  def methodMissing(String name, Object args) {
    Object[] argArray = args as Object[]
    if (argArray.length != 1 || !(argArray[0] instanceof Closure)) {
      throw new MissingMethodException(name, getClass(), argArray)
    }
    FacetDefinition facet = new FacetDefinition(name)
    Closure configuration = (argArray[0] as Closure).clone() as Closure
    configuration.resolveStrategy = Closure.DELEGATE_FIRST
    configuration.delegate = facet
    configuration.call(facet)
    addFacet(facet)
    return facet
  }

  private void addFacet(FacetDefinition facet) {
    project.plugins.withType(JavaBasePlugin) {
      SourceSetContainer sourceSets = project.extensions.getByType(SourceSetContainer)
      sourceSets.matching { it.name == facet.parentSourceSet }.all { SourceSet parent ->
        SourceSet sourceSet = createSourceSet(sourceSets, parent, facet)

        [
            [parent.compileClasspathConfigurationName, sourceSet.compileClasspathConfigurationName],
            [parent.runtimeClasspathConfigurationName, sourceSet.runtimeClasspathConfigurationName],
            [parent.annotationProcessorConfigurationName, sourceSet.annotationProcessorConfigurationName]
        ].each { String parentName, String childName ->
          project.configurations.getByName(childName)
              .extendsFrom(project.configurations.getByName(parentName))
        }

        project.tasks.named('build').configure { it.dependsOn(sourceSet.classesTaskName) }

        if (facet.isTestFacet()) {
          def testTask = createTestTask(facet.testTaskName, sourceSet)
          if (facet.includeInCheckLifecycle) {
            project.tasks.named('check').configure { it.dependsOn(testTask) }
          }
        }

        configureIde(sourceSet, facet.isTestFacet())
      }
    }
  }

  /**
   * Creates the facet's source set. Its classpaths also include the output of its parent and,
   * for a parent other than {@code main}, the output of {@code main}, which the parent sees.
   */
  private SourceSet createSourceSet(SourceSetContainer sourceSets, SourceSet parent,
      FacetDefinition facet) {
    SourceSet main = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME)
    List<Object> parentOutputs = parent == main ? [main.output] : [parent.output, main.output]
    return sourceSets.create(facet.name) { SourceSet sourceSet ->
      sourceSet.compileClasspath = project.objects.fileCollection()
          .from([sourceSet.compileClasspath] + parentOutputs)
      sourceSet.runtimeClasspath = project.objects.fileCollection()
          .from([sourceSet.runtimeClasspath] + parentOutputs)
    }
  }

  private def createTestTask(String testTaskName, SourceSet sourceSet) {
    return project.tasks.register(testTaskName, Test) { Test test ->
      test.group = JavaBasePlugin.VERIFICATION_GROUP
      test.description = "Runs the ${sourceSet.name} tests"
      test.reports.html.outputLocation.set(
          project.layout.buildDirectory.dir("reports/${sourceSet.name}"))
      test.reports.junitXml.outputLocation.set(
          project.layout.buildDirectory.dir("${sourceSet.name}-results"))
      test.testClassesDirs = sourceSet.output.classesDirs
      test.classpath = sourceSet.runtimeClasspath
      test.mustRunAfter(project.tasks.named('test'))
    }
  }

  private void configureIde(SourceSet sourceSet, boolean isTest) {
    def classpathConfigurations = [
        project.configurations.getByName(sourceSet.compileClasspathConfigurationName),
        project.configurations.getByName(sourceSet.runtimeClasspathConfigurationName)
    ]
    project.plugins.withType(IdeaPlugin) {
      def module = project.extensions.getByType(IdeaModel).module
      if (isTest) {
        module.testSources.from(sourceSet.allSource.srcDirs)
        module.scopes.TEST.plus += classpathConfigurations
      } else {
        module.sourceDirs += sourceSet.allSource.srcDirs
        module.scopes.COMPILE.plus += classpathConfigurations
      }
    }
    project.plugins.withType(EclipsePlugin) {
      project.extensions.getByType(EclipseModel).classpath.plusConfigurations +=
          classpathConfigurations
    }
  }
}

class FacetDefinition {
  final String name
  String parentSourceSet = SourceSet.MAIN_SOURCE_SET_NAME
  String testTaskName
  boolean includeInCheckLifecycle = true

  FacetDefinition(String name) {
    this.name = name
  }

  String getTestTaskName() {
    return testTaskName ?: name
  }

  boolean isTestFacet() {
    return name.contains('Test')
  }
}
