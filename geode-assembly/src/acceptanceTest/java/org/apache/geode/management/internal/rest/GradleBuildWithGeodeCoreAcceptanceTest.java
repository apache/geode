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

package org.apache.geode.management.internal.rest;

import static org.apache.geode.test.util.ResourceUtils.copyDirectoryResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URL;

import org.gradle.tooling.BuildLauncher;
import org.gradle.tooling.GradleConnector;
import org.gradle.tooling.ProjectConnection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

public class GradleBuildWithGeodeCoreAcceptanceTest {

  @Rule
  public RequiresGeodeHome geodeHome = new RequiresGeodeHome();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testBasicGradleBuild() {
    URL projectDir = getResource("/gradle-test-projects/management");
    assertThat(projectDir).isNotNull();

    String projectGroup = System.getProperty("projectGroup");
    assertThat(projectGroup)
        .as("'projectGroup' system property")
        .isNotBlank();

    String gradleJvm = System.getenv("GRADLE_JVM");
    assertThat(gradleJvm)
        .as("'GRADLE_JVM' environment variable")
        .isNotBlank();

    File gradleJvmFile = new File(gradleJvm);
    assertThat(gradleJvmFile)
        .as("'GRADLE_JVM' directory")
        .isDirectory();

    String geodeVersion = GemFireVersion.getGemFireVersion();

    File buildDir = temp.getRoot();
    copyDirectoryResource(projectDir, buildDir);

    GradleConnector connector = GradleConnector.newConnector();
    connector.useBuildDistribution();
    connector.forProjectDirectory(buildDir);

    ProjectConnection connection = connector.connect();
    BuildLauncher build = connection.newBuild();
    build.setJavaHome(gradleJvmFile);

    build.setStandardError(System.err);
    build.setStandardOutput(System.out);
    build.withArguments(
        // CHANGE: Add --rerun-tasks to force task re-execution
        // REASON: Fixes Gradle cache corruption issues with Groovy VM plugin
        // When the Gradle daemon caches corrupted plugin state, subsequent builds fail
        // This flag forces Gradle to ignore cached task outputs and re-execute from scratch
        "--rerun-tasks",
        "-Pversion=" + geodeVersion,
        "-Pgroup=" + projectGroup,
        "-PgeodeHome=" + geodeHome);

    // CHANGE: Add "clean" task to ensure fresh build state
    // REASON: Complements --rerun-tasks by also clearing build directory
    // This provides two-layered approach to cache corruption recovery:
    // 1. clean removes all generated files and cached artifacts
    // 2. --rerun-tasks forces re-execution of all tasks
    build.forTasks("clean", "installDist", "run");
    build.run();

    connection.close();
  }
}
