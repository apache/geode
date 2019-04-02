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
import org.apache.geode.test.util.ResourceUtils;

public class GradleBuildWithGeodeCoreAcceptanceTest {

  @Rule
  public RequiresGeodeHome geodeHome = new RequiresGeodeHome();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testBasicGradleBuild() throws Exception {
    URL projectDir = ResourceUtils.getResource("/gradle-test-projects/management");
    assertThat(projectDir).isNotNull();

    String projectGroup = System.getProperty("projectGroup");
    assertThat(projectGroup).as("'projectGroup' is not available as a system property")
        .isNotBlank();

    String geodeVersion = GemFireVersion.getGemFireVersion();

    File buildDir = temp.getRoot();
    ResourceUtils.copyDirectoryResource(projectDir, buildDir);

    GradleConnector connector = GradleConnector.newConnector();
    connector.useBuildDistribution();
    connector.forProjectDirectory(buildDir);

    ProjectConnection connection = connector.connect();
    BuildLauncher build = connection.newBuild();

    build.setStandardError(System.err);
    build.setStandardOutput(System.out);
    build.withArguments("-PgeodeVersion=" + geodeVersion,
        "-PprojectGroup=" + projectGroup,
        "-PgeodeHome=" + geodeHome.toString());

    build.forTasks("installDist", "run");
    build.run();

    connection.close();
  }

}
