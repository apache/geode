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

package org.apache.geode.management;

import static org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert.assertManagementListResult;
import static org.apache.geode.test.junit.rules.gfsh.GfshRule.startLocatorCommand;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class DeploymentManagementUpgradeTest {
  private final String oldVersion;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(s -> TestVersion.compare(s, "1.10.0") < 0);
    return result;
  }

  public DeploymentManagementUpgradeTest(String version) {
    oldVersion = version;
    oldGfsh = new GfshRule(oldVersion);
  }

  @Rule
  public GfshRule oldGfsh;

  @Rule
  public GfshRule gfsh = new GfshRule();

  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();
  private static File stagingDir, clusterJar;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // prepare the jars to be deployed
    stagingDir = tempFolder.newFolder("staging");
    clusterJar = new File(stagingDir, "cluster.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(clusterJar, "Class1");
  }

  @Test
  public void newLocatorCanReadOldConfigurationData() throws IOException {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    int httpPort = ports[0];
    int locatorPort = ports[1];
    int jmxPort = ports[2];
    GfshExecution execute =
        GfshScript.of(startLocatorCommand("test", locatorPort, jmxPort, httpPort, 0))
            .and("deploy --jar=" + clusterJar.getAbsolutePath())
            .and("shutdown --include-locators")
            .execute(oldGfsh);

    // use the latest gfsh to start the locator in the same working dir
    GfshScript.of(startLocatorCommand("test", locatorPort, jmxPort, httpPort, 0))
        .execute(gfsh, execute.getWorkingDir());

    ClusterManagementService cms = new ClusterManagementServiceBuilder()
        .setPort(httpPort)
        .build();
    assertManagementListResult(cms.list(new Deployment())).isSuccessful()
        .hasConfigurations().hasSize(1);
  }
}
