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

import static java.nio.file.Files.createDirectory;
import static java.util.stream.Collectors.toList;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert.assertManagementListResult;
import static org.apache.geode.test.version.TestVersions.atLeast;
import static org.apache.geode.test.version.VmConfigurations.hasGeodeVersion;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.FolderRule;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshExecutor;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VmConfiguration;
import org.apache.geode.test.version.VmConfigurations;

@Category(BackwardCompatibilityTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class DeploymentManagementUpgradeTest {

  @Parameters(name = "{0}")
  public static Collection<VmConfiguration> data() {
    return VmConfigurations.upgrades().stream()
        .filter(hasGeodeVersion(atLeast(TestVersion.valueOf("1.10.0"))))
        .collect(toList());
  }

  private static final String HOSTNAME = "localhost";

  private final VmConfiguration sourceVmConfiguration;

  private File clusterJar;
  private GfshExecutor oldGfsh;
  private GfshExecutor gfsh;

  public DeploymentManagementUpgradeTest(VmConfiguration sourceVmConfiguration) {
    this.sourceVmConfiguration = sourceVmConfiguration;
  }

  @Rule(order = 0)
  public FolderRule tempFolder = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule();

  @Before
  public void setUp() throws IOException {
    // prepare the jars to be deployed
    File stagingDir = createDirectory(tempFolder.getFolder().toPath().resolve("staging")).toFile();
    clusterJar = stagingDir.toPath().resolve("cluster.jar").toFile();
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(clusterJar, "Class1");

    oldGfsh = gfshRule.executor()
        .withVmConfiguration(sourceVmConfiguration)
        .build(tempFolder.getFolder().toPath());
    gfsh = gfshRule.executor()
        .build(tempFolder.getFolder().toPath());
  }

  @Test
  public void newLocatorCanReadOldConfigurationData() {
    int[] ports = getRandomAvailableTCPPorts(3);
    int httpPort = ports[0];
    int locatorPort = ports[1];
    int jmxPort = ports[2];
    GfshExecution execute = GfshScript
        .of(startLocatorCommand("test", locatorPort, jmxPort, httpPort, 0))
        .and("deploy --jar=" + clusterJar.getAbsolutePath())
        .and("shutdown --include-locators")
        .execute(oldGfsh);

    // use the latest gfsh to start the locator in the same working dir
    GfshScript
        .of(startLocatorCommand("test", locatorPort, jmxPort, httpPort, 0))
        .execute(gfsh, execute.getWorkingDir());

    ClusterManagementService cms = new ClusterManagementServiceBuilder()
        .setPort(httpPort)
        .build();
    assertManagementListResult(cms.list(new Deployment()))
        .isSuccessful()
        .hasConfigurations()
        .hasSize(1);
  }

  private static String startLocatorCommand(String name, int port, int jmxPort, int httpPort,
      int connectedLocatorPort) {
    String startLocatorCommand =
        "start locator --name=%s --port=%d --http-service-port=%d --J=-Dgemfire.jmx-manager-port=%d";
    if (connectedLocatorPort > 0) {
      return String.format(startLocatorCommand + " --locators=%s[%d]",
          name, port, httpPort, jmxPort, HOSTNAME, connectedLocatorPort);
    }
    return String.format(startLocatorCommand, name, port, httpPort, jmxPort);
  }
}
