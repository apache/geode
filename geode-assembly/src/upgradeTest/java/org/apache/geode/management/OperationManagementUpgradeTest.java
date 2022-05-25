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

import static java.util.stream.Collectors.toList;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.version.TestVersions.greaterThan;
import static org.apache.geode.test.version.VmConfigurations.hasGeodeVersion;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
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
public class OperationManagementUpgradeTest {

  @Parameters(name = "{0}")
  public static Collection<VmConfiguration> data() {
    return VmConfigurations.upgrades().stream()
        .filter(hasGeodeVersion(greaterThan(TestVersion.valueOf("1.13.0"))))
        .collect(toList());
  }

  private static final String HOSTNAME = "localhost";

  private final VmConfiguration sourceVmConfiguration;

  private GfshExecutor currentGfsh;
  private GfshExecutor oldGfsh;
  private VM vm;

  public OperationManagementUpgradeTest(VmConfiguration sourceVmConfiguration) {
    this.sourceVmConfiguration = sourceVmConfiguration;
  }

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule(folderRule::getFolder);
  @Rule(order = 2)
  public DistributedRule distributedRule = new DistributedRule();

  @Before
  public void setUp() {
    currentGfsh = gfshRule.executor().build();
    oldGfsh = gfshRule.executor().withVmConfiguration(sourceVmConfiguration).build();

    // get the vm with the same version of the oldGfsh
    vm = getVM(sourceVmConfiguration, 0);
  }

  @Test
  public void newLocatorCanReadOldConfigurationData() {
    int[] ports = getRandomAvailableTCPPorts(7);
    int locatorPort1 = ports[0];
    int jmxPort1 = ports[1];
    int httpPort1 = ports[2];
    int locatorPort2 = ports[3];
    int jmxPort2 = ports[4];
    int httpPort2 = ports[5];
    int serverPort = ports[6];
    GfshExecution execute = GfshScript
        .of(startLocatorCommand("locator1", locatorPort1, jmxPort1, httpPort1, 0))
        .and(startLocatorCommand("locator2", locatorPort2, jmxPort2, httpPort2, locatorPort1))
        .and(startServerCommand("server", serverPort, locatorPort1))
        .execute(oldGfsh);

    String operationId = vm.invoke(() -> {
      // start a cms client that connects to locator1's http port
      ClusterManagementService cms = new ClusterManagementServiceBuilder()
          .setHost(HOSTNAME)
          .setPort(httpPort1)
          .build();

      ClusterManagementOperationResult<RebalanceOperation, RebalanceResult> startResult =
          cms.start(new RebalanceOperation());
      assertThat(startResult.getStatusCode())
          .isEqualTo(ClusterManagementResult.StatusCode.ACCEPTED);
      return startResult.getOperationId();
    });

    // stop locator1
    execute.locatorStopper().stop("locator1");
    // use new gfsh to start locator1, make sure new locator can start
    GfshScript
        .of(startLocatorCommand("locator1", locatorPort1, jmxPort1, httpPort1, locatorPort2))
        .execute(currentGfsh, execute.getWorkingDir());

    // use the new cms client
    ClusterManagementService cms = new ClusterManagementServiceBuilder()
        .setHost(HOSTNAME)
        .setPort(httpPort1)
        .build();
    ClusterManagementOperationResult<RebalanceOperation, RebalanceResult> operationResult =
        cms.get(new RebalanceOperation(), operationId);
    System.out.println(operationResult);
    assertThat(operationResult.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
  }

  private static String startServerCommand(String name, int port, int connectedLocatorPort) {
    return String.format("start server --name=%s --server-port=%d --locators=%s[%d]",
        name, port, HOSTNAME, connectedLocatorPort);
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
