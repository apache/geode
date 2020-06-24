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

import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert.assertManagementListResult;
import static org.apache.geode.test.junit.rules.gfsh.GfshRule.startLocatorCommand;
import static org.apache.geode.test.junit.rules.gfsh.GfshRule.startServerCommand;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.operation.RebalanceOperation;
import org.apache.geode.management.runtime.RebalanceResult;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
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
public class OperationManagementUpgradeTest {
  private final String oldVersion;
  private VM vm;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(s -> TestVersion.compare(s, "1.12.0") < 0);
    return result;
  }

  public OperationManagementUpgradeTest(String version) {
    oldVersion = version;
    oldGfsh = new GfshRule(oldVersion);
    DUnitLauncher.launchIfNeeded(false);
    // get the vm with the same version of the oldGfsh
    vm = getHost(0).getVM(oldVersion, 0);
  }

  @Rule
  public GfshRule oldGfsh;

  @Rule
  public GfshRule gfsh = new GfshRule();

  @Test
  public void newLocatorCanReadOldConfigurationData()
      throws IOException, ExecutionException, InterruptedException {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(10);
    GfshExecution execute =
        GfshScript.of(startLocatorCommand("locator1", ports[0], ports[1], ports[2], 0))
            .and(startLocatorCommand("locator2", ports[3], ports[4], ports[5], ports[0]))
            .and(startServerCommand("server", ports[6], ports[0]))
            .execute(oldGfsh);

    String operationId = vm.invoke(()->{
      // start a cms client that connects to locator1's http port
      ClusterManagementService cms = new ClusterManagementServiceBuilder()
          .setHost("localhost")
          .setPort(ports[2])
          .build();

      ClusterManagementOperationResult<RebalanceOperation, RebalanceResult> startResult =
          cms.start(new RebalanceOperation());
      assertThat(startResult.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.ACCEPTED);
      return startResult.getOperationId();
    });

    // stop locator1
    oldGfsh.stopLocator(execute, "locator1");
    // use new gfsh to start locator1, make sure new locator can start
    GfshScript.of(startLocatorCommand("locator1", ports[0], ports[1], ports[2], ports[3]))
        .execute(gfsh, execute.getWorkingDir());

    // use the new cms client
    ClusterManagementService cms = new ClusterManagementServiceBuilder()
        .setHost("localhost")
        .setPort(ports[2])
        .build();
    ClusterManagementOperationResult<RebalanceOperation, RebalanceResult>
        operationResult =
        cms.get(new RebalanceOperation(), operationId);
    System.out.println(operationResult);
    assertThat(operationResult.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
  }
}
