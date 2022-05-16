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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.internal.util.ProductVersionUtil;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;
import org.apache.geode.test.version.VmConfiguration;
import org.apache.geode.test.version.VmConfigurations;

@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class GfshCompatibilityTest {
  private final VmConfiguration sourceVmConfiguration;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<VmConfiguration> data() {
    return VmConfigurations.upgrades();
  }

  public GfshCompatibilityTest(VmConfiguration sourceVmConfiguration) {
    this.sourceVmConfiguration = sourceVmConfiguration;;
  }

  private MemberVM oldLocator;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void currentGfshConnectToOlderVersionsOfLocator() throws Exception {
    oldLocator = cluster.startLocatorVM(0, sourceVmConfiguration);
    TestVersion oldVersion = sourceVmConfiguration.geodeVersion();
    int locatorPort = oldLocator.getPort();
    cluster.startServerVM(1, sourceVmConfiguration,
        s -> s.withConnectionToLocator(locatorPort));
    if (oldVersion.lessThan(TestVersion.valueOf("1.5.0"))) {
      gfsh.connect(oldLocator.getPort(), GfshCommandRule.PortType.locator);
      assertThat(gfsh.isConnected()).isFalse();
      assertThat(gfsh.getGfshOutput()).contains("Cannot use a")
          .contains("gfsh client to connect to this cluster.");
    } else if (oldVersion.lessThan(TestVersion.valueOf("1.10.0"))) {
      // pre 1.10, it should not be able to connect
      gfsh.connect(oldLocator.getPort(), GfshCommandRule.PortType.locator);
      assertThat(gfsh.isConnected()).isFalse();
      assertThat(gfsh.getGfshOutput()).contains("Cannot use a")
          .contains("gfsh client to connect to a " + oldVersion + " cluster.");
    } else {
      // post 1.10 (including) should connect and be able to execute command
      gfsh.connectAndVerify(oldLocator);
      String expectedVersion = VersionManager.isCurrentVersion(oldVersion.toString())
          ? ProductVersionUtil.getDistributionVersion().getVersion()
          : oldVersion.toString();
      assertThat(gfsh.getGfshOutput())
          .contains("You are connected to a cluster of version: " + expectedVersion);
      gfsh.executeAndAssertThat("list members")
          .statusIsSuccess();
    }
  }
}
