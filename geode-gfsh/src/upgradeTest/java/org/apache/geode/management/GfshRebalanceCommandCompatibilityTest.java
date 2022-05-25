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
import static org.apache.geode.test.version.VmConfigurations.hasGeodeVersion;

import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.TestVersions;
import org.apache.geode.test.version.VmConfiguration;
import org.apache.geode.test.version.VmConfigurations;

@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
public class GfshRebalanceCommandCompatibilityTest {
  private final VmConfiguration sourceVmConfiguration;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<VmConfiguration> data() {
    return VmConfigurations.upgrades().stream()
        .filter(hasGeodeVersion(TestVersions.atLeast(TestVersion.valueOf("1.11.0"))))
        .collect(toList());
  }

  public GfshRebalanceCommandCompatibilityTest(VmConfiguration sourceVmConfiguration) {
    this.sourceVmConfiguration = sourceVmConfiguration;
  }

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void whenCurrentVersionLocatorsExecuteRebalanceOnOldServersThenItMustSucceed()
      throws Exception {
    MemberVM locator1 = cluster.startLocatorVM(0, sourceVmConfiguration);
    int locatorPort1 = locator1.getPort();
    MemberVM locator2 =
        cluster.startLocatorVM(1, 0, sourceVmConfiguration,
            x -> x.withConnectionToLocator(locatorPort1));
    int locatorPort2 = locator2.getPort();
    cluster.startServerVM(2, sourceVmConfiguration,
        s -> s.withRegion(RegionShortcut.PARTITION, "region")
            .withConnectionToLocator(locatorPort1, locatorPort2));
    cluster.startServerVM(3, sourceVmConfiguration,
        s -> s.withRegion(RegionShortcut.PARTITION, "region")
            .withConnectionToLocator(locatorPort1, locatorPort2));
    cluster.stop(0);
    locator1 = cluster.startLocatorVM(0, x -> x.withConnectionToLocator(locatorPort2));
    cluster.stop(1);
    int locatorPort1_v2 = locator1.getPort();
    cluster.startLocatorVM(1, x -> x.withConnectionToLocator(locatorPort1_v2));
    gfsh.connectAndVerify(locator1);
    gfsh.executeAndAssertThat("rebalance ")
        .statusIsSuccess();

  }
}
