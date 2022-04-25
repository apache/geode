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
public class ConfigurationCompatibilityTest {
  private final VmConfiguration sourceVmConfiguration;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<VmConfiguration> data() {
    return VmConfigurations.upgrades().stream()
        .filter(hasGeodeVersion(TestVersions.atLeast(TestVersion.valueOf("1.10.0"))))
        .collect(toList());
  }

  public ConfigurationCompatibilityTest(VmConfiguration sourceVmConfiguration) {
    this.sourceVmConfiguration = sourceVmConfiguration;
  }

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Test
  public void whenConfigurationIsExchangedBetweenMixedVersionLocatorsThenItShouldNotThrowExceptions()
      throws Exception {
    MemberVM locator1 = clusterStartupRule.startLocatorVM(0, sourceVmConfiguration);
    int locatorPort1 = locator1.getPort();
    MemberVM locator2 = clusterStartupRule
        .startLocatorVM(1, 0, sourceVmConfiguration, l -> l.withConnectionToLocator(locatorPort1));
    int locatorPort2 = locator2.getPort();

    gfsh.connect(locator1);
    gfsh.executeAndAssertThat("configure pdx --read-serialized=true --disk-store=DEFAULT")
        .statusIsSuccess();

    clusterStartupRule.startServerVM(2, sourceVmConfiguration,
        s -> s.withConnectionToLocator(locatorPort1, locatorPort2).withRegion(
            RegionShortcut.PARTITION, "region"));
    clusterStartupRule.startServerVM(3, sourceVmConfiguration,
        s -> s.withConnectionToLocator(locatorPort1, locatorPort2)
            .withRegion(RegionShortcut.PARTITION, "region"));

    clusterStartupRule.stop(0);
    locator1 = clusterStartupRule.startLocatorVM(0, l -> l.withConnectionToLocator(locatorPort2));
    int newLocatorPort1 = locator1.getPort();

    // configure pdx command is executed to trigger a cluster configuration change event.
    gfsh.disconnect();
    gfsh.connect(locator1);
    gfsh.executeAndAssertThat("configure pdx --read-serialized=true --disk-store=DEFAULT")
        .statusIsSuccess();

    clusterStartupRule.stop(1);
    locator2 =
        clusterStartupRule.startLocatorVM(1, l -> l.withConnectionToLocator(newLocatorPort1));
    int newLocatorPort2 = locator2.getPort();

    // configure pdx command is executed to trigger a cluster configuration change event.
    gfsh.disconnect();
    gfsh.connect(locator2);
    gfsh.executeAndAssertThat("configure pdx --read-serialized=true --disk-store=DEFAULT")
        .statusIsSuccess();

    clusterStartupRule.stop(2);
    clusterStartupRule.startServerVM(2,
        s -> s.withConnectionToLocator(newLocatorPort1, newLocatorPort2)
            .withRegion(RegionShortcut.PARTITION, "region"));

    clusterStartupRule.stop(3);
    clusterStartupRule.startServerVM(3,
        s -> s.withConnectionToLocator(newLocatorPort1, newLocatorPort2)
            .withRegion(RegionShortcut.PARTITION, "region"));

  }
}
