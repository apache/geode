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
package org.apache.geode.internal.cache;


import static java.util.stream.Collectors.toList;
import static org.apache.geode.test.version.VmConfigurations.hasGeodeVersion;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.internal.configuration.functions.GetClusterConfigurationFunction;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.TestVersions;
import org.apache.geode.test.version.VmConfiguration;
import org.apache.geode.test.version.VmConfigurations;

@RunWith(Parameterized.class)
public class GetClusterConfigurationFunctionCompatibilityTest {

  private final VmConfiguration sourceConfiguration;

  @Parameterized.Parameters(name = "From {0}")
  public static Collection<VmConfiguration> data() {
    TestVersion minimumGeodeVersion = TestVersion.valueOf("1.12.0");
    return VmConfigurations.upgrades().stream()
        .filter(hasGeodeVersion(TestVersions.atLeast(minimumGeodeVersion)))
        .collect(toList());
  }

  public GetClusterConfigurationFunctionCompatibilityTest(VmConfiguration sourceConfiguration) {
    this.sourceConfiguration = sourceConfiguration;
  }

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  /*
   * The goal of the test is that GetClusterConfigurationFunction can be
   * deserialized in the old versions members
   * Changes to the class can cause the serialVersionUUID to change which
   * can cause the serialization to fail in old members.
   */
  @Test
  public void newLocatorCanGetClusterConfigurationFromOldLocator() {
    // Start locators in old version
    MemberVM locator1 = clusterStartupRule.startLocatorVM(0, sourceConfiguration);
    int locator1Port = locator1.getPort();
    MemberVM locator2 =
        clusterStartupRule.startLocatorVM(1, AvailablePortHelper.getRandomAvailableTCPPort(),
            sourceConfiguration, l -> l.withConnectionToLocator(locator1Port));
    // Roll one locator to the new version
    locator2.stop(false);
    locator2 = clusterStartupRule.startLocatorVM(1, l -> l.withConnectionToLocator(locator1Port));
    // Execute the function from the new locator
    locator2.invoke(() -> {
      Set<InternalDistributedMember> locators =
          ClusterStartupRule.getCache().getDistributionManager().getLocatorDistributionManagerIds();
      locators.forEach(locator -> {
        FunctionService.onMember(locator).setArguments(new HashSet<>())
            .execute(new GetClusterConfigurationFunction());
      });
    });
    /*
     * LogCheckers will detect the deserialization error in the logs and fail the
     * test if it occurs.
     */

  }



}
