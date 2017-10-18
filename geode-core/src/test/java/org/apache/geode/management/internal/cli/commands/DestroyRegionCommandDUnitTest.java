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

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.waitAtMost;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;

@Category(DistributedTest.class)
public class DestroyRegionCommandDUnitTest {
  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  private static MemberVM locator, server1, server2, server3;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, locator.getPort());
    server2 = lsRule.startServerVM(2, locator.getPort());
    server3 = lsRule.startServerVM(3, locator.getPort());
  }

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testDestroyDistributedRegion() {
    MemberVM.invokeInEveryMember(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.PARTITION);
      factory.create("Customer");

      PartitionAttributesFactory paFactory = new PartitionAttributesFactory();
      paFactory.setColocatedWith("Customer");
      factory.setPartitionAttributes(paFactory.create());
      factory.create("Order");
    }, server1, server2);

    locator.invoke(() -> waitForRegionMBeanCreation("/Customer", 2));
    locator.invoke(() -> waitForRegionMBeanCreation("/Order", 2));

    // Test unable to destroy with co-location
    gfsh.executeAndVerifyCommandError("destroy region --name=/Customer",
        "The parent region [/Customer] in colocation chain cannot be destroyed");

    // Test success
    gfsh.executeAndVerifyCommand("destroy region --name=/Order", "destroyed successfully");
    gfsh.executeAndVerifyCommand("destroy region --name=/Customer", "destroyed successfully");

    // destroy something that's not exist anymore
    gfsh.executeAndVerifyCommandError("destroy region --name=/Customer", "Could not find a Region");
    gfsh.executeAndVerifyCommand("destroy region --name=/Customer --if-exists");
  }

  @Test
  public void testDestroyLocalRegions() {
    MemberVM.invokeInEveryMember(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
      factory.setScope(Scope.LOCAL);
      factory.create("Customer");
    }, server1, server2, server3);

    locator.invoke(() -> waitForRegionMBeanCreation("/Customer", 3));

    gfsh.executeAndVerifyCommand("destroy region --name=Customer", "destroyed successfully");

    MemberVM.invokeInEveryMember(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      assertThat(cache.getRegion("Customer")).isNull();
    }, server1, server2, server3);
  }

  @Test
  public void testDestroyLocalAndDistributedRegions() {
    MemberVM.invokeInEveryMember(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.PARTITION);
      factory.create("Customer");
      factory.create("Customer_2");
      factory.create("Customer_3");
    }, server1, server2);

    server3.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
      factory.setScope(Scope.LOCAL);
      factory.create("Customer");
      factory.create("Customer_2");
      factory.create("Customer_3");
    });

    locator.invoke(() -> waitForRegionMBeanCreation("/Customer", 3));
    locator.invoke(() -> waitForRegionMBeanCreation("/Customer_2", 3));
    locator.invoke(() -> waitForRegionMBeanCreation("/Customer_3", 3));

    gfsh.executeAndVerifyCommand("destroy region --name=Customer", "destroyed successfully");
    gfsh.executeAndVerifyCommand("destroy region --name=Customer_2", "destroyed successfully");
    gfsh.executeAndVerifyCommand("destroy region --name=Customer_3", "destroyed successfully");

    MemberVM.invokeInEveryMember(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      assertThat(cache.getRegion("Customer")).isNull();
      assertThat(cache.getRegion("Customer_2")).isNull();
      assertThat(cache.getRegion("Customer_3")).isNull();
    }, server1, server2, server3);
  }

  @Test
  public void testDestroyRegionWithSharedConfig() throws IOException {
    gfsh.executeAndVerifyCommand("create region --name=Customer --type=REPLICATE");

    locator.invoke(() -> {
      // Make sure the region exists in the cluster config
      ClusterConfigurationService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
      assertThat(sharedConfig.getConfiguration("cluster").getCacheXmlContent())
          .contains("Customer");
    });

    // make sure region does exists
    MemberVM.invokeInEveryMember(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      assertThat(cache.getRegion("Customer")).isNotNull();
    }, server1, server2, server3);

    // destroy the region
    gfsh.executeAndVerifyCommand("destroy region --name=Customer", "destroyed successfully");

    // make sure the region was removed from cluster config
    locator.invoke(() -> {
      ClusterConfigurationService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
      assertThat(sharedConfig.getConfiguration("cluster").getCacheXmlContent())
          .doesNotContain("Customer");
    });

    // restart one server to make sure the region does not exist anymore
    lsRule.stopVM(1);
    lsRule.startServerVM(1, locator.getPort());

    // make sure region does not exist
    MemberVM.invokeInEveryMember(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      assertThat(cache.getRegion("Customer")).isNull();
    }, server1, server2, server3);
  }

  private static void waitForRegionMBeanCreation(String regionPath, int mbeanCount) {
    waitAtMost(5, TimeUnit.SECONDS).until(() -> {
      try {
        MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
        String queryExp =
            MessageFormat.format(ManagementConstants.OBJECTNAME__REGION_MXBEAN, regionPath, "*");
        ObjectName queryExpON = new ObjectName(queryExp);
        return mbeanServer.queryNames(queryExpON, null).size() == mbeanCount;
      } catch (MalformedObjectNameException mone) {
        throw new RuntimeException(mone);
      }
    });
  }
}
