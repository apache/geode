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


import static org.apache.geode.test.junit.rules.GfshCommandRule.PortType.jmxManager;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@SuppressWarnings("serial")
public class RebalanceCommandDistributedTestBase {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  protected static MemberVM locator, server1, server2;

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService());

    int locatorPort = locator.getPort();
    server1 = cluster.startServerVM(1, "localhost", locatorPort);
    server2 = cluster.startServerVM(2, "localhost", locatorPort);

    setUpRegions();
  }

  @Before
  public void before() throws Exception {
    gfsh.connect(locator.getJmxPort(), jmxManager);
  }

  @Test
  public void testSimulateForEntireDSWithTimeout() {
    // check if DistributedRegionMXBean is available so that command will not fail
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/region-1", 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/region-2", 1);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/region-3", 1);

    String command = "rebalance --simulate=true --time-out=-1";

    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  private static void setUpRegions() {
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<Integer, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      Region region = dataRegionFactory.create("region-1");
      for (int i = 0; i < 10; i++) {
        region.put("key" + (i + 200), "value" + (i + 200));
      }
      region = dataRegionFactory.create("region-2");
      for (int i = 0; i < 100; i++) {
        region.put("key" + (i + 200), "value" + (i + 200));
      }
    });
    server2.invoke(() -> {
      // no need to close cache as it will be closed as part of teardown2
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<Integer, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      Region region = dataRegionFactory.create("region-1");
      for (int i = 0; i < 100; i++) {
        region.put("key" + (i + 400), "value" + (i + 400));
      }
      region = dataRegionFactory.create("region-3");
      for (int i = 0; i < 10; i++) {
        region.put("key" + (i + 200), "value" + (i + 200));
      }
    });
  }
}
