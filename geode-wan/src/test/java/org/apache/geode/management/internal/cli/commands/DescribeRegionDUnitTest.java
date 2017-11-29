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

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class DescribeRegionDUnitTest {
  private static final String REGION1 = "region1";
  private static final String REGION2 = "region2";
  private static final String REGION3 = "region3";
  private static final String SUBREGION1A = "subregion1A";
  private static final String SUBREGION1B = "subregion1B";
  private static final String SUBREGION1C = "subregion1C";
  private static final String PR1 = "PR1";
  private static final String LOCAL_REGION = "LocalRegion";

  private static final String PART1_NAME = "Par1";
  private static final String PART2_NAME = "Par2";

  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void setupSystem() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    MemberVM sending_locator = lsRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + sending_locator.getPort() + "]");
    lsRule.startLocatorVM(2, props);

    MemberVM server1 = lsRule.startServerVM(3, "group1", sending_locator.getPort());
    MemberVM server2 = lsRule.startServerVM(4, "group2", sending_locator.getPort());

    configureServers(server1, server2);

    gfsh.connectAndVerify(sending_locator);
    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 "
        + "--listener=org.apache.geode.internal.cache.wan.MyAsyncEventListener").statusIsSuccess();
    gfsh.executeAndAssertThat("create gateway-sender --id=sender1 --remote-distributed-system-id=2")
        .statusIsSuccess();
    sending_locator.waitTillAsyncEventQueuesAreReadyOnServers("queue1", 1);
    sending_locator.waitTilGatewaySendersAreReady(2);

    gfsh.executeAndAssertThat(
        "create region --name=region4 --type=REPLICATE --async-event-queue-id=queue1 --gateway-sender-id=sender1")
        .statusIsSuccess();

  }

  @SuppressWarnings("deprecation")
  private static void configureServers(MemberVM server1, MemberVM server2) {
    server1.invoke(() -> {
      final Cache cache = LocatorServerStartupRule.getCache();
      RegionFactory<String, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      dataRegionFactory.setConcurrencyLevel(4);
      EvictionAttributes ea =
          EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
      dataRegionFactory.setEvictionAttributes(ea);
      dataRegionFactory.setEnableAsyncConflation(true);

      FixedPartitionAttributes fpa =
          FixedPartitionAttributes.createFixedPartition(PART1_NAME, true);
      PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(100)
          .setRecoveryDelay(2).setTotalMaxMemory(200).setRedundantCopies(1)
          .addFixedPartitionAttributes(fpa).create();
      dataRegionFactory.setPartitionAttributes(pa);

      dataRegionFactory.create(PR1);
      createLocalRegion(LOCAL_REGION);
    });

    server2.invoke(() -> {
      final Cache cache = LocatorServerStartupRule.getCache();
      RegionFactory<String, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      dataRegionFactory.setConcurrencyLevel(4);
      EvictionAttributes ea =
          EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
      dataRegionFactory.setEvictionAttributes(ea);
      dataRegionFactory.setEnableAsyncConflation(true);

      FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition(PART2_NAME, 4);
      PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(150)
          .setRecoveryDelay(4).setTotalMaxMemory(200).setRedundantCopies(1)
          .addFixedPartitionAttributes(fpa).create();
      dataRegionFactory.setPartitionAttributes(pa);

      dataRegionFactory.create(PR1);
      createRegionsWithSubRegions();
    });
  }

  @Test
  public void describeRegionWithGatewayAndAsyncEventQueue() throws Exception {
    gfsh.executeAndAssertThat("describe region --name=region4").statusIsSuccess()
        .containsOutput("gateway-sender-id", "sender1", "async-event-queue-id", "queue1");
  }

  private static void createLocalRegion(final String regionName) {
    final Cache cache = CacheFactory.getAnyInstance();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.LOCAL);
    dataRegionFactory.create(regionName);
  }

  @SuppressWarnings("deprecation")
  private static void createRegionsWithSubRegions() {
    final Cache cache = CacheFactory.getAnyInstance();

    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    dataRegionFactory.setConcurrencyLevel(3);
    Region<String, Integer> region1 = dataRegionFactory.create(REGION1);
    region1.createSubregion(SUBREGION1C, region1.getAttributes());
    Region<String, Integer> subregion2 =
        region1.createSubregion(SUBREGION1A, region1.getAttributes());

    subregion2.createSubregion(SUBREGION1B, subregion2.getAttributes());
    dataRegionFactory.create(REGION2);
    dataRegionFactory.create(REGION3);
  }
}
