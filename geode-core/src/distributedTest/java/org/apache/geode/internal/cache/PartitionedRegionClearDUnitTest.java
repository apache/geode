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

import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getClientCache;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;


public class PartitionedRegionClearDUnitTest implements Serializable {
  protected static final String REGION_NAME = "testPR";
  protected static final int NUM_ENTRIES = 1000;

  protected int locatorPort;
  protected MemberVM locator;
  protected MemberVM dataStore1, dataStore2, accessor;
  protected ClientVM client1, client2;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(6);

  @Before
  public void setUp() throws Exception {
    locator = cluster.startLocatorVM(0);
    locatorPort = locator.getPort();
    dataStore1 = cluster.startServerVM(1, locatorPort);
    dataStore2 = cluster.startServerVM(2, locatorPort);
    accessor = cluster.startServerVM(3, locatorPort);
    client1 = cluster.startClientVM(4,
        c -> c.withPoolSubscription(true).withLocatorConnection((locatorPort)));
    client2 = cluster.startClientVM(5,
        c -> c.withPoolSubscription(true).withLocatorConnection((locatorPort)));
    dataStore1.invoke(this::initDataStore);
    dataStore2.invoke(this::initDataStore);
    accessor.invoke(this::initAccessor);
    client1.invoke(this::initClientCache);
    client2.invoke(this::initClientCache);
  }

  protected RegionShortcut getRegionShortCut() {
    return RegionShortcut.PARTITION_REDUNDANT;
  }

  private Region getRegion(boolean isClient) {
    if (isClient) {
      return getClientCache().getRegion(REGION_NAME);
    } else {
      return getCache().getRegion(REGION_NAME);
    }
  }

  private void verifyRegionSize(boolean isClient, int expectedNum) {
    assertThat(getRegion(isClient).size()).isEqualTo(expectedNum);
  }

  private void initClientCache() {
    Region region = getClientCache().createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .create(REGION_NAME);
    region.registerInterest("ALL_KEYS");
  }

  private void initDataStore() {
    getCache().createRegionFactory(getRegionShortCut())
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(10).create())
        .create(REGION_NAME);
  }

  private void initAccessor() {
    getCache().createRegionFactory(getRegionShortCut())
        .setPartitionAttributes(
            new PartitionAttributesFactory().setTotalNumBuckets(10).setLocalMaxMemory(0).create())
        .create(REGION_NAME);
  }

  private void feed(boolean isClient) {
    Region region = getRegion(isClient);
    IntStream.range(0, NUM_ENTRIES).forEach(i -> region.put(i, "value" + i));
  }

  @Test
  public void normalClearFromDataStore() {
    accessor.invoke(() -> feed(false));
    dataStore1.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));
    dataStore2.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));

    dataStore1.invoke(() -> getRegion(false).clear());
    dataStore1.invoke(() -> verifyRegionSize(false, 0));
    dataStore2.invoke(() -> verifyRegionSize(false, 0));
  }

  @Test
  public void normalClearFromAccessor() {
    accessor.invoke(() -> feed(false));
    dataStore1.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));
    dataStore2.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));

    accessor.invoke(() -> getRegion(false).clear());
    dataStore1.invoke(() -> verifyRegionSize(false, 0));
    dataStore2.invoke(() -> verifyRegionSize(false, 0));
  }

  @Test
  public void normalClearFromClient() {
    client1.invoke(() -> feed(true));
    client2.invoke(() -> verifyRegionSize(true, NUM_ENTRIES));
    dataStore1.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));
    dataStore2.invoke(() -> verifyRegionSize(false, NUM_ENTRIES));

    client1.invoke(() -> getRegion(true).clear());
    dataStore1.invoke(() -> verifyRegionSize(false, 0));
    dataStore2.invoke(() -> verifyRegionSize(false, 0));
    client1.invoke(() -> verifyRegionSize(true, 0));
    client2.invoke(() -> verifyRegionSize(true, 0));
  }
}
