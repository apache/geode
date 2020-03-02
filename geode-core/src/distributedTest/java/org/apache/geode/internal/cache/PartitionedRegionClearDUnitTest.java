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

import org.apache.geode.cache.Cache;
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
    // client1.invoke(this::initClientCache);
    // client2.invoke(this::initClientCache);
  }

  protected RegionShortcut getRegionShortCut() {
    return RegionShortcut.PARTITION_REDUNDANT;
  }

  private void verifyDataIsLoadedAtServer() {
    Region region = getCache().getRegion(REGION_NAME);
    assertThat(region.size()).isEqualTo(NUM_ENTRIES);
  }

  private void verifyDataIsLoadedAtClient() {
    Region region = getClientCache().getRegion(REGION_NAME);
    assertThat(region.size()).isEqualTo(NUM_ENTRIES);
  }

  private void verifyRegionIsCleared() {
    Region region = getCache().getRegion(REGION_NAME);
    assertThat(region.size()).isEqualTo(0);
  }

  private void initClientCache() {
    Region region = getClientCache().createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .create(REGION_NAME);
    region.registerInterest("ALL_KEYS");
  }

  private void initDataStore() {
    Cache cache = getCache();
    cache.createRegionFactory(getRegionShortCut())
        .setPartitionAttributes(new PartitionAttributesFactory().setTotalNumBuckets(10).create())
        .create(REGION_NAME);
  }

  private void initAccessor() {
    Cache cache = getCache();
    cache.createRegionFactory(getRegionShortCut())
        .setPartitionAttributes(
            new PartitionAttributesFactory().setTotalNumBuckets(10).setLocalMaxMemory(0).create())
        .create(REGION_NAME);
  }

  private void feedFromServer() {
    Cache cache = getCache();
    Region region = cache.getRegion(REGION_NAME);
    IntStream.range(0, NUM_ENTRIES).forEach(i -> region.put(i, "value" + i));
    assertThat(region.size()).isEqualTo(NUM_ENTRIES);
  }

  private void feedFromClient() {
    Region region = getClientCache().getRegion(REGION_NAME);
    IntStream.range(0, NUM_ENTRIES).forEach(i -> region.put(i, "value" + i));
    assertThat(region.size()).isEqualTo(NUM_ENTRIES);
  }

  @Test
  public void normalClearFromDataStore() {
    accessor.invoke(this::feedFromServer);
    dataStore1.invoke(this::verifyDataIsLoadedAtServer);
    dataStore2.invoke(this::verifyDataIsLoadedAtServer);

    dataStore1.invoke(() -> {
      Region region = getCache().getRegion(REGION_NAME);
      assertThat(region.size()).isEqualTo(NUM_ENTRIES);

      region.clear();
      assertThat(region.size()).isEqualTo(0);
    });
    dataStore2.invoke(this::verifyRegionIsCleared);
  }

  @Test
  public void normalClearFromAccessor() {
    accessor.invoke(this::feedFromServer);
    dataStore1.invoke(this::verifyDataIsLoadedAtServer);
    dataStore2.invoke(this::verifyDataIsLoadedAtServer);

    accessor.invoke(() -> {
      Region region = getCache().getRegion(REGION_NAME);
      assertThat(region.size()).isEqualTo(NUM_ENTRIES);
      region.clear();

      assertThat(region.size()).isEqualTo(0);
    });
    dataStore1.invoke(this::verifyRegionIsCleared);
    dataStore2.invoke(this::verifyRegionIsCleared);
  }

  // @Test
  // public void normalClearFromClient() {
  // client1.invoke(this::feedFromClient);
  // client2.invoke(this::verifyDataIsLoadedAtClient);
  // dataStore1.invoke(this::verifyDataIsLoadedAtServer);
  // dataStore2.invoke(this::verifyDataIsLoadedAtServer);
  //
  // client1.invoke(()->{
  // Region region = getClientCache().getRegion(REGION_NAME);
  // assertThat(region.size()).isEqualTo(NUM_ENTRIES);
  // region.clear();
  // assertThat(region.size()).isEqualTo(0);
  // });
  // dataStore1.invoke(this::verifyRegionIsCleared);
  // dataStore2.invoke(this::verifyRegionIsCleared);
  // client2.invoke(()-> {
  // Region region = getClientCache().getRegion(REGION_NAME);
  // assertThat(region.size()).isEqualTo(0);
  // });
  // }
}
