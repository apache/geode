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
package org.apache.geode.internal.cache.eviction;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.lang.SystemPropertyHelper;
import org.apache.geode.test.junit.categories.EvictionTest;

/**
 * This class tests the LRUCapacityController's core clock algorithm.
 */
@Category({EvictionTest.class})
public class LRUListWithAsyncSortingIntegrationTest {

  @Rule
  public TestName testName = new TestName();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private InternalRegion region;
  private LRUListWithAsyncSorting evictionList;
  private List<EvictionNode> nodes;

  @Before
  public void setUp() throws Exception {
    region = createRegion();
    evictionList = getEvictionList(new TestEvictionController());
    nodes = new ArrayList<>();
    IntStream.range(0, 10).forEach(i -> {
      EvictionNode node = new LRUTestEntry(i);
      nodes.add(node);
      evictionList.appendEntry(node);
    });
  }

  @Test
  public void testAddEvictionList() throws Exception {
    IntStream.range(0, 10).forEach(i -> {
      LRUTestEntry entry = (LRUTestEntry) evictionList.getEvictableEntry();
      assertThat(entry.id()).as("check node ids in order").isEqualTo(i);
    });
    assertThat(evictionList.getEvictableEntry()).as("check list is now empty").isNull();
  }

  @Test
  public void testEvicted() throws Exception {
    actOnEvenNodes(i -> evictionList.destroyEntry(nodes.get(i)));

    actOnOddNodes(i -> {
      LRUTestEntry node = (LRUTestEntry) evictionList.getEvictableEntry();
      assertThat(node.id()).as("check node ids in order").isEqualTo(i);
    });

    assertThat(evictionList.getEvictableEntry()).as("check list is now empty").isNull();
  }

  @Test
  public void testRecentlyUsed() throws Exception {
    actOnEvenNodes(i -> nodes.get(i).setRecentlyUsed(region));

    evictionList.scan();

    actOnOddNodes(i -> {
      LRUTestEntry node = (LRUTestEntry) evictionList.getEvictableEntry();
      assertThat(node.id()).as("check non-recently used entries returned first").isEqualTo(i);
    });

    actOnEvenNodes(i -> {
      LRUTestEntry node = (LRUTestEntry) evictionList.getEvictableEntry();
      assertThat(node.id()).as("check recently used entries returned last").isEqualTo(i);
    });

    assertThat(evictionList.getEvictableEntry()).as("check list is now empty").isNull();
  }

  @Test
  public void testRemoveHead() throws Exception {
    evictionList.destroyEntry(nodes.get(0));
    IntStream.range(1, 10).forEach(i -> {
      LRUTestEntry entry = (LRUTestEntry) evictionList.getEvictableEntry();
      assertThat(entry.id()).as("all but first node should remain").isEqualTo(i);
    });

    assertThat(evictionList.getEvictableEntry()).as("check list is now empty").isNull();
  }

  @Test
  public void testRemoveMiddle() throws Exception {
    evictionList.destroyEntry(nodes.get(5));
    IntStream.range(0, 5).forEach(i -> {
      LRUTestEntry entry = (LRUTestEntry) evictionList.getEvictableEntry();
      assertThat(entry.id()).as("nodes before removed one should remain").isEqualTo(i);
    });

    IntStream.range(6, 10).forEach(i -> {
      LRUTestEntry entry = (LRUTestEntry) evictionList.getEvictableEntry();
      assertThat(entry.id()).as("nodes after removed one should remain").isEqualTo(i);
    });
    assertThat(evictionList.getEvictableEntry()).as("check list is now empty").isNull();
  }

  @Test
  public void testRemoveTail() throws Exception {
    evictionList.destroyEntry(nodes.get(9));
    IntStream.range(0, 9).forEach(i -> {
      LRUTestEntry entry = (LRUTestEntry) evictionList.getEvictableEntry();
      assertThat(entry.id()).as("all but first node should remain").isEqualTo(i);
    });

    assertThat(evictionList.getEvictableEntry()).as("check list is now empty").isNull();
  }

  @Test
  public void doesNotEvictRecentlyUsedEntryIfUnderLimit() throws Exception {
    nodes.get(0).setRecentlyUsed(region);
    assertThat(evictionList.getEvictableEntry()).isSameAs(nodes.get(1));
    assertThat(evictionList.size()).isEqualTo(9);
  }

  @Test
  public void evictsRecentlyUsedNodeIfOverLimit() throws Exception {
    EvictionNode node = new LRUTestEntry(10);
    nodes.add(node);
    evictionList.appendEntry(node);
    IntStream.range(0, 11).forEach(i -> nodes.get(i).setRecentlyUsed(region));
    assertThat(evictionList.getEvictableEntry()).isSameAs(nodes.get(10));
  }

  private LRUListWithAsyncSorting getEvictionList(EvictionController eviction) {
    System.setProperty("geode." + SystemPropertyHelper.EVICTION_SCAN_ASYNC, "true");
    return (LRUListWithAsyncSorting) new EvictionListBuilder(eviction).create();
  }

  private InternalRegion createRegion() throws Exception {
    Cache cache = new CacheFactory().set("locators", "").set("mcast-port", "0").create();
    return (InternalRegion) cache.createRegionFactory(RegionShortcut.PARTITION)
        .create(testName.getMethodName());
  }

  private void actOnEvenNodes(IntConsumer s) {
    IntStream.range(0, 10).filter(i -> i % 2 == 0).forEach(s);
  }

  private void actOnOddNodes(IntConsumer s) {
    IntStream.range(0, 10).filter(i -> i % 2 == 1).forEach(s);
  }
}
