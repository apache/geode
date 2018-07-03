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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.versions.RegionVersionVector;

public class AbstractEvictionListTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private BucketRegion bucketRegion;
  private EvictionCounters stats;
  private EvictionController controller;

  @Before
  public void setup() {
    bucketRegion = mock(BucketRegion.class);
    stats = mock(EvictionCounters.class);
    controller = mock(EvictionController.class);
    when(controller.getCounters()).thenReturn(stats);
  }

  @Test
  public void sizeIsZeroByDefault() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    assertThat(evictionList.size()).isZero();
  }

  @Test
  public void sizeIncreasesWithAppendEntry() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);

    evictionList.appendEntry(new LinkableEvictionNode());
    assertThat(evictionList.size()).isEqualTo(1);

    evictionList.appendEntry(new LinkableEvictionNode());
    assertThat(evictionList.size()).isEqualTo(2);
  }

  @Test
  public void sizeDecreasedWhenDecremented() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);

    evictionList.appendEntry(new LinkableEvictionNode());
    evictionList.decrementSize();
    assertThat(evictionList.size()).isZero();
  }

  @Test
  public void getStatisticsReturnsRightObject() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    assertThat(evictionList.getStatistics()).isSameAs(stats);
  }

  @Test
  public void closeStats() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    evictionList.closeStats();
    verify(stats).close();
  }

  @Test
  public void clearWithVersionVectorDoesNotChangeStats() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);

    evictionList.appendEntry(new LinkableEvictionNode());
    assertThat(evictionList.size()).isEqualTo(1);
    evictionList.clear(mock(RegionVersionVector.class), bucketRegion);
    assertThat(evictionList.size()).isEqualTo(1);
  }

  @Test
  public void clearWithoutBucketRegionResetsStats() throws Exception {
    TestEvictionList noBucketRegionEvictionList = new TestEvictionList(controller);

    noBucketRegionEvictionList.appendEntry(new LinkableEvictionNode());
    assertThat(noBucketRegionEvictionList.size()).isEqualTo(1);
    noBucketRegionEvictionList.clear(null, null);
    verify(stats).resetCounter();
    assertThat(noBucketRegionEvictionList.size()).isZero();
  }

  @Test
  public void clearWithBucketRegionResetsBucketStats() throws Exception {
    long bucketSize = 10L;
    when(bucketRegion.getCounter()).thenReturn(bucketSize);
    TestEvictionList evictionList = new TestEvictionList(controller);

    evictionList.clear(null, bucketRegion);
    verify(bucketRegion).resetCounter();
    verify(stats).decrementCounter(bucketSize);
    assertThat(evictionList.size()).isZero();
  }

  @Test
  public void appendEntryAlreadyInListDoesNothing() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    EvictionNode node = mock(EvictionNode.class);
    when(node.next()).thenReturn(mock(EvictionNode.class));

    evictionList.appendEntry(node);
    verify(node, never()).unsetRecentlyUsed();
  }

  @Test
  public void appendingNewEntryAddsItToList() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    EvictionNode node = mock(EvictionNode.class);
    evictionList.appendEntry(node);

    verify(node).setNext(evictionList.tail);
    verify(node).setPrevious(evictionList.head);
    assertThat(evictionList.tail.previous()).isSameAs(node);
    assertThat(evictionList.head.next()).isSameAs(node);
    assertThat(evictionList.size()).isEqualTo(1);
  }

  @Test
  public void unlinkEntryNotInListTest() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    EvictionNode node = mock(EvictionNode.class);

    evictionList.destroyEntry(node);
    assertThat(evictionList.size()).isEqualTo(0);
  }

  @Test
  public void unlinkEntryInListTest() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    EvictionNode node = mock(EvictionNode.class);
    when(node.next()).thenReturn(evictionList.tail);
    when(node.previous()).thenReturn(evictionList.head);

    evictionList.appendEntry(mock(EvictionNode.class));
    assertThat(evictionList.size()).isEqualTo(1);

    evictionList.destroyEntry(node);
    assertThat(evictionList.size()).isEqualTo(0);
    verify(stats).incDestroys();
  }

  @Test
  public void unlinkHeadOnEmptyListReturnsNull() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    assertThat(evictionList.unlinkHeadEntry()).isNull();
  }

  @Test
  public void unlinkTailOnEmptyListReturnsNull() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    assertThat(evictionList.unlinkTailEntry()).isNull();
  }

  @Test
  public void unlinkHeadInListTest() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    EvictionNode node = mock(EvictionNode.class);
    when(node.next()).thenReturn(null, evictionList.tail);
    when(node.previous()).thenReturn(evictionList.head);
    evictionList.appendEntry(node);

    assertThat(evictionList.unlinkHeadEntry()).isSameAs(node);
    assertThat(evictionList.size()).isEqualTo(0);
  }

  @Test
  public void unlinkTailInListTest() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(controller);
    EvictionNode node = mock(EvictionNode.class);
    when(node.next()).thenReturn(null, evictionList.tail);
    when(node.previous()).thenReturn(evictionList.head);
    evictionList.appendEntry(node);

    assertThat(evictionList.unlinkTailEntry()).isSameAs(node);
    assertThat(evictionList.size()).isEqualTo(0);
  }

  @Test
  public void nodeUsedByTransactionIsNotEvictable() {
    TestEvictionList evictionList = new TestEvictionList(controller);
    EvictionNode node = mock(EvictionNode.class);
    when(node.isInUseByTransaction()).thenReturn(true);

    assertThat(evictionList.isEvictable(node)).isFalse();
  }

  @Test
  public void evictedNodeIsNotEvictable() {
    TestEvictionList evictionList = new TestEvictionList(controller);
    EvictionNode node = mock(EvictionNode.class);
    when(node.isEvicted()).thenReturn(true);

    assertThat(evictionList.isEvictable(node)).isFalse();
  }

  @Test
  public void defaultNodeIsEvictable() {
    TestEvictionList evictionList = new TestEvictionList(controller);
    EvictionNode node = mock(EvictionNode.class);

    assertThat(evictionList.isEvictable(node)).isTrue();
  }

  private static class TestEvictionList extends AbstractEvictionList {

    TestEvictionList(EvictionController controller) {
      super(controller);
    }

    @Override
    public EvictableEntry getEvictableEntry() {
      return null;
    }

    @Override
    public void incrementRecentlyUsed() {

    }
  }

}
