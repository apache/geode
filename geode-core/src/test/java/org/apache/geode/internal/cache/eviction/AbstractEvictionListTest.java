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
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbstractEvictionListTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private BucketRegion bucketRegion;
  private InternalEvictionStatistics stats;

  @Before
  public void setup() {
    bucketRegion = mock(BucketRegion.class);
    stats = mock(InternalEvictionStatistics.class);
  }

  @Test
  public void cannotInstantiateWithoutStats() {
    thrown.expect(IllegalArgumentException.class);
    new TestEvictionList(null, bucketRegion);
  }

  @Test
  public void sizeIsZeroByDefault() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);
    assertThat(evictionList.size()).isZero();
  }

  @Test
  public void sizeIncreasesWithAppendEntry() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);

    evictionList.appendEntry(new LinkableEvictionNode());
    assertThat(evictionList.size()).isEqualTo(1);

    evictionList.appendEntry(new LinkableEvictionNode());
    assertThat(evictionList.size()).isEqualTo(2);
  }

  @Test
  public void sizeDecreasedWhenDecremented() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);

    evictionList.appendEntry(new LinkableEvictionNode());
    evictionList.decrementSize();
    assertThat(evictionList.size()).isZero();
  }

  @Test(expected = IllegalStateException.class)
  public void sizeCannotBeNegative() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);
    evictionList.decrementSize();
  }

  @Test
  public void getStatisticsReturnsRightObject() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);
    assertThat(evictionList.getStatistics()).isSameAs(stats);
  }

  @Test
  public void closeStats() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);
    evictionList.closeStats();
    verify(stats).close();
  }

  @Test
  public void clearWithVersionVectorDoesNotChangeStats() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);

    evictionList.appendEntry(new LinkableEvictionNode());
    assertThat(evictionList.size()).isEqualTo(1);
    evictionList.clear(mock(RegionVersionVector.class));
    assertThat(evictionList.size()).isEqualTo(1);
  }

  @Test
  public void clearWithoutBucketRegionResetsStats() throws Exception {
    TestEvictionList noBucketRegionEvictionList = new TestEvictionList(stats, null);

    noBucketRegionEvictionList.appendEntry(new LinkableEvictionNode());
    assertThat(noBucketRegionEvictionList.size()).isEqualTo(1);
    noBucketRegionEvictionList.clear(null);
    verify(stats).resetCounter();
    assertThat(noBucketRegionEvictionList.size()).isZero();
  }

  @Test
  public void clearWithBucketRegionResetsBucketStats() throws Exception {
    long bucketSize = 10L;
    when(bucketRegion.getCounter()).thenReturn(bucketSize);
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);

    evictionList.clear(null);
    verify(bucketRegion).resetCounter();
    verify(stats).decrementCounter(bucketSize);
    assertThat(evictionList.size()).isZero();
  }

  @Test
  public void setBucketRegionWithWrongTypeDoesNothing() throws Exception {
    TestEvictionList noBucketRegionEvictionList = new TestEvictionList(stats, null);

    noBucketRegionEvictionList.appendEntry(new LinkableEvictionNode());
    Region notABucketRegion = mock(Region.class);
    noBucketRegionEvictionList.setBucketRegion(notABucketRegion);
    noBucketRegionEvictionList.clear(null);
    verifyZeroInteractions(notABucketRegion);
  }

  @Test
  public void setBucketRegionWithBucketRegionTest() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, null);
    evictionList.setBucketRegion(bucketRegion);

    evictionList.clear(null);
    verify(bucketRegion).resetCounter();
  }

  @Test
  public void appendEntryAlreadyInListDoesNothing() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);
    EvictionNode node = mock(EvictionNode.class);
    when(node.next()).thenReturn(mock(EvictionNode.class));

    evictionList.appendEntry(node);
    verify(node, never()).unsetRecentlyUsed();
  }

  @Test
  public void appendingNewEntryAddsItToList() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);
    EvictionNode node = mock(EvictionNode.class);
    evictionList.appendEntry(node);

    verify(node).unsetRecentlyUsed();
    verify(node).setNext(evictionList.tail);
    verify(node).setPrevious(evictionList.head);
    assertThat(evictionList.tail.previous()).isSameAs(node);
    assertThat(evictionList.head.next()).isSameAs(node);
    assertThat(evictionList.size()).isEqualTo(1);
  }

  @Test
  public void unlinkEntryNotInListTest() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);
    EvictionNode node = mock(EvictionNode.class);

    evictionList.destroyEntry(node);
    assertThat(evictionList.size()).isEqualTo(0);
  }

  @Test
  public void unlinkEntryInListTest() throws Exception {
    TestEvictionList evictionList = new TestEvictionList(stats, bucketRegion);
    EvictionNode node = mock(EvictionNode.class);
    when(node.next()).thenReturn(evictionList.tail);
    when(node.previous()).thenReturn(evictionList.head);

    evictionList.appendEntry(mock(EvictionNode.class));
    assertThat(evictionList.size()).isEqualTo(1);

    evictionList.destroyEntry(node);
    assertThat(evictionList.size()).isEqualTo(0);
    verify(stats).incDestroys();
  }

  private static class TestEvictionList extends AbstractEvictionList {

    TestEvictionList(InternalEvictionStatistics stats, BucketRegion region) {
      super(stats, region);
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
