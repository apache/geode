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
package org.apache.geode.internal.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.OutOfOffHeapMemoryException;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.statistics.LocalStatisticsFactory;
import org.apache.geode.util.internal.GeodeGlossary;

public class OffHeapStorageJUnitTest {

  private static final long MEGABYTE = 1024 * 1024;
  private static final long GIGABYTE = 1024 * 1024 * 1024;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void testParseOffHeapMemorySizeNegative() {
    assertEquals(0, OffHeapStorage.parseOffHeapMemorySize("-1"));
  }

  @Test
  public void testParseOffHeapMemorySizeNull() {
    assertEquals(0, OffHeapStorage.parseOffHeapMemorySize(null));
  }

  @Test
  public void testParseOffHeapMemorySizeEmpty() {
    assertEquals(0, OffHeapStorage.parseOffHeapMemorySize(""));
  }

  @Test
  public void testParseOffHeapMemorySizeBytes() {
    assertEquals(MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("1"));
    assertEquals(Integer.MAX_VALUE * MEGABYTE,
        OffHeapStorage.parseOffHeapMemorySize("" + Integer.MAX_VALUE));
  }

  @Test
  public void testParseOffHeapMemorySizeKiloBytes() {
    try {
      OffHeapStorage.parseOffHeapMemorySize("1k");
      fail("Did not receive expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected
    }
  }

  @Test
  public void testParseOffHeapMemorySizeMegaBytes() {
    assertEquals(MEGABYTE, OffHeapStorage.parseOffHeapMemorySize("1m"));
    assertEquals(Integer.MAX_VALUE * MEGABYTE,
        OffHeapStorage.parseOffHeapMemorySize("" + Integer.MAX_VALUE + "m"));
  }

  @Test
  public void testParseOffHeapMemorySizeGigaBytes() {
    assertEquals(GIGABYTE, OffHeapStorage.parseOffHeapMemorySize("1g"));
    assertEquals(Integer.MAX_VALUE * GIGABYTE,
        OffHeapStorage.parseOffHeapMemorySize("" + Integer.MAX_VALUE + "g"));
  }

  @Test
  public void testCalcMaxSlabSize() {
    assertEquals(100, OffHeapStorage.calcMaxSlabSize(100L));
    assertEquals(Integer.MAX_VALUE, OffHeapStorage.calcMaxSlabSize(Long.MAX_VALUE));
    try {
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_SLAB_SIZE", "99");
      assertEquals(99 * 1024 * 1024, OffHeapStorage.calcMaxSlabSize(100L * 1024 * 1024));
      assertEquals(88, OffHeapStorage.calcMaxSlabSize(88));
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_SLAB_SIZE", "88m");
      assertEquals(88 * 1024 * 1024, OffHeapStorage.calcMaxSlabSize(100L * 1024 * 1024));
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_SLAB_SIZE", "77M");
      assertEquals(77 * 1024 * 1024, OffHeapStorage.calcMaxSlabSize(100L * 1024 * 1024));
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_SLAB_SIZE", "1g");
      assertEquals(1 * 1024 * 1024 * 1024, OffHeapStorage.calcMaxSlabSize(2L * 1024 * 1024 * 1024));
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_SLAB_SIZE", "1G");
      assertEquals(1L * 1024 * 1024 * 1024,
          OffHeapStorage.calcMaxSlabSize(2L * 1024 * 1024 * 1024 + 1));
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_SLAB_SIZE", "foobarG");
      try {
        OffHeapStorage.calcMaxSlabSize(100);
        fail("expected IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
      }
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_SLAB_SIZE", "");
      assertEquals(100, OffHeapStorage.calcMaxSlabSize(100L));
      assertEquals(Integer.MAX_VALUE, OffHeapStorage.calcMaxSlabSize(Long.MAX_VALUE));
    } finally {
      System.clearProperty(GeodeGlossary.GEMFIRE_PREFIX + "OFF_HEAP_SLAB_SIZE");
    }
  }

  @Test
  public void createOffHeapStorageReturnsNullIfForceLocator() {
    System.setProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE, "true");
    assertEquals(null, OffHeapStorage.createOffHeapStorage(null, 1, null));
  }

  @Test
  public void createOffHeapStorageReturnsNullIfMemorySizeIsZero() {
    assertEquals(null, OffHeapStorage.createOffHeapStorage(null, 0, null));
  }

  @Test
  public void exceptionIfSlabCountTooSmall() {
    StatisticsFactory statsFactory = mock(StatisticsFactory.class);
    try {
      OffHeapStorage.createOffHeapStorage(statsFactory, OffHeapStorage.MIN_SLAB_SIZE - 1, null);
    } catch (IllegalArgumentException expected) {
      expected.getMessage()
          .equals("The amount of off heap memory must be at least " + OffHeapStorage.MIN_SLAB_SIZE
              + " but it was set to " + (OffHeapStorage.MIN_SLAB_SIZE - 1));
    }
  }

  @Test
  public void exceptionIfDistributedSystemNull() {
    StatisticsFactory statsFactory = mock(StatisticsFactory.class);
    try {
      OffHeapStorage.createOffHeapStorage(statsFactory, OffHeapStorage.MIN_SLAB_SIZE,
          null);
    } catch (IllegalArgumentException expected) {
      expected.getMessage().equals("InternalDistributedSystem is null");
    }
  }

  @Test
  public void createOffHeapStorageWorks() {
    StatisticsFactory localStatsFactory = new LocalStatisticsFactory(null);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    MemoryAllocator ma =
        OffHeapStorage.createOffHeapStorage(localStatsFactory, OffHeapStorage.MIN_SLAB_SIZE, ids);
    System.setProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "true");
    ma.close();
  }

  @Test
  public void testCreateOffHeapStorage() {
    StatisticsFactory localStatsFactory = new LocalStatisticsFactory(null);
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    MemoryAllocator ma =
        OffHeapStorage.basicCreateOffHeapStorage(localStatsFactory, 1024 * 1024, ooohml);
    try {
      OffHeapMemoryStats stats = ma.getStats();
      assertNotNull(stats.getStats());
      assertEquals(1024 * 1024, stats.getFreeMemory());
      assertEquals(1024 * 1024, stats.getMaxMemory());
      assertEquals(0, stats.getUsedMemory());
      assertEquals(0, stats.getDefragmentations());
      assertEquals(0, stats.getDefragmentationsInProgress());
      assertEquals(0, stats.getDefragmentationTime());
      assertEquals(0, stats.getFragmentation());
      assertEquals(1, stats.getFragments());
      assertEquals(1024 * 1024, stats.getLargestFragment());
      assertEquals(0, stats.getObjects());
      assertEquals(0, stats.getReads());

      stats.incFreeMemory(100);
      assertEquals(1024 * 1024 + 100, stats.getFreeMemory());
      stats.incFreeMemory(-100);
      assertEquals(1024 * 1024, stats.getFreeMemory());

      stats.incMaxMemory(100);
      assertEquals(1024 * 1024 + 100, stats.getMaxMemory());
      stats.incMaxMemory(-100);
      assertEquals(1024 * 1024, stats.getMaxMemory());

      stats.incUsedMemory(100);
      assertEquals(100, stats.getUsedMemory());
      stats.incUsedMemory(-100);
      assertEquals(0, stats.getUsedMemory());

      stats.incObjects(100);
      assertEquals(100, stats.getObjects());
      stats.incObjects(-100);
      assertEquals(0, stats.getObjects());

      stats.incReads();
      assertEquals(1, stats.getReads());

      stats.setFragmentation(100);
      assertEquals(100, stats.getFragmentation());
      stats.setFragmentation(0);
      assertEquals(0, stats.getFragmentation());

      stats.setFragments(2);
      assertEquals(2, stats.getFragments());
      stats.setFragments(1);
      assertEquals(1, stats.getFragments());

      stats.setLargestFragment(100);
      assertEquals(100, stats.getLargestFragment());
      stats.setLargestFragment(1024 * 1024);
      assertEquals(1024 * 1024, stats.getLargestFragment());

      boolean originalEnableClockStats = DistributionStats.enableClockStats;
      DistributionStats.enableClockStats = true;
      try {
        long start = stats.startDefragmentation();
        assertEquals(1, stats.getDefragmentationsInProgress());
        while (DistributionStats.getStatTime() == start) {
          Thread.yield();
        }
        stats.endDefragmentation(start);
        assertEquals(1, stats.getDefragmentations());
        assertEquals(0, stats.getDefragmentationsInProgress());
        assertTrue(stats.getDefragmentationTime() > 0);
      } finally {
        DistributionStats.enableClockStats = originalEnableClockStats;
      }

      stats.incObjects(100);
      stats.incUsedMemory(100);
      stats.setFragmentation(100);
      OffHeapStorage ohs = (OffHeapStorage) stats;
      ohs.initialize(new NullOffHeapMemoryStats());
      assertEquals(0, stats.getFreeMemory());
      assertEquals(0, stats.getMaxMemory());
      assertEquals(0, stats.getUsedMemory());
      assertEquals(0, stats.getDefragmentations());
      assertEquals(0, stats.getDefragmentationsInProgress());
      assertEquals(0, stats.getDefragmentationTime());
      assertEquals(0, stats.getFragmentation());
      assertEquals(0, stats.getFragments());
      assertEquals(0, stats.getLargestFragment());
      assertEquals(0, stats.getObjects());
      assertEquals(0, stats.getReads());

      OutOfOffHeapMemoryException ex = null;
      try {
        ma.allocate(1024 * 1024 + 1);
        fail("expected OutOfOffHeapMemoryException");
      } catch (OutOfOffHeapMemoryException expected) {
        ex = expected;
      }
      verify(ooohml).outOfOffHeapMemory(ex);
      try {
        ma.allocate(1024 * 1024 + 1);
        fail("expected OutOfOffHeapMemoryException");
      } catch (OutOfOffHeapMemoryException expected) {
        ex = expected;
      }
      verify(ooohml).outOfOffHeapMemory(ex);

    } finally {
      System.setProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "true");
      try {
        ma.close();
      } finally {
        System.clearProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY);
      }
    }
  }

  @Test
  public void testCalcSlabCount() {
    final long MSS = OffHeapStorage.MIN_SLAB_SIZE;
    assertEquals(100, OffHeapStorage.calcSlabCount(MSS * 4, MSS * 4 * 100));
    assertEquals(100, OffHeapStorage.calcSlabCount(MSS * 4, (MSS * 4 * 100) + (MSS - 1)));
    assertEquals(101, OffHeapStorage.calcSlabCount(MSS * 4, (MSS * 4 * 100) + MSS));
    assertEquals(Integer.MAX_VALUE, OffHeapStorage.calcSlabCount(MSS, MSS * Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE,
        OffHeapStorage.calcSlabCount(MSS, (MSS * Integer.MAX_VALUE) + MSS - 1));
    try {
      OffHeapStorage.calcSlabCount(MSS, (MSS * Integer.MAX_VALUE) + MSS);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
  }
}
