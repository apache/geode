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
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Tests the {@link CacheStatistics} that are maintained by a partitioned {@link Region} .
 *
 *
 * @since GemFire 3.0
 */

public class CacheStatisticsPartitionedRegionDUnitTest extends JUnit4CacheTestCase {

  public CacheStatisticsPartitionedRegionDUnitTest() {
    super();
  }

  //////// Helper Methods

  /**
   * Asserts that two <code>long</code>s are equal concerning a delta.
   */
  public static void assertInRange(long start, long end, long actual) {
    assertTrue("Expected: " + actual + " >= " + start, actual >= start);
    assertTrue("Expected: " + actual + " <= " + end, actual <= end);
  }

  //////// Test methods

  /**
   * Tests that the {@link CacheStatistics#getHitCount hit count} and
   * {@link CacheStatistics#getMissCount miss count} are updated properly for a partitioned region
   * with only one server.
   */
  @Test
  public void testHitMissCount() throws CacheException {
    String name = this.getUniqueName();
    Object key = "KEY"; // value exists
    Object key2 = "KEY2"; // no entry
    Object key3 = "KEY3"; // entry, invalid
    Object value = "VALUE";

    final int MSECS = 1000;

    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(true);

    Region region = createPartitionedRegion(name, factory.create());
    CacheStatistics rStats = region.getStatistics();
    assertEquals(0, rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    assertNull(region.get(key));
    GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
        .until(() -> 1 == rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    assertNull(region.get(key));
    GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
        .until(() -> 2 == rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    rStats.resetCounts();
    GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
        .until(() -> 0 == rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    region.put(key, value);
    GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
        .until(() -> 0 == rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());

    region.get(key);
    GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
        .until(() -> 1 == rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());
    assertEquals(1.0f, rStats.getHitRatio(), 0.0f);

    region.get(key2);
    GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
        .until(() -> 1 == rStats.getMissCount());
    assertEquals(1, rStats.getHitCount());
    assertEquals(0.5f, rStats.getHitRatio(), 0.0f);

    region.create(key3, null);
    region.get(key3); // miss on existing entry
    GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
        .until(() -> 2 == rStats.getMissCount());
    assertEquals(1, rStats.getHitCount());
    assertEquals(2, rStats.getMissCount());
    assertEquals(0.33f, rStats.getHitRatio(), 0.01f);

    rStats.resetCounts();
    GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
        .until(() -> 0 == rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    region.invalidate(key);
    region.get(key);

    GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
        .until(() -> 1 == rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(1, rStats.getMissCount());
  }

  /**
   * Tests that the {@linkplain CacheStatistics#getLastAccessedTime last access time} and
   * {@link CacheStatistics#getLastModifiedTime last modified time} are updated appropriately for a
   * partitioned region with only one server.
   */
  @Test
  public void testTimeStats() throws CacheException, InterruptedException {
    final long ESTAT_RES = 100; // the resolution, in ms, of entry stats
    String name = this.getUniqueName();
    Object key = "KEY";
    Object key2 = "KEY2";
    Object value = "VALUE";
    long before;
    long after;
    long oldBefore;
    long oldAfter;

    final int MSECS = 1000;

    AttributesFactory factory = new AttributesFactory();
    // factory.setScope(Scope.LOCAL);
    factory.setStatisticsEnabled(true);

    Region region = createPartitionedRegion(name, factory.create());
    CacheStatistics rStats = region.getStatistics();

    before = ((GemFireCacheImpl) getCache()).cacheTimeMillis();
    // In order for the stats to have a value, an entry must be accessed first.
    region.get(key);
    after = ((GemFireCacheImpl) getCache()).cacheTimeMillis();

    {
      final long localBefore = before;
      final long localAfter = after;
      GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
          .until(() -> (localBefore <= rStats.getLastAccessedTime()
              && localAfter >= rStats.getLastAccessedTime()));
    }
    assertInRange(before, after, rStats.getLastModifiedTime());

    oldBefore = before;
    oldAfter = after;
    before = ((GemFireCacheImpl) getCache()).cacheTimeMillis();
    region.get(key);
    after = ((GemFireCacheImpl) getCache()).cacheTimeMillis();

    {
      final long localBefore = before;
      final long localAfter = after;
      GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
          .until(() -> localBefore <= rStats.getLastAccessedTime()
              && localAfter >= rStats.getLastAccessedTime());
    }
    assertInRange(oldBefore, oldAfter, rStats.getLastModifiedTime());

    before = ((GemFireCacheImpl) getCache()).cacheTimeMillis();
    region.put(key, value);
    CacheStatistics eStats = region.getEntry(key).getStatistics();
    after = ((GemFireCacheImpl) getCache()).cacheTimeMillis();
    {
      final long localBefore = before;
      final long localAfter = after;
      GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
          .until(() -> localBefore <= rStats.getLastModifiedTime()
              && localAfter >= rStats.getLastModifiedTime());
    }
    assertInRange(before, after, rStats.getLastAccessedTime());

    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats.getLastAccessedTime());
    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats.getLastModifiedTime());

    oldBefore = before;
    oldAfter = after;
    before = ((GemFireCacheImpl) getCache()).cacheTimeMillis();
    region.get(key);
    // eStats must be obtained again. The previous object is not updated.
    // Seems it just provides a snapshot at the moment it is received.
    eStats = region.getEntry(key).getStatistics();
    after = ((GemFireCacheImpl) getCache()).cacheTimeMillis();
    {
      final long localBefore = before;
      final long localAfter = after;
      GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
          .until(() -> localBefore <= rStats.getLastAccessedTime()
              && localAfter >= rStats.getLastAccessedTime());
    }
    assertInRange(oldBefore, oldAfter, rStats.getLastModifiedTime());
    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats.getLastAccessedTime());
    assertInRange(oldBefore - ESTAT_RES, oldAfter + ESTAT_RES, eStats.getLastModifiedTime());

    long oldOldBefore = oldBefore;
    long oldOldAfter = oldAfter;
    oldBefore = before;
    oldAfter = after;
    before = ((GemFireCacheImpl) getCache()).cacheTimeMillis();
    region.create(key2, null);
    CacheStatistics eStats2 = region.getEntry(key2).getStatistics();
    after = ((GemFireCacheImpl) getCache()).cacheTimeMillis();
    {
      final long localBefore = before;
      final long localAfter = after;
      GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
          .until(() -> localBefore <= rStats.getLastModifiedTime()
              && localAfter >= rStats.getLastModifiedTime());
      assertInRange(before, after, rStats.getLastAccessedTime());
    }

    assertInRange(oldBefore - ESTAT_RES, oldAfter + ESTAT_RES, eStats.getLastAccessedTime());
    assertInRange(oldOldBefore - ESTAT_RES, oldOldAfter + ESTAT_RES, eStats.getLastModifiedTime());

    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats2.getLastAccessedTime());
    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats2.getLastModifiedTime());

    // Invalidation does not update the modification/access
    // times
    oldBefore = before;
    oldAfter = after;
    region.invalidate(key2);
    after = ((GemFireCacheImpl) getCache()).cacheTimeMillis();

    {
      final long localOldBefore = oldBefore;
      final long localOldAfter = oldAfter;

      GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
          .until(() -> localOldBefore <= rStats.getLastModifiedTime()
              && localOldAfter >= rStats.getLastModifiedTime());
    }
    assertInRange(oldBefore - ESTAT_RES, oldAfter + ESTAT_RES, eStats2.getLastAccessedTime());
    assertInRange(oldBefore - ESTAT_RES, oldAfter + ESTAT_RES, eStats2.getLastModifiedTime());
    assertInRange(oldBefore, oldAfter, rStats.getLastAccessedTime());

    region.destroy(key2);
    after = ((GemFireCacheImpl) getCache()).cacheTimeMillis();

    {
      final long localOldBefore = oldBefore;
      final long localOldAfter = oldAfter;
      GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
          .until(() -> localOldBefore <= rStats.getLastModifiedTime()
              && localOldAfter >= rStats.getLastModifiedTime());
    }
    assertInRange(oldBefore, oldAfter, rStats.getLastAccessedTime());
  }

  /** The last time an entry was accessed */
  protected static volatile long lastAccessed;
  protected static volatile long lastModified;


  /**
   * Tests that the {@link CacheStatistics#getHitCount hit count},
   * {@link CacheStatistics#getMissCount miss count},
   * {@linkplain CacheStatistics#getLastAccessedTime last access time}
   * and {@link CacheStatistics#getLastModifiedTime last modified time} are updated
   * properly for a partitioned region
   * with more than one server.
   */
  @Test
  public void testDistributedStats() {
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object key1 = "KEY1";
    final Object key2 = "KEY2";
    final Object key3 = "KEY3";
    final Object value = "VALUE";
    final Object value1 = "VALUE1";
    final Object value2 = "VALUE2";
    final Object value3 = "VALUE3";
    final Object notPresentKey1 = "NOT_PRESENT_1";
    final Object notPresentKey2 = "NOT_PRESENT_2";
    final Object notPresentKey3 = "NOT_PRESENT_3";
    final Object notPresentKey4 = "NOT_PRESENT_4";

    final int MSECS = 1000;

    SerializableRunnable create = new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setEarlyAck(false);
        factory.setStatisticsEnabled(true);
        createPartitionedRegion(name, factory.create());
      }
    };

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Define entry") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        region.put(key, value);
        region.put(key1, value1);

        GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
            .until(() -> stats.getLastModifiedTime() > lastModified);
        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
      }
    });

    waitForOp();

    vm1.invoke(new CacheSerializableRunnable("Define entry") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        region.put(key2, value3);
        region.put(key3, value3);

        GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
            .until(() -> stats.getLastModifiedTime() > lastModified);
        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());

      }
    });

    vm0.invoke(new CacheSerializableRunnable("Net search") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        lastAccessed = stats.getLastAccessedTime();
        lastModified = stats.getLastModifiedTime();

        Object result = region.get(key);
        region.get(key1);
        region.get(key2);
        region.get(key3);
        region.get(notPresentKey1);
        region.get(notPresentKey2);
        region.get(notPresentKey3);
        region.get(notPresentKey4);

        assertEquals(value, result);
        GeodeAwaitility.await().atMost(MSECS * 1000, TimeUnit.MILLISECONDS)
            .until(() -> 2 == stats.getMissCount());
        assertEquals(2, stats.getHitCount());
        assertTrue(stats.getLastAccessedTime() > lastAccessed);
      }
    });

    waitForOp();

    vm1.invoke(new CacheSerializableRunnable("Net search") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        lastAccessed = stats.getLastAccessedTime();

        Object result = region.get(key);
        region.get(key1);
        region.get(key2);
        region.get(key3);
        region.get(notPresentKey1);
        region.get(notPresentKey2);
        region.get(notPresentKey3);
        region.get(notPresentKey4);

        assertEquals(value, result);
        GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
            .until(() -> 4 == stats.getHitCount());
        assertEquals(4, stats.getMissCount());
        assertTrue(stats.getLastAccessedTime() > lastAccessed);
      }
    });

    waitForOp();

    vm0.invoke(new CacheSerializableRunnable("Update") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        long before = ((GemFireCacheImpl) getCache()).cacheTimeMillis();

        region.put(key, value);
        region.put(key1, value1);
        region.put(key2, value2);
        region.put(key3, value3);
        long after = ((GemFireCacheImpl) getCache()).cacheTimeMillis();

        GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
            .until(() -> before <= stats.getLastModifiedTime()
                && after >= stats.getLastModifiedTime());
      }
    });

    waitForOp();

    vm1.invoke(new CacheSerializableRunnable("Update") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        lastModified = stats.getLastModifiedTime();
        long before = ((GemFireCacheImpl) getCache()).cacheTimeMillis();
        region.put(key, value);
        region.put(key1, value1);
        region.put(key2, value2);
        region.put(key3, value3);
        long after = ((GemFireCacheImpl) getCache()).cacheTimeMillis();

        GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
            .until(() -> before <= stats.getLastModifiedTime()
                && after >= stats.getLastModifiedTime());
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Invalidate") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        lastAccessed = stats.getLastAccessedTime();
        lastModified = stats.getLastModifiedTime();
        region.invalidate(key);
        region.invalidate(key1);

        GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
            .until(() -> stats.getLastAccessedTime() == lastAccessed);
        assertEquals(lastModified, stats.getLastModifiedTime());
        assertEquals(4, stats.getHitCount());
        assertEquals(4, stats.getMissCount());
      }
    });

    waitForOp();

    vm1.invoke(new CacheSerializableRunnable("Invalidate") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        lastAccessed = stats.getLastAccessedTime();
        lastModified = stats.getLastModifiedTime();
        region.invalidate(key2);
        region.invalidate(key3);

        GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
            .until(() -> stats.getLastAccessedTime() == lastAccessed);
        assertEquals(lastModified, stats.getLastModifiedTime());
        assertEquals(4, stats.getHitCount());
        assertEquals(4, stats.getMissCount());
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Destroy Entry") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        lastModified = stats.getLastModifiedTime();
        region.destroy(key);
        region.destroy(key1);

        GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
            .until(() -> stats.getLastModifiedTime() == lastModified);
        assertEquals(4, stats.getHitCount());
        assertEquals(4, stats.getMissCount());
      }
    });

    waitForOp();

    vm1.invoke(new CacheSerializableRunnable("Destroy Entry") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        CacheStatistics stats = region.getStatistics();
        lastModified = stats.getLastModifiedTime();
        region.destroy(key2);
        region.destroy(key3);

        GeodeAwaitility.await().atMost(MSECS, TimeUnit.MILLISECONDS)
            .until(() -> stats.getLastModifiedTime() == lastModified);
        assertEquals(4, stats.getHitCount());
        assertEquals(4, stats.getMissCount());
      }
    });

  }

  /**
   * Tests that an attempt to get statistics when they are disabled results in a
   * {@link StatisticsDisabledException}.
   */
  @Test
  public void testDisabledStatistics() throws CacheException {
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";

    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(false);
    Region region = createPartitionedRegion(name, factory.create());
    try {
      region.getStatistics();
      fail("Should have thrown a StatisticsDisabledException");

    } catch (StatisticsDisabledException ex) {
      // pass...
    }

    region.put(key, value);
    Region.Entry entry = region.getEntry(key);

    try {
      region.getStatistics();

    } catch (StatisticsDisabledException ex) {
      // pass...
    }
  }

  private void waitForOp() {
    long OPS_WAIT_MSECS = 100;
    try {
      Thread.sleep(OPS_WAIT_MSECS);
    } catch (InterruptedException ex) {
      fail("interrupted");
    }
  }
}
