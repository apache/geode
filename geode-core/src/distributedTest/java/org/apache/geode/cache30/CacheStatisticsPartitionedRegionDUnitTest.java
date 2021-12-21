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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.StatisticsDisabledException;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.dunit.SerializableRunnableIF;
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
    String name = getUniqueName();
    Object key = "KEY"; // value exists
    Object key2 = "KEY2"; // no entry
    Object key3 = "KEY3"; // entry, invalid
    Object value = "VALUE";

    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(true);

    Region region = createPartitionedRegion(name, factory.create());
    CacheStatistics rStats = region.getStatistics();
    assertEquals(0, rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    region.get(key);
    assertEquals(1, rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    region.get(key);
    assertEquals(2, rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    rStats.resetCounts();
    assertEquals(0, rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    region.put(key, value);
    assertEquals(0, rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    region.get(key);
    assertEquals(1, rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());
    assertEquals(1.0f, rStats.getHitRatio(), 0.0f);

    region.get(key2);
    assertEquals(1, rStats.getMissCount());
    assertEquals(1, rStats.getHitCount());
    assertEquals(0.5f, rStats.getHitRatio(), 0.0f);

    region.create(key3, null);
    region.get(key3); // miss on existing entry
    assertEquals(2, rStats.getMissCount());
    assertEquals(1, rStats.getHitCount());
    assertEquals(0.33f, rStats.getHitRatio(), 0.01f);

    rStats.resetCounts();
    assertEquals(0, rStats.getMissCount());
    assertEquals(0, rStats.getHitCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    region.invalidate(key);
    region.get(key);

    assertEquals(0, rStats.getHitCount());
    assertEquals(1, rStats.getMissCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);
  }

  /**
   * Tests that the {@linkplain CacheStatistics#getLastAccessedTime last access time} and
   * {@link CacheStatistics#getLastModifiedTime last modified time} are updated appropriately for a
   * partitioned region with only one server.
   */
  @Test
  public void testTimeStats() throws CacheException, InterruptedException {
    final long ESTAT_RES = 100; // the resolution, in ms, of entry stats
    String name = getUniqueName();
    Object key1 = Integer.valueOf(1);
    Object key2 = Integer.valueOf(2);
    Object missingKey = Integer.valueOf(999);
    Object value = "VALUE";
    long before;
    long after;
    long oldBefore;
    long oldAfter;

    AttributesFactory factory = new AttributesFactory();
    // factory.setScope(Scope.LOCAL);
    factory.setStatisticsEnabled(true);

    Region region = createPartitionedRegion(name, factory.create());
    CacheStatistics rStats = region.getStatistics();

    assertEquals(0, rStats.getLastAccessedTime());
    assertEquals(0, rStats.getLastModifiedTime());

    before = ((InternalRegion) region).cacheTimeMillis();
    // When the get is invoked, the lastAccessedTime and lastModified time
    // are updated as a new BucketRegion is created.
    region.get(missingKey);

    after = ((InternalRegion) region).cacheTimeMillis();

    assertInRange(before, after, rStats.getLastAccessedTime());
    assertInRange(before, after, rStats.getLastModifiedTime());

    oldBefore = before;
    oldAfter = after;
    waitForClockToChange(region);
    before = ((InternalRegion) region).cacheTimeMillis();
    region.get(missingKey);

    after = ((InternalRegion) region).cacheTimeMillis();

    assertInRange(before, after, rStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter, rStats.getLastModifiedTime());

    waitForClockToChange(region);
    before = ((InternalRegion) region).cacheTimeMillis();
    region.put(key1, value);
    CacheStatistics eStats = region.getEntry(key1).getStatistics();
    after = ((InternalRegion) region).cacheTimeMillis();

    assertInRange(before, after, rStats.getLastModifiedTime());
    assertInRange(before, after, rStats.getLastAccessedTime());

    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats.getLastAccessedTime());
    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats.getLastModifiedTime());

    oldBefore = before;
    oldAfter = after;
    waitForClockToChange(region);
    before = ((InternalRegion) region).cacheTimeMillis();
    region.get(key1);
    // eStats must be obtained again. The previous object is not updated.
    // Seems it just provides a snapshot at the moment it is received.
    eStats = region.getEntry(key1).getStatistics();
    after = ((InternalRegion) region).cacheTimeMillis();

    assertInRange(before, after, rStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter, rStats.getLastModifiedTime());
    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats.getLastAccessedTime());
    assertInRange(oldBefore - ESTAT_RES, oldAfter + ESTAT_RES, eStats.getLastModifiedTime());

    long oldOldBefore = oldBefore;
    long oldOldAfter = oldAfter;
    oldBefore = before;
    oldAfter = after;
    waitForClockToChange(region, ESTAT_RES);
    before = ((InternalRegion) region).cacheTimeMillis();
    region.create(key2, null);
    region.get(key2);
    CacheStatistics eStats2 = region.getEntry(key2).getStatistics();
    after = ((InternalRegion) region).cacheTimeMillis();

    assertInRange(before, after, rStats.getLastModifiedTime());
    assertInRange(before, after, rStats.getLastAccessedTime());

    assertInRange(oldBefore - ESTAT_RES, oldAfter + ESTAT_RES, eStats.getLastAccessedTime());
    assertInRange(oldOldBefore - ESTAT_RES, oldOldAfter + ESTAT_RES, eStats.getLastModifiedTime());

    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats2.getLastAccessedTime());
    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats2.getLastModifiedTime());

    // Invalidation does not update the modification/access
    // times
    region.invalidate(key2);

    assertInRange(before, after, rStats.getLastModifiedTime());
    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats2.getLastAccessedTime());
    assertInRange(before - ESTAT_RES, after + ESTAT_RES, eStats2.getLastModifiedTime());
    assertInRange(before, after, rStats.getLastAccessedTime());

    region.destroy(key2);

    assertInRange(before, after, rStats.getLastModifiedTime());
    assertInRange(before, after, rStats.getLastAccessedTime());
  }

  private void waitForClockToChange(Region region) {
    waitForClockToChange(region, 0);
  }

  /**
   * Waits for the time in the <code>region</code> to step up
   * beyond the current time in the region plus the number of
   * milliseconds passed in the <code>millisecs</code> argument.
   */
  private void waitForClockToChange(Region region, long millisecs) {
    long time = ((InternalRegion) region).cacheTimeMillis();
    while (time >= ((InternalRegion) region).cacheTimeMillis() + millisecs) {
    }
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
    final String name = getUniqueName();
    final Object key0 = Integer.valueOf(0);
    final Object key1 = Integer.valueOf(1);
    final Object key2 = Integer.valueOf(2);
    final Object key3 = Integer.valueOf(3);
    final Object value = "VALUE";
    final Object value1 = "VALUE1";
    final Object value2 = "VALUE2";
    final Object value3 = "VALUE3";
    final Object notPresentKey1 = "NOT_PRESENT_1";
    final Object notPresentKey2 = "NOT_PRESENT_2";
    final Object notPresentKey3 = "NOT_PRESENT_3";
    final Object notPresentKey4 = "NOT_PRESENT_4";

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    SerializableRunnableIF create = () -> {
      AttributesFactory factory = new AttributesFactory();
      factory.setEarlyAck(false);
      factory.setStatisticsEnabled(true);
      createPartitionedRegion(name, factory.create());
    };

    vm0.invoke(create);
    vm1.invoke(create);

    vm0.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      region.put(key0, value);
      region.put(key1, value1);

      assertTrue(stats.getLastModifiedTime() > lastModified);
      assertEquals(0, stats.getHitCount());
      assertEquals(0, stats.getMissCount());
    });

    vm1.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      region.put(key2, value3);
      region.put(key3, value3);

      assertTrue(stats.getLastModifiedTime() > lastModified);
      assertEquals(0, stats.getHitCount());
      assertEquals(0, stats.getMissCount());
    });

    vm0.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      lastAccessed = stats.getLastAccessedTime();
      lastModified = stats.getLastModifiedTime();

      Object result = region.get(key0);
      region.get(key1);
      region.get(key2);
      region.get(key3);
      region.get(notPresentKey1);
      region.get(notPresentKey2);
      region.get(notPresentKey3);
      region.get(notPresentKey4);

      assertEquals(value, result);
      assertEquals(2, stats.getMissCount());
      assertEquals(2, stats.getHitCount());
      assertTrue(stats.getLastAccessedTime() > lastAccessed);
    });

    vm0.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      lastAccessed = stats.getLastAccessedTime();

      Object result = region.get(key0);
      region.get(key1);
      region.get(key2);
      region.get(key3);
      region.get(notPresentKey1);
      region.get(notPresentKey2);
      region.get(notPresentKey3);
      region.get(notPresentKey4);

      assertEquals(value, result);
      assertEquals(4, stats.getHitCount());
      assertEquals(4, stats.getMissCount());
      assertTrue(stats.getLastAccessedTime() > lastAccessed);
    });

    vm0.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      long before = ((InternalRegion) region).cacheTimeMillis();
      region.put(key0, value);
      region.put(key1, value1);
      region.put(key2, value2);
      region.put(key3, value3);
      long after = ((InternalRegion) region).cacheTimeMillis();
      assertTrue(before <= stats.getLastModifiedTime());
      assertTrue(after >= stats.getLastModifiedTime());
    });

    vm1.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      long before = ((InternalRegion) region).cacheTimeMillis();
      region.put(key0, value);
      region.put(key1, value1);
      region.put(key2, value2);
      region.put(key3, value3);
      long after = ((InternalRegion) region).cacheTimeMillis();
      assertTrue(before <= stats.getLastModifiedTime());
      assertTrue(after >= stats.getLastModifiedTime());
    });

    vm0.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      lastAccessed = stats.getLastAccessedTime();
      lastModified = stats.getLastModifiedTime();
      region.invalidate(key0);
      region.invalidate(key1);
      assertEquals(lastAccessed, stats.getLastAccessedTime());
      assertEquals(lastModified, stats.getLastModifiedTime());
      assertEquals(4, stats.getHitCount());
      assertEquals(4, stats.getMissCount());
    });

    vm1.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      lastAccessed = stats.getLastAccessedTime();
      lastModified = stats.getLastModifiedTime();
      region.invalidate(key2);
      region.invalidate(key3);

      assertEquals(lastAccessed, stats.getLastAccessedTime());
      assertEquals(lastModified, stats.getLastModifiedTime());
      assertEquals(4, stats.getHitCount());
      assertEquals(4, stats.getMissCount());
    });

    vm0.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      lastModified = stats.getLastModifiedTime();
      region.destroy(key0);
      region.destroy(key1);

      assertEquals(lastModified, stats.getLastModifiedTime());
      assertEquals(4, stats.getHitCount());
      assertEquals(4, stats.getMissCount());
    });

    vm1.invoke(() -> {
      Region region = getRootRegion(name);
      CacheStatistics stats = region.getStatistics();
      lastModified = stats.getLastModifiedTime();
      region.destroy(key2);
      region.destroy(key3);

      assertEquals(lastModified, stats.getLastModifiedTime());
      assertEquals(4, stats.getHitCount());
      assertEquals(4, stats.getMissCount());
    });

  }

  /**
   * Tests that an attempt to get statistics when they are disabled results in a
   * {@link StatisticsDisabledException}.
   */
  @Test
  public void testDisabledStatistics() throws CacheException {
    String name = getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";

    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(false);
    Region region = createPartitionedRegion(name, factory.create());

    assertThatThrownBy(() -> region.getStatistics())
        .isInstanceOf(StatisticsDisabledException.class);

    region.put(key, value);
    Region.Entry entry = region.getEntry(key);

    assertThatThrownBy(() -> entry.getStatistics()).isInstanceOf(StatisticsDisabledException.class);
  }

  /**
   * Tests that the stats values for a Proxy PartitionedRegion are all 0.
   */
  @Test
  public void testProxyRegionStats() throws CacheException {
    String name = getUniqueName();
    Object key = Integer.valueOf(0);
    Object missingKey = Integer.valueOf(999);
    Object value = "VALUE";

    // Create the PARTITION_PROXY region
    AttributesFactory proxyRegionAttrsFactory = new AttributesFactory();
    proxyRegionAttrsFactory.setStatisticsEnabled(true);
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    // Setting LocalMaxMemory to 0 makes the region of PROXY type
    paf.setLocalMaxMemory(0);
    proxyRegionAttrsFactory.setPartitionAttributes(paf.create());
    Region region = createPartitionedRegion(name, proxyRegionAttrsFactory.create());

    // Create the region as not proxy on another server to be able to add entries.
    VM vm0 = VM.getVM(0);
    vm0.invoke(() -> {
      AttributesFactory factory = new AttributesFactory();
      factory.setStatisticsEnabled(true);
      createPartitionedRegion(name, factory.create());
    });

    region.put(key, value);
    region.get(key);
    region.get(missingKey);

    CacheStatistics stats = region.getStatistics();
    assertEquals(0, stats.getLastModifiedTime());
    assertEquals(0, stats.getLastAccessedTime());
    assertEquals(0, stats.getHitCount());
    assertEquals(0, stats.getMissCount());
    assertEquals(0.0f, stats.getHitRatio(), 0.0f);

    // Call reset counts to check that no exception is raised
    stats.resetCounts();
  }
}
