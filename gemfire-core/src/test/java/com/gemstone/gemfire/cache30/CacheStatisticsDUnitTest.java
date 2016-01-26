/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheStatistics;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.StatisticsDisabledException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests the {@link CacheStatistics} that are maintained by a {@link
 * Region} and a {@link com.gemstone.gemfire.cache.Region.Entry}.
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public class CacheStatisticsDUnitTest extends CacheTestCase {

  public CacheStatisticsDUnitTest(String name) {
    super(name);
  }

  ////////  Helper Methods

  /**
   * Asserts that two <code>long</code>s are equal concerning a
   * delta.
   */
//   public static void assertEquals(long expected, long actual,
//                                   long delta) {
//     long difference = Math.abs(expected - actual);
//     assertTrue("Expected: " + expected
//                + " Actual: " + actual,
//                 difference <= delta);
//   }

  public static void assertInRange(long start, long end,
                                  long actual) {
    assertTrue("Expected: " + actual + " >= " + start,
                actual >= start);
    assertTrue("Expected: " + actual + " <= " + end,
                actual <= end);
  }

  ////////  Test methods

  /**
   * Tests that the {@link CacheStatistics#getHitCount hit count} and
   * {@link CacheStatistics#getMissCount miss count} are updated
   * properly for a local region and its entries.
   */
  public void testHitMissCount() throws CacheException {
    String name = this.getUniqueName();
    Object key = "KEY"; // value exists
    Object key2 = "KEY2"; // no entry
    Object key3 = "KEY3"; // entry, invalid
    Object value = "VALUE";

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setStatisticsEnabled(true);

    Region region =
      createRegion(name, factory.create());
    CacheStatistics rStats = region.getStatistics();
    assertEquals(0, rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    assertNull(region.get(key));
    assertEquals(0, rStats.getHitCount());
    assertEquals(1, rStats.getMissCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    assertNull(region.get(key));
    assertEquals(0, rStats.getHitCount());
    assertEquals(2, rStats.getMissCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    rStats.resetCounts();
    assertEquals(0, rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());
    assertEquals(0.0f, rStats.getHitRatio(), 0.0f);

    region.put(key, value);
    assertEquals(0, rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());

    CacheStatistics eStats = region.getEntry(key).getStatistics();
    assertEquals(0, eStats.getHitCount());
    assertEquals(0, eStats.getMissCount());
    assertEquals(0.0f, eStats.getHitRatio(), 0.0f);

    region.get(key);
    assertEquals(1, eStats.getHitCount());
    assertEquals(0, eStats.getMissCount());
    assertEquals(1.0f, eStats.getHitRatio(), 0.0f);

    assertEquals(1, rStats.getHitCount());
    assertEquals(0, rStats.getMissCount());
    assertEquals(1.0f, rStats.getHitRatio(), 0.0f);

    region.get(key2);
    // assert independent from eStats
    assertEquals(1, eStats.getHitCount());
    assertEquals(0, eStats.getMissCount());
    assertEquals(1.0f, eStats.getHitRatio(), 0.0f);

    assertEquals(1, rStats.getHitCount());
    assertEquals(1, rStats.getMissCount());
    assertEquals(0.5f, rStats.getHitRatio(), 0.0f);

    region.create(key3, null);
    CacheStatistics e3Stats = region.getEntry(key3).getStatistics();
    assertEquals(0, e3Stats.getHitCount());
    assertEquals(0, e3Stats.getMissCount());
    assertEquals(0.0f, e3Stats.getHitRatio(), 0.0f);

    region.get(key3); // miss on existing entry
    assertEquals(0, e3Stats.getHitCount());
    assertEquals(1, e3Stats.getMissCount());
    assertEquals(0.0f, e3Stats.getHitRatio(), 0.0f);

    assertEquals(1, rStats.getHitCount());
    assertEquals(2, rStats.getMissCount());
    assertEquals(0.33f, rStats.getHitRatio(), 0.01f);

    eStats.resetCounts();
    assertEquals(0, eStats.getHitCount());
    assertEquals(0, eStats.getMissCount());
    assertEquals(0.0f, eStats.getHitRatio(), 0.0f);

    rStats.resetCounts();
    region.invalidate(key);
    assertEquals(0, eStats.getHitCount());
    assertEquals(0, eStats.getMissCount());
    assertEquals(0.0f, eStats.getHitRatio(), 0.0f);

    region.get(key);
    assertEquals(0, eStats.getHitCount());
    assertEquals(1, eStats.getMissCount());

    assertEquals(0, rStats.getHitCount());
    assertEquals(1, rStats.getMissCount());

    region.destroy(key);
    try {
      eStats.getMissCount();
      fail("Should have thrown an EntryDestroyedException");

    } catch (EntryDestroyedException ex) {
      // pass...
    }
  }

  /**
   * Tests that the {@linkplain CacheStatistics#getLastAccessedTime
   * last access time} and {@link CacheStatistics#getLastModifiedTime
   * last modified time} are update appropriately for a local region
   * and its entries.  It also validates that the last modification
   * and last access times are propagated to parent regions.
   */
  public void testTimeStats() throws CacheException, InterruptedException
  {
    final long ESTAT_RES = 100; // the resolution, in ms, of entry stats
    String name = this.getUniqueName();
    Object key = "KEY";
    Object key2 = "KEY2";
    Object value = "VALUE";
    long before;
    long after;
    long oldBefore;
    long oldAfter;

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setStatisticsEnabled(true);

    before = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    Region region =
      createRegion(name, factory.create());
    CacheStatistics rStats = region.getStatistics();
    CacheStatistics rootStats =
      getRootRegion().getStatistics();
    after = ((GemFireCacheImpl)getCache()).cacheTimeMillis();

    assertInRange(before, after,  rStats.getLastAccessedTime());
    assertInRange(before, after,  rStats.getLastModifiedTime());
    assertInRange(before, after,  rootStats.getLastAccessedTime());
    assertInRange(before, after,  rootStats.getLastModifiedTime());

    oldBefore = before;
    oldAfter = after;
    pause(150);
    before = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    region.get(key);
    after = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    assertInRange(before, after,  rStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rStats.getLastModifiedTime());
    assertInRange(before, after,  rootStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rootStats.getLastModifiedTime());

    pause(150);
    before = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    region.put(key, value);
    CacheStatistics eStats = region.getEntry(key).getStatistics();
    after = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    assertInRange(before, after,  rStats.getLastAccessedTime());
    assertInRange(before, after,  rStats.getLastModifiedTime());
    assertInRange(before, after,  rootStats.getLastAccessedTime());
    assertInRange(before, after,  rootStats.getLastModifiedTime());

    assertInRange(before-ESTAT_RES, after+ESTAT_RES,  eStats.getLastAccessedTime());
    assertInRange(before-ESTAT_RES, after+ESTAT_RES,  eStats.getLastModifiedTime());

    oldBefore = before;
    oldAfter = after;
    pause(150);
    before = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    region.get(key);
    after = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    assertInRange(before, after,  rStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rStats.getLastModifiedTime());
    assertInRange(before, after,  rootStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rootStats.getLastModifiedTime());
    assertInRange(before-ESTAT_RES, after+ESTAT_RES,  eStats.getLastAccessedTime());
    assertInRange(oldBefore-ESTAT_RES, oldAfter+ESTAT_RES,  eStats.getLastModifiedTime());

    long oldOldBefore = oldBefore;
    long oldOldAfter = oldAfter;
    oldBefore = before;
    oldAfter = after;
    pause(150);
    before = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    region.create(key2, null);
    CacheStatistics eStats2 = region.getEntry(key2).getStatistics();
    after = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    assertInRange(before, after,  rStats.getLastAccessedTime());
    assertInRange(before, after,  rStats.getLastModifiedTime());
    assertInRange(before, after,  rootStats.getLastAccessedTime());
    assertInRange(before, after,  rootStats.getLastModifiedTime());

    assertInRange(oldBefore-ESTAT_RES, oldAfter+ESTAT_RES,  eStats.getLastAccessedTime());
    assertInRange(oldOldBefore-ESTAT_RES, oldOldAfter+ESTAT_RES, eStats.getLastModifiedTime());

    assertInRange(before-ESTAT_RES, after+ESTAT_RES,  eStats2.getLastAccessedTime());
    assertInRange(before-ESTAT_RES, after+ESTAT_RES,  eStats2.getLastModifiedTime());

    // Invalidation and destruction do not update the modification/access
    // times
    oldBefore = before;
    oldAfter = after;
    pause(150);
    before = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    region.invalidate(key2);
    after = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    assertInRange(oldBefore-ESTAT_RES, oldAfter+ESTAT_RES,  eStats2.getLastAccessedTime());
    assertInRange(oldBefore-ESTAT_RES, oldAfter+ESTAT_RES,  eStats2.getLastModifiedTime());
    assertInRange(oldBefore, oldAfter,  rStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rStats.getLastModifiedTime());
    assertInRange(oldBefore, oldAfter,  rootStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rootStats.getLastModifiedTime());

    pause(150);
    before = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    region.destroy(key2);
    after = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    assertInRange(oldBefore, oldAfter,  rStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rStats.getLastModifiedTime());
    assertInRange(oldBefore, oldAfter,  rootStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rootStats.getLastModifiedTime());

    before = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    Region sub =
      region.createSubregion("sub", region.getAttributes());
    after = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    assertInRange(before, after,  rStats.getLastAccessedTime());
    assertInRange(before, after,  rStats.getLastModifiedTime());
    assertInRange(before, after,  rootStats.getLastAccessedTime());
    assertInRange(before, after,  rootStats.getLastModifiedTime());

    oldBefore = before;
    oldAfter = after;
    before = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    sub.destroyRegion();
    after = ((GemFireCacheImpl)getCache()).cacheTimeMillis();
    assertInRange(oldBefore, oldAfter,  rStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rStats.getLastModifiedTime());
    assertInRange(oldBefore, oldAfter,  rootStats.getLastAccessedTime());
    assertInRange(oldBefore, oldAfter,  rootStats.getLastModifiedTime());
  }

  /** The last time an entry was accessed */
  protected static volatile long lastAccessed;
  protected static volatile long lastModified;
  protected static volatile long lastModifiedLocal;
  protected static volatile long lastModifiedRemote;

  /**
   * Tests that distributed operations update the {@link
   * CacheStatistics#getLastModifiedTime last modified time}, but not
   * the {@link CacheStatistics#getLastAccessedTime last accessed
   * time}.  It also validates that distributed operations do not
   * affect the hit and miss counts in remote caches.
   */
  public void testDistributedStats() {
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
          public void run2() throws CacheException {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setEarlyAck(false);
            factory.setStatisticsEnabled(true);
            createRegion(name, factory.create());
          }
        };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Define entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
         // region.create(key, null);
          region.put(key,value);
          CacheStatistics stats =
            region.getEntry(key).getStatistics();
          lastAccessed = stats.getLastAccessedTime();
          lastModified = stats.getLastModifiedTime();
          getCache().getLogger().fine("DEFINE: lastAccessed: " + lastAccessed + ", lastModified: " + lastModified);
          assertEquals(0, stats.getHitCount());
          assertEquals(0, stats.getMissCount());
          assertTrue(lastModified > 0);
          lastModifiedLocal = lastModified;
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Net search") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
            Object result = region.get(key);
            assertEquals(value,result);
            CacheStatistics stats =
             region.getEntry(key).getStatistics();
           lastModifiedRemote = stats.getLastModifiedTime();
           getCache().getLogger().fine("NETSEARCH: lastAccessed: " + stats.getLastAccessedTime() + ", lastModified: " + stats.getLastModifiedTime());

        }
      });

    vm0.invoke(new CacheSerializableRunnable("Verify stats") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          CacheStatistics stats =
            region.getEntry(key).getStatistics();
          assertEquals(lastAccessed, stats.getLastAccessedTime());
          assertEquals(lastModified, stats.getLastModifiedTime());
          assertEquals(0, stats.getHitCount());
          assertEquals(0, stats.getMissCount());
        }
      });

    assertEquals(lastModifiedLocal,lastModifiedRemote);

    // make sure at least 100ms have passed; otherwise, the update
    // may not actually bump the statistics
    pause(100);

    vm1.invoke(new CacheSerializableRunnable("Update") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          //assertNull(region.getEntry(key));
          region.put(key, value);
          assertEquals(value, region.getEntry(key).getValue());
          CacheStatistics stats = region.getEntry(key).getStatistics();
          getCache().getLogger().fine("UPDATE: lastAccessed: " + stats.getLastAccessedTime() + ", lastModified: " + stats.getLastModifiedTime());
        }
      });

    final long errorMargin = 50;

    vm0.invoke(new CacheSerializableRunnable("Verify stats") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          CacheStatistics stats =
            region.getEntry(key).getStatistics();
          long ta = stats.getLastAccessedTime();
          long tm = stats.getLastModifiedTime();
          long hc = stats.getHitCount();
          long mc = stats.getMissCount();

          getCache().getLogger().fine("VERIFY: lastAccessed: " + ta + ", lastModified: " + tm);
          assertTrue("lastAccessedTime was " + ta +
                     " but was expected to be > " + lastAccessed,
                      lastAccessed < (ta + errorMargin));
          assertTrue("lastAccessed=" + lastAccessed +
                        " should be < stats.getLastModifiedTime=" + tm,
                     lastAccessed < (tm + errorMargin));
          assertEquals(0, hc);
          assertEquals(0, mc);
          lastAccessed = stats.getLastAccessedTime();
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Invalidate") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.invalidate(key);
        }
      });
    vm0.invoke(new CacheSerializableRunnable("Verify stats") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          CacheStatistics stats =
            region.getEntry(key).getStatistics();
          assertEquals(lastAccessed, stats.getLastAccessedTime());
          assertEquals(lastAccessed, stats.getLastModifiedTime());
          assertEquals(0, stats.getHitCount());
          assertEquals(0, stats.getMissCount());
        }
      });
    vm1.invoke(new CacheSerializableRunnable("Destroy Entry") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          region.destroy(key);
        }
      });
    vm0.invoke(new CacheSerializableRunnable("Verify region stats") {
        public void run2() throws CacheException {
          Region region =
            getRootRegion().getSubregion(name);
          CacheStatistics stats = region.getStatistics();

          // lastAccessed var contains stat from an Entry, which may be
          // up to 100 ms off from stat in Region because Entry has
          // less precision
          //assertEquals(lastAccessed, stats.getLastAccessedTime(), 100);
          assertEquals(0, stats.getHitCount());
          assertEquals(0, stats.getMissCount());
        }
      });
  }

  /**
   * Tests that an attempt to get statistics when they are disabled
   * results in a {@link StatisticsDisabledException}.
   */
  public void testDisabledStatistics() throws CacheException {
    String name = this.getUniqueName();
    Object key = "KEY";
    Object value = "VALUE";

    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(false);
    Region region =
      createRegion(name, factory.create());
    try {
      region.getStatistics();
      fail("Should have thrown a StatisticsDisabledException");

    } catch (StatisticsDisabledException ex) {
      // pass...
    }

    region.put(key, value);
    Region.Entry entry = region.getEntry(key);

    try {
      entry.getStatistics();

    } catch (StatisticsDisabledException ex) {
      // pass...
    }
  }

}
