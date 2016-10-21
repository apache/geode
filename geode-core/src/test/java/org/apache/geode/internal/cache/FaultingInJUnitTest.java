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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Tests faulting in from current oplog, old oplog and htree for different modes (overflow only,
 * persist+overflow : Sync/Async)
 */
@Category(IntegrationTest.class)
public class FaultingInJUnitTest extends DiskRegionTestingBase {

  private volatile boolean hasBeenNotified;

  private DiskRegionProperties diskProps = new DiskRegionProperties();

  @Override
  protected final void postSetUp() throws Exception {
    deleteFiles();
    diskProps.setDiskDirs(dirs);
    diskProps.setCompactionThreshold(100);
    this.hasBeenNotified = false;
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
  }

  @Override
  protected final void preTearDown() throws Exception {
    closeDown();
    deleteFiles();
  }

  /**
   * fault in a value from teh current oplog
   */
  private void faultInFromCurrentOplog() {
    put100Int();
    putTillOverFlow(region);
    region.put(new Integer(200), new Integer(200));
    region.put(new Integer(201), new Integer(201));
    if (!(region.get(new Integer(2)).equals(new Integer(2)))) {
      fail(" fault in value not correct");
    }
  }

  /**
   * fault in a value from an old oplog
   */
  private void faultInFromOldOplog() {
    put100Int();
    putTillOverFlow(region);
    region.put(new Integer(200), new Integer(200));
    region.put(new Integer(201), new Integer(201));
    region.forceRolling();
    if (!(region.get(new Integer(2)).equals(new Integer(2)))) {
      fail(" fault in value not correct");
    }
  }

  /**
   * fault in a value that has been copied forward by compaction
   */
  private void faultInFromCompactedOplog() {
    put100Int();
    putTillOverFlow(region);
    region.put(new Integer(101), new Integer(101));
    region.put(new Integer(102), new Integer(102));
    region.put(new Integer(103), new Integer(103));
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {
      public void beforeGoingToCompact() {
        region.getCache().getLogger().info("beforeGoingToCompact before sleep");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignore) {
          fail("interrupted");
        }
        region.getCache().getLogger().info("beforeGoingToCompact after sleep");
      }

      public void afterHavingCompacted() {
        region.getCache().getLogger().info("afterHavingCompacted");
        synchronized (region) {
          hasBeenNotified = true;
          region.notify();
        }
      }
    });
    // first give time for possible async writes to be written to disk
    ((LocalRegion) region).forceFlush();
    // no need to force roll or wait for compact if overflow only
    if (((LocalRegion) region).getDiskRegion().isBackup()) {
      region.getCache().getLogger().info("before forceRolling");
      region.forceRolling();
      region.getCache().getLogger().info("after forceRolling");
      synchronized (region) {
        while (!hasBeenNotified) {
          try {
            region.wait(9000);
            assertTrue(hasBeenNotified);
          } catch (InterruptedException e) {
            fail("exception not expected" + e);
          }
        }
      }
    }

    // make sure it was faulted out
    assertEquals(null, ((LocalRegion) region).getValueInVM(new Integer(2)));
    // and that the correct value will be faulted in
    verify100Int(false);
  }

  /**
   * test OverflowOnly Sync Faultin From CurrentOplog
   */
  @Test
  public void testOverflowOnlyFaultinSyncFromCurrentOplog() {
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    faultInFromCurrentOplog();
  }

  @Test
  public void testOverflowOnlyFaultinSyncFromOldOplog() {
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    faultInFromOldOplog();
  }

  @Test
  public void testOverflowOnlyFaultinSyncFromCompactedOplog() {
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, diskProps);
    faultInFromCompactedOplog();
  }

  @Test
  public void testOverflowOnlyFaultinAsyncFromCurrentOplog() {
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps);
    faultInFromCurrentOplog();
  }

  @Test
  public void testOverflowOnlyFaultinAsyncFromOldOplog() {
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps);
    faultInFromOldOplog();
  }

  @Test
  public void testOverflowOnlyFaultinAsyncFromCompactedOplog() {
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, diskProps);
    faultInFromCompactedOplog();
  }

  @Test
  public void testOverflowAndPersistFaultinSyncFromCurrentOplog() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    faultInFromCurrentOplog();
  }

  @Test
  public void testOverflowAndPersistFaultinSyncFromOldOplog() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    faultInFromOldOplog();
  }

  @Test
  public void testOverflowAndPersistFaultinSyncFromCompactedOplog() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    faultInFromCompactedOplog();
  }

  @Test
  public void testOverflowAndPersistFaultinAsyncFromCurrentOplog() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    faultInFromCurrentOplog();
  }

  @Test
  public void testOverflowAndPersistFaultinAsyncFromOldOplog() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    faultInFromOldOplog();
  }

  @Test
  public void testOverflowAndPersistFaultinAsyncFromCompactedOplog() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    faultInFromCompactedOplog();
  }
}
