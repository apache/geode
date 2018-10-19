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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.DistributionConfig;

/**
 * The test will verify <br>
 * 1. Multiple oplogs are being rolled at once <br>
 * 2. The Number of entries getting logged to the HTree are taking care of creation
 */
public class MultipleOplogsRollingFeatureJUnitTest extends DiskRegionTestingBase {

  private volatile boolean FLAG = false;

  private Object mutex = new Object();

  private boolean CALLBACK_SET = false;

  private DiskRegionProperties diskProps = new DiskRegionProperties();

  @Override
  protected final void preTearDown() throws Exception {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
  }

  @Override
  protected final void postTearDown() throws Exception {
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "MAX_OPLOGS_PER_COMPACTION");
    diskProps.setDiskDirs(dirs);
  }

  /**
   * The test will verify <br>
   * 1. Multiple oplogs are being rolled at once 2. The Number of entries are properly conflated
   */
  @Test
  public void testMultipleRolling() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "MAX_OPLOGS_PER_COMPACTION", "17");

    deleteFiles();
    diskProps.setMaxOplogSize(450);
    diskProps.setCompactionThreshold(100);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertNotNull(region);
    DiskRegion diskRegion = ((LocalRegion) region).getDiskRegion();
    assertNotNull(diskRegion);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserverHolder.setInstance(getCacheObserver());

    try {

      CALLBACK_SET = true;

      assertEquals(null, diskRegion.getOplogToBeCompacted());

      logWriter.info("testMultipleRolling adding entry 1");
      addEntries(1 /* oplogNumber */, 50 /* byte array size */);

      ((LocalRegion) region).getDiskStore().forceCompaction();
      await().until(() -> FLAG == true);
      logWriter.info("testMultipleRolling after waitForCompactor");
      // the compactor copied two tombstone and 1 entry to oplog #2
      // The total oplog size will become 429, that why we need to
      // set oplogmaxsize to be 450. After compaction, the size become 151
      // the compactor thread is now stuck waiting for mutex.notify

      // check the oplog rolling is null (since the compactor was able to delete it)
      if (diskRegion.getOplogToBeCompacted() != null) {
        logWriter.info("testMultipleRolling OplogToBeCompacted="
            + java.util.Arrays.toString(diskRegion.getOplogToBeCompacted()));
      }
      assertEquals(null, diskRegion.getOplogToBeCompacted());

      // update key3 with 360 bytes to cause it not fit in oplog #2 so it will create oplog #3
      // this does an update to key 3 (3 is no longer in oplog#2)
      // So oplog#2 is EMPTY.
      logWriter.info("testMultipleRolling adding entry 2");
      addEntries(2 /* oplogNumber */, 360 /* byte array size */);

      // #2 is now owned by the compactor so even though it is empty it
      // will not be deleted until the compactor does so.
      // only one oplog to be compacted because there's no drf
      assertEquals(1, diskRegion.getOplogToBeCompacted().length);

      // This add will not fit in oplog #3 so it will create oplog #4
      // It does an update to key 3 (so 3 is no longer in oplog#3)
      // which empties it out
      logWriter.info("testMultipleRolling adding entry 3");
      addEntries(3 /* oplogNumber */, 180 /* byte array size */);

      // #3 is was ready to be compacted but since it was EMPTIED
      // but because the compaction thread (the pool defaults to 1 thread)
      // is hung it will not be deleted immedaitely.
      assertEquals(2, diskRegion.getOplogToBeCompacted().length);

      // this add will fit in the current oplog
      // Just added an update to oplog #4
      logWriter.info("testMultipleRolling adding entry 4");
      addEntries(4 /* oplogNumber */, 1 /* byte array size */);

      assertEquals(2, diskRegion.getOplogToBeCompacted().length);
      logWriter.info("testMultipleRolling forceRolling");
      region.forceRolling();
      assertEquals(3, diskRegion.getOplogToBeCompacted().length);

    } finally {
      synchronized (mutex) {
        // let the compactor go
        CALLBACK_SET = false;
        FLAG = false;
        logWriter.info("testMultipleRolling letting compactor go");
        mutex.notify();
      }
    }

    // let the main thread sleep so that rolling gets over
    await().until(() -> FLAG == true);

    assertTrue("Number of Oplogs to be rolled is not null : this is unexpected",
        diskRegion.getOplogToBeCompacted() == null);
    cache.close();
    cache = createCache();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertTrue("Recreated region size is not 1 ", region.size() == 1);

    closeDown();
    deleteFiles();
  }

  private void addEntries(int opLogNum, int valueSize) {
    assertNotNull(region);
    byte[] val = new byte[valueSize];
    for (int i = 0; i < valueSize; ++i) {
      val[i] = (byte) i;
    }

    // Creating opLog1
    if (opLogNum == 1) {
      for (int i = 1; i < 4; i++) {
        // create 3 entries
        region.create(new Integer(i), val);

      }
      // destroy Entry 1 and 2
      region.destroy(new Integer(1));
      region.destroy(new Integer(2));
    }

    else if (opLogNum == 2) {
      // update Entry 3
      region.put(new Integer(3), val);

    }

    else if (opLogNum == 3) {
      // update Entry 3
      region.put(new Integer(3), val);

    }

    else if (opLogNum == 4) {
      // update Entry 3
      region.put(new Integer(3), val);
    }
  }

  private CacheObserver getCacheObserver() {
    return (new CacheObserverAdapter() {

      public void beforeGoingToCompact() {

        if (logWriter.fineEnabled()) {

          logWriter.fine("In beforeGoingToCompact");
        }

      }

      public void afterHavingCompacted() {
        FLAG = true;
        if (CALLBACK_SET) {
          synchronized (mutex) {
            try {
              mutex.wait();
            } catch (InterruptedException e) {
              fail("interrupted");
            }
          }
        }
        if (logWriter.fineEnabled()) {

          logWriter.fine("In afterHavingCompacted");
        }

      }
    });
  }
}
