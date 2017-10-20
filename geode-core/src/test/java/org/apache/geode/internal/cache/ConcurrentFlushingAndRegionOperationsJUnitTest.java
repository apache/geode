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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * This JUnit tests concurrent rolling and normal region operations put,get,clear,destroy in both
 * sync and async mode
 * 
 * A region operation is done on the same key that is about to be rolled or has just been rolled and
 * the region operation is verified to have been correctly executed.
 */
@Category(IntegrationTest.class)
public class ConcurrentFlushingAndRegionOperationsJUnitTest extends DiskRegionTestingBase {

  protected boolean alreadyComeHere = false;

  /**
   * A put is done on the same entry before flushing that entry
   * 
   * The test ensures that put is not done twice by using a alreadyComeHere boolean.
   * 
   * @param region
   */
  void putBeforeFlush(final Region region) {
    alreadyComeHere = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void goingToFlush() {
        if (!alreadyComeHere) {
          // this should do an in place update of the re we just took of the queue
          region.put("Key", "Value2");
        }
        alreadyComeHere = true;
      }
    });
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    region.put("Key", "Value1");
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    try {
      assertEquals("Value2", region.get("Key"));
      assertEquals("Value2", getValueOnDisk(region));
    } catch (EntryNotFoundException e) {
      logWriter.error("Exception occurred", e);
      fail("Entry not found although was supposed to be there");
    }
  }

  /**
   * a single get is done on the entry about to be flushed. multiple gets are avoided by the
   * alreadyComeHere boolean
   * 
   * 
   * 
   * @param region
   */
  void getBeforeFlush(final Region region) {
    alreadyComeHere = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void goingToFlush() {
        if (!alreadyComeHere) {
          region.get("Key");
        }
        alreadyComeHere = true;
      }
    });
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    region.put("Key", "Value1");
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    try {
      assertEquals("Value1", getValueOnDisk(region));
    } catch (EntryNotFoundException e) {
      logWriter.error("Exception occurred", e);
      fail("Entry not found although was supposed to be there");
    }
  }

  /**
   * the entry which is about to be flushed is deleted
   * 
   * @param region
   */
  void delBeforeFlush(final Region region) {
    alreadyComeHere = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void goingToFlush() {
        if (!alreadyComeHere) {
          try {
            region.destroy("Key");
          } catch (Exception e) {
            logWriter.error("Exception occurred", e);
            fail("Exception occurred when it was not supposed to occur");
          }
        }
        alreadyComeHere = true;
      }
    });
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    region.put("Key", "Value1");
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    boolean entryNotFound = false;
    Object v = null;
    try {
      v = getValueOnDisk(region);
    } catch (EntryNotFoundException e) {
      entryNotFound = true;
    }
    if (!entryNotFound && v != null && !v.equals(Token.TOMBSTONE)) {
      fail("EntryNotFoundException was expected but did not get it");
    }
    entryNotFound = false;
    Object obj = ((LocalRegion) region).basicGetEntry("Key");
    if (obj == null) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
  }

  /**
   * region is closed before going to be flushed The region is going to be closed in a separate
   * thread. A 3000 ms wait is done to ensure that the separate thread has successfully closed the
   * region
   * 
   * @param region
   */
  void closeBeforeFlush(final Region region) {
    hasBeenNotified = false;
    alreadyComeHere = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void goingToFlush() {
        if (!alreadyComeHere) {
          try {
            new Close(region).start();
            try {
              Thread.sleep(3000);
            } catch (InterruptedException ok) {
              // this is ok; the async writer thread is interrupted during shutdown
              Thread.currentThread().interrupt();
            }
          } catch (Exception e) {
            logWriter.error("Exception occurred", e);
            fail("Exception occurred when it was not supposed to occur");
          }
        }
        alreadyComeHere = true;
      }
    });
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    region.put("Key", "Value1");
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    synchronized (region) {
      try {
        if (!hasBeenNotified) {
          region.wait();
        }
      } catch (InterruptedException e) {
        logWriter.error("Exception occurred", e);
        fail("interrupted");
      }
    }
  }

  /**
   * A region close is done after flush is over. The close is done in a separate thread and a 3000
   * ms wait is put to ensure that the separate thread has closed the region
   * 
   * @param region
   */
  void closeAfterFlush(final Region region) {
    hasBeenNotified = false;
    alreadyComeHere = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void afterWritingBytes() {
        if (!alreadyComeHere) {
          try {
            new Close(region).start();
            try {
              Thread.sleep(3000);
            } catch (InterruptedException ok) {
              // this is ok; the async writer thread is interrupted during shutdown
              Thread.currentThread().interrupt();
            }
          } catch (Exception e) {
            logWriter.error("Exception occurred", e);
            fail("Exception occurred when it was not supposed to occur");
          }
        }
        alreadyComeHere = true;
      }
    });
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    region.put("Key", "Value1");
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    synchronized (region) {
      try {
        if (!hasBeenNotified) {
          region.wait();
        }
      } catch (InterruptedException e) {
        logWriter.error("Exception occurred", e);
        fail("interrupted");
      }
    }
  }

  void clearBeforeFlush(final Region region) {
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    region.put("Key", "Value1");
    region.clear();
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    boolean entryNotFound = false;
    try {
      getValueOnDisk(region);
    } catch (EntryNotFoundException e) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
    entryNotFound = false;
    Object obj = ((LocalRegion) region).basicGetEntry("Key");
    if (obj == null) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
  }

  void putAfterFlush(final Region region) {
    alreadyComeHere = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void afterWritingBytes() {
        if (!alreadyComeHere) {
          DiskEntry de = (DiskEntry) ((LocalRegion) region).basicGetEntry("Key");
          if (de == null)
            return; // this is caused by the first flush
          DiskId id = de.getDiskId();
          long oldOplogId = id.getOplogId();
          long oldOplogOffset = id.getOffsetInOplog();
          ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
          // region.getCache().getLogger().info("putting value2");
          region.put("Key", "Value2");
          id = ((DiskEntry) (((LocalRegion) region).basicGetEntry("Key"))).getDiskId();
          long newOplogId = id.getOplogId();
          long newOplogOffset = id.getOffsetInOplog();
          id.setOplogId(oldOplogId);
          id.setOffsetInOplog(oldOplogOffset);
          assertEquals("Value1", ((LocalRegion) region).getDiskRegion().getNoBuffer(id));
          id.setOplogId(newOplogId);
          id.setOffsetInOplog(newOplogOffset);
        }
        alreadyComeHere = true;
      }
    });
    // region.getCache().getLogger().info("before pause");
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    // region.getCache().getLogger().info("putting value1");
    region.put("Key", "Value1");
    // region.getCache().getLogger().info("before flush");
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    try {
      // region.getCache().getLogger().info("getting value2");
      assertEquals("Value2", region.get("Key"));
    } catch (Exception e) {
      logWriter.error("Exception occurred", e);
      fail("Entry not found although was supposed to be there");
    }

    ((LocalRegion) region).getDiskRegion().flushForTesting();

    try {
      assertEquals("Value2", getValueOnDisk(region));
    } catch (EntryNotFoundException e) {
      logWriter.error("Exception occurred", e);
      fail("Entry not found although was supposed to be there");
    }

  }

  void getAfterFlush(final Region region) {
    alreadyComeHere = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void afterWritingBytes() {
        if (!alreadyComeHere) {
          region.get("key");
        }
        alreadyComeHere = true;
      }
    });
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    region.put("Key", "Value1");
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    try {
      assertEquals("Value1", getValueOnDisk(region));
    } catch (EntryNotFoundException e) {
      logWriter.error("Exception occurred", e);
      fail("Entry not found although was supposed to be there");
    }
  }

  void delAfterFlush(final Region region) {
    alreadyComeHere = false;
    CacheObserverHolder.setInstance(new CacheObserverAdapter() {

      public void afterWritingBytes() {
        if (!alreadyComeHere) {
          try {

            region.destroy("Key");
          } catch (Exception e1) {
            logWriter.error("Exception occurred", e1);
            fail("encounter exception when not expected " + e1);
          }
        }
        alreadyComeHere = true;
      }
    });
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    region.put("Key", "Value1");
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    boolean entryNotFound = false;
    Object v = null;
    try {
      v = getValueOnDisk(region);
    } catch (EntryNotFoundException e) {
      entryNotFound = true;
    }
    if (!entryNotFound && v != null && !v.equals(Token.TOMBSTONE)) {
      fail("EntryNotFoundException was expected but did not get it");
    }
    entryNotFound = false;
    Object obj = ((LocalRegion) region).basicGetEntry("Key");
    if (obj == null) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
  }

  void clearAfterFlush(final Region region) {
    ((LocalRegion) region).getDiskRegion().pauseFlusherForTesting();
    region.put("Key", "Value1");
    ((LocalRegion) region).getDiskRegion().flushForTesting();
    region.clear();
    boolean entryNotFound = false;
    try {
      getValueOnDisk(region);
    } catch (EntryNotFoundException e) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
    entryNotFound = false;
    Object obj = ((LocalRegion) region).basicGetEntry("Key");
    if (obj == null) {
      entryNotFound = true;
    }
    if (!entryNotFound) {
      fail("EntryNotFoundException was expected but did not get it");
    }
  }

  Object getValueOnDisk(Region region) throws EntryNotFoundException {
    ((LocalRegion) region).getDiskRegion().forceFlush();
    return ((LocalRegion) region).getValueOnDisk("Key");
  }

  @Test
  public void testPutBeforeFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    putBeforeFlush(region);
    region.destroyRegion();
  }

  @Test
  public void testPutAfterFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    putAfterFlush(region);
    region.destroyRegion();
  }

  @Test
  public void testGetBeforeFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    getBeforeFlush(region);
    region.destroyRegion();
  }

  @Test
  public void testGetAfterFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    getAfterFlush(region);
    region.destroyRegion();
  }

  @Test
  public void testClearBeforeFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    clearBeforeFlush(region);
    region.destroyRegion();
  }

  @Test
  public void testClearAfterFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    clearAfterFlush(region);
    region.destroyRegion();
  }

  @Test
  public void testDelBeforeFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    delBeforeFlush(region);
    region.destroyRegion();
  }

  @Test
  public void testDelAfterFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    delAfterFlush(region);
    region.destroyRegion();
  }

  @Test
  public void testCloseBeforeFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    closeBeforeFlush(region);
    assertFalse(failureCause, testFailed);
  }

  @Test
  public void testCloseAfterFlush() {
    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(100000000L);
    diskRegionProperties.setTimeInterval(100000000L);
    diskRegionProperties.setDiskDirs(dirs);
    diskRegionProperties.setRolling(true);
    region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskRegionProperties);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    closeAfterFlush(region);
    assertFalse(failureCause, testFailed);
  }

  protected boolean hasBeenNotified = false;

  class Close extends Thread {

    private Region region;

    Close(Region region) {
      this.region = region;
    }

    public void run() {
      try {
        region.close();
        synchronized (region) {
          region.notify();
          hasBeenNotified = true;
        }
      } catch (Exception e) {
        logWriter.error("Exception occurred", e);
        testFailed = true;
        failureCause =
            "Exception occurred when it was not supposed to occur, due to " + e + "in Close::run";
        fail("Exception occurred when it was not supposed to occur, due to " + e);
      }
    }
  }

}
