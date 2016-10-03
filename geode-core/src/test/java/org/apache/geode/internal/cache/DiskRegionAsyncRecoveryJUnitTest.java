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
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class DiskRegionAsyncRecoveryJUnitTest extends DiskRegionTestingBase {

  @Override
  protected final void postTearDown() throws Exception {
    DiskStoreObserver.setInstance(null);
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "true");
    System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "false");
  }
  
  @Test
  public void testValuesNotRecoveredSynchronously() throws InterruptedException {
    Region region = createRegion();
    
    putEntries(region, 0, 50, "A");
    
    cache.close();
    
    cache =createCache();

    final CountDownLatch suspendRecovery = new CountDownLatch(1);
    final CountDownLatch recoveryDone = new CountDownLatch(1);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      
      @Override
      public void beforeAsyncValueRecovery(DiskStoreImpl store) {
        try {
          suspendRecovery.await();
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }

      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
        recoveryDone.countDown();
      }
    });
    try {
    region = createRegion();
    
    checkEntriesInMemory(region, 0, 50, "A", false);
    checkEntries(region, 0, 40, "A");
    
    putEntries(region, 0, 10, "B");
    invalidateEntries(region, 10, 20);
    removeEntries(region, 20, 30);
    
    suspendRecovery.countDown();
    recoveryDone.await();
    
    checkEntriesInMemory(region, 0, 10, "B", true);
    checkEntriesInMemory(region, 10, 20, Token.INVALID, true);
    checkEntries(region, 10, 20, null);
    checkEntries(region, 20, 30, null);
    checkEntriesInMemory(region, 30, 50, "A", true);
    } finally {
      suspendRecovery.countDown();
    }
  }
  
  @Test
  public void testBug42728() throws InterruptedException, IOException {
    Region region = createRegion();
    
    putEntries(region, 0, 5, "A");
    putEntries(region, 0, 1, "B");
    invalidateEntries(region, 1, 2);
    removeEntries(region, 2, 3);
    
    //this ensures we don't get a chance to create a krf
    //but instead recover from the crf, which I think is the
    //cause of this bug.
    backupDisk();
    cache.close();
    restoreDisk();
    
    cache =createCache();
    
    //no go ahead and recovery.
    region = createRegion();
    
    //and finally, create a krf and reopen the cache
    cache.close();
    cache =createCache();

    final CountDownLatch suspendRecovery = new CountDownLatch(1);
    final CountDownLatch recoveryDone = new CountDownLatch(1);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      
      @Override
      public void beforeAsyncValueRecovery(DiskStoreImpl store) {
        try {
          suspendRecovery.await();
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }

      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
        recoveryDone.countDown();
      }
    });
    try {
    region = createRegion();
    
    checkEntriesInMemory(region, 0, 1, "B", false);
    checkEntriesInMemory(region, 1, 2, Token.INVALID, true);
    checkEntries(region, 1, 2, null);
    checkEntries(region, 2, 3, null);
    checkEntriesInMemory(region, 3, 5, "A", false);
    
    suspendRecovery.countDown();
    recoveryDone.await();
    
    checkEntriesInMemory(region, 0, 1, "B", true);
    checkEntriesInMemory(region, 1, 2, Token.INVALID, true);
    checkEntries(region, 1, 2, null);
    checkEntries(region, 2, 3, null);
    checkEntriesInMemory(region, 3, 5, "A", true);
    } finally {
      suspendRecovery.countDown();
    }
  }
  
  /**
   * Test to make sure that we create missing krfs when we restart the system.
   */
  @Test
  public void testKrfCreatedAfterRestart() throws InterruptedException, IOException {
    LocalRegion region = (LocalRegion) createRegion();
    
    putEntries(region, 0, 5, "A");
    putEntries(region, 0, 1, "B");
    invalidateEntries(region, 1, 2);
    removeEntries(region, 2, 3);
    
    //this ensures we don't get a chance to create a krf
    //but instead recover from the crf
    backupDisk();
    cache.close();
    restoreDisk();
    
    cache =createCache();
    
    //no go ahead and recover
    region = (LocalRegion) createRegion();
    
    putEntries(region, 5, 10, "A");
    DiskStoreImpl store = (DiskStoreImpl) cache.findDiskStore(region.getAttributes().getDiskStoreName());
    //Create a new oplog, to make sure we still create a krf for the old oplog.
    store.forceRoll();
    putEntries(region, 10, 15, "A");

    PersistentOplogSet set = store.getPersistentOplogSet(region.getDiskRegion());
    String currentChild = set.getChild().getOplogFile().getName();
    //Wait for the krfs to be created
    Set<String> crfs;
    Set<String> krfs;
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    do {
      crfs = new HashSet<String>();
      krfs = new HashSet<String>();
      getCrfsAndKrfs(crfs, krfs);
      //Remove the current child because it does not have a krf
      crfs.remove(currentChild.split("\\.")[0]);
    } while(!crfs.equals(krfs) && System.nanoTime() < end);
    
    //Make sure all of the crfs have krfs
    assertEquals("KRFS were not created within 30 seconds", crfs, krfs);
    
    cache.close();
    
    crfs = new HashSet<String>();
    krfs = new HashSet<String>();
    getCrfsAndKrfs(crfs, krfs);
    assertEquals("last krf was not created on cache close", crfs, krfs);
  }

  protected void getCrfsAndKrfs(Set<String> crfs, Set<String> krfs) {
    for(File dir: dirs) {
      File[] files = dir.listFiles();
      for(File file: files) {
        if(file.getName().endsWith(".crf")) {
          crfs.add(file.getName().split("\\.")[0]);
        }
        else if(file.getName().endsWith(".krf")) {
          krfs.add(file.getName().split("\\.")[0]);
        }
      }
    }
  }
  
  @Test
  public void testValuesRecoveredIntoPlaceholder() throws InterruptedException {
    Region region = createRegion();
    
    putEntries(region, 0, 50, "A");
    
    cache.close();
    
    cache =createCache();

    final CountDownLatch recoveryDone = new CountDownLatch(1);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {

      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
        recoveryDone.countDown();
      }
    });
    region = createRegion();
    recoveryDone.await();
    
    checkEntriesInMemory(region, 0, 50, "A", true);
    checkEntries(region, 0, 50, "A");
  }
  
  @Test
  public void testMultipleRegions() throws InterruptedException {
    Region region = createRegion();
    Region region2 = createRegion("region2");
    
    putEntries(region, 0, 50, "A");
    putEntries(region2, 0, 50, "A");
    
    cache.close();
    
    cache =createCache();

    final CountDownLatch suspendRecovery = new CountDownLatch(1);
    final CountDownLatch recoveryDone = new CountDownLatch(1);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      
      @Override
      public void beforeAsyncValueRecovery(DiskStoreImpl store) {
        try {
          suspendRecovery.await();
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }

      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
        recoveryDone.countDown();
      }
    });
    try {
    region = createRegion();
    region2 = createRegion("region2");
    
    checkEntriesInMemory(region, 0, 50, "A", false);
    checkEntries(region, 0, 40, "A");
    
    putEntries(region, 0, 10, "B");
    invalidateEntries(region, 10, 20);
    removeEntries(region, 20, 30);
    
    checkEntriesInMemory(region2, 0, 50, "A", false);
    checkEntries(region2, 0, 40, "A");
    
    putEntries(region2, 0, 10, "B");
    invalidateEntries(region2, 10, 20);
    removeEntries(region2, 20, 30);
    
    suspendRecovery.countDown();
    recoveryDone.await();
    
    checkEntriesInMemory(region, 0, 10, "B", true);
    checkEntriesInMemory(region, 10, 20, Token.INVALID, true);
    checkEntries(region, 10, 20, null);
    checkEntries(region, 20, 30, null);
    checkEntriesInMemory(region, 30, 50, "A", true);
    
    checkEntriesInMemory(region2, 0, 10, "B", true);
    checkEntriesInMemory(region2, 10, 20, Token.INVALID, true);
    checkEntries(region2, 10, 20, null);
    checkEntries(region2, 20, 30, null);
    checkEntriesInMemory(region2, 30, 50, "A", true);
    } finally {
      suspendRecovery.countDown();
    }
  }
  
  @Category(FlakyTest.class) // GEODE-1957: recovery does not always happen; possible disk issue?
  @Test
  public void testCloseOpenRegion() throws InterruptedException {
    Region region = createRegion();
    
    putEntries(region, 0, 50, "A");
    
    final CountDownLatch recoveryDone1 = new CountDownLatch(1);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      
      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
        recoveryDone1.countDown();
      }
    });
    //This will trigger krf creation. Region close doesn't cut it.
    cache.close();
    cache = createCache();
    
    region = createRegion();
    
    //Make sure the first recovery is completely done.
    recoveryDone1.await();
    
    region.close();

    final CountDownLatch suspendRecovery = new CountDownLatch(1);
    final CountDownLatch recoveryDone = new CountDownLatch(1);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      
      @Override
      public void beforeAsyncValueRecovery(DiskStoreImpl store) {
        try {
          suspendRecovery.await();
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }

      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
        recoveryDone.countDown();
      }
    });
    try {
    region = createRegion();
    
    checkEntriesInMemory(region, 0, 50, "A", false);
    checkEntries(region, 0, 40, "A");
    
    putEntries(region, 0, 10, "B");
    invalidateEntries(region, 10, 20);
    removeEntries(region, 20, 30);
    
    suspendRecovery.countDown();
    recoveryDone.await();
    
    checkEntriesInMemory(region, 0, 10, "B", true);
    checkEntriesInMemory(region, 10, 20, Token.INVALID, true);
    checkEntries(region, 10, 20, null);
    checkEntries(region, 20, 30, null);
    checkEntriesInMemory(region, 30, 50, "A", true);
    } finally {
      suspendRecovery.countDown();
    }
  }
  
  @Test
  public void testSynchronousProperty() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, "true");
    Region region = createRegion();
    
    putEntries(region, 0, 50, "A");
    
    cache.close();
    
    cache =createCache();

    final CountDownLatch suspendRecovery = new CountDownLatch(1);
    DiskStoreObserver.setInstance(new DiskStoreObserver() {
      
      @Override
      public void beforeAsyncValueRecovery(DiskStoreImpl store) {
        try {
          suspendRecovery.await();
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      }

      @Override
      public void afterAsyncValueRecovery(DiskStoreImpl store) {
      }
    });
    try {
    region = createRegion();
    
    checkEntriesInMemory(region, 0, 50, "A", true);
    checkEntries(region, 0, 50, "A");
    } finally {
      suspendRecovery.countDown();
    }
  }
  
  @Test
  public void testNoValuesProperty() throws InterruptedException {
    System.setProperty(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, "false");
    Region region = createRegion();
    
    putEntries(region, 0, 50, "A");
    
    cache.close();
    
    cache =createCache();

    region = createRegion();
    
    //Give us a chance to recover values if we're going to.
    Thread.sleep(1000);
    checkEntriesInMemory(region, 0, 50, "A", false);
    checkEntries(region, 0, 50, "A");
  }
  
  /**
   * Validate that values are, or are not, in memory
   */
  private void checkEntriesInMemory(Region r, int start, int end, Object invalid,
      boolean inMemory) {
    LocalRegion region = (LocalRegion) r;
    for(int i = start; i < end ; i++) {
      Object inMemoryValue = region.getValueInVM(i);
      if(inMemory) {
        if(inMemoryValue instanceof VMCachedDeserializable) {
          inMemoryValue = ((VMCachedDeserializable) inMemoryValue).getDeserializedForReading();
        }
        assertEquals("Failed on entry " + i, invalid, inMemoryValue);
      } else {
        assertEquals("Failed on entry " + i, null, inMemoryValue);
      }
    }
  }
  
  private void checkEntries(Region r, int start, int end, String value) {
    LocalRegion region = (LocalRegion) r;
    for(int i = start; i < end ; i++) {
      assertEquals(value, region.get(i));
    }
  }
  
  private void checkInvalid(Region r, int start, int end) {
    LocalRegion region = (LocalRegion) r;
    for(int i = start; i < end ; i++) {
      assertTrue(region.containsKey(i));
      assertNull(region.get(i));
    }
  }
  
  private Region createRegion() {
    return createRegion("regionName");
  }

  private Region createRegion(String regionName) {
    if(cache.findDiskStore("store") == null) {
    cache.createDiskStoreFactory()
      .setMaxOplogSize(1)
      .setDiskDirs(dirs)
      .create("store");
    }
    Region region = cache.createRegionFactory()
      .setDiskStoreName("store")
      .setDataPolicy(DataPolicy.PERSISTENT_REPLICATE).create(regionName);
    return region;
  }
  
  private void backupDisk() throws IOException {
    
    File tmpDir = new File(dirs[0].getParent(), "backupDir");
    tmpDir.mkdirs();
    for(File file : dirs) {
      FileUtil.copy(file, new File(tmpDir, file.getName()));
    }
  }
  
  private void restoreDisk() throws IOException {
    File tmpDir = new File(dirs[0].getParent(), "backupDir");
    for(File file : dirs) {
      FileUtil.delete(file);
      FileUtil.copy(new File(tmpDir, file.getName()), file);
    }
  }

  private void putEntries(Region region, int start, int end, String value) {
    for(int i = start; i < end ; i++) {
      region.put(i, value);
    }
  }
  
  private void invalidateEntries(Region region, int start, int end) {
    for(int i = start; i < end ; i++) {
      region.invalidate(i);
    }
  }
  
  private void removeEntries(Region region, int start, int end) {
    for(int i = start; i < end ; i++) {
      region.remove(i);
    }
  }
}
