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

import java.io.File;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Bug40299 DUNIT Test: The Clear operation during a NetSearchMessage.doGet() in progress can cause
 * DiskAccessException by accessing cleared oplogs and eventually destroy region. The Test verifies
 * that fix prevents this.
 */
@Category(DistributedTest.class)
public class Bug40299DUnitTest extends JUnit4CacheTestCase {

  protected static String regionName = "TestRegion";

  static Properties props = new Properties();

  protected static DistributedSystem distributedSystem = null;

  private static VM vm0 = null;

  protected static Cache cache = null;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    vm0.invoke(destroyRegion());
  }

  /**
   * This method is used to create Cache in VM0
   * 
   * @return CacheSerializableRunnable
   */

  private CacheSerializableRunnable createCacheForVM0() {
    SerializableRunnable createCache = new CacheSerializableRunnable("createCache") {
      public void run2() {
        try {

          distributedSystem = (new Bug40299DUnitTest()).getSystem(props);
          assertTrue(distributedSystem != null);
          cache = CacheFactory.create(distributedSystem);
          assertTrue(cache != null);
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          File dir = new File("testingDirectoryDefault");
          dir.mkdir();
          dir.deleteOnExit();
          File[] dirs = {dir};
          factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
          factory.setDiskSynchronous(false);
          factory.setDiskStoreName(cache.createDiskStoreFactory()
              .setDiskDirsAndSizes(dirs, new int[] {Integer.MAX_VALUE}).setQueueSize(1)
              .setMaxOplogSize(60) // does the test want 60 bytes or 60M?
              .setAutoCompact(false).setTimeInterval(1000).create("Bug40299DUnitTest").getName());
          factory.setEvictionAttributes(
              EvictionAttributes.createLRUEntryAttributes(2, EvictionAction.OVERFLOW_TO_DISK));
          RegionAttributes attr = factory.create();
          cache.createRegion(regionName, attr);
        } catch (Exception ex) {
          ex.printStackTrace();
          fail("Error Creating cache / region ");
        }
      }
    };
    return (CacheSerializableRunnable) createCache;
  }

  /**
   * This method puts in 7 in the Region
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable putSomeEntries() {
    SerializableRunnable puts = new CacheSerializableRunnable("putSomeEntries") {
      public void run2() {
        assertTrue("Cache is found as null ", cache != null);
        Region rgn = cache.getRegion(regionName);
        for (int i = 0; i < 7; i++) {
          rgn.put("key" + i, new Long(i));
        }
      }
    };
    return (CacheSerializableRunnable) puts;
  }

  /**
   * This method does concurrent NetSearch.doGet with clear in the Region
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable concurrentNetSearchGetAndClear() {
    SerializableRunnable getAndClear =
        new CacheSerializableRunnable("concurrentNetSearchGetAndClear") {
          public void run2() {
            assertTrue("Cache is found as null ", cache != null);
            Region rgn = cache.getRegion(regionName);
            assertTrue("Region size expected to be 7 but is " + rgn.size(), rgn.size() == 7);

            Thread getThread1 = null;
            LocalRegion lr = (LocalRegion) rgn;
            lr.getDiskRegion().acquireWriteLock();
            // got writeLock from diskregion
            try {
              getThread1 = new Thread(new getThread((LocalRegion) rgn));

              // start getThread
              getThread1.start();

              // sleep for a while to allow getThread to wait for readLock.
              Thread.sleep(1000);

              // This test appears to be testing a problem with the non-RVV
              // based clear. So we'll use that functionality here.
              // Region.clear uses an RVV, and will deadlock if called while
              // the write lock is held.
              RegionEventImpl regionEvent = new RegionEventImpl(lr, Operation.REGION_CLEAR, null,
                  false, lr.getMyId(), lr.generateEventID());
              // clearRegion to remove entry that getThread has reference of
              lr.cmnClearRegion(regionEvent, true, false);
            } catch (InterruptedException e) {
              if (cache.getLogger().fineEnabled()) {
                cache.getLogger().fine("InterruptedException in run of localClearThread");
              }
            } finally {
              ((LocalRegion) rgn).getDiskRegion().releaseWriteLock();
            }
            // allow getThread to join to set getAfterClearSuccessful
            try {
              getThread1.join();
            } catch (InterruptedException ie) {
              if (cache.getLogger().fineEnabled()) {
                cache.getLogger().fine("InterruptedException in join of getThread");
              }
            }
          }
        };

    return (CacheSerializableRunnable) getAndClear;
  }

  /**
   * 
   * getThread
   * 
   */
  protected class getThread implements Runnable {
    LocalRegion region = null;

    getThread(LocalRegion rgn) {
      super();
      this.region = rgn;
    }

    public void run() {

      SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
      processor.initialize((LocalRegion) region, "key1", null);
      processor.testNetSearchMessageDoGet(region.getName(), "key1", 1500, 1500, 1500);
    }
  }

  /**
   * This method verifies that region is not destroyed
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable verifyRegionNotDestroyed() {
    SerializableRunnable verifyR = new CacheSerializableRunnable("verifyRegionNotDestroyed") {
      public void run2() {
        assertTrue("Cache is found as null ", cache != null);
        Region region = cache.getRegion(regionName);
        assertTrue("Region was destroyed", region != null);
      }
    };
    return (CacheSerializableRunnable) verifyR;
  }

  /**
   * This method destroys the Region
   * 
   * @return CacheSerializableRunnable
   */
  private CacheSerializableRunnable destroyRegion() {
    SerializableRunnable destroyR = new CacheSerializableRunnable("destroyRegion") {
      public void run2() {
        try {
          assertTrue("Cache is found as null ", cache != null);

          Region rgn = cache.getRegion(regionName);
          rgn.localDestroyRegion();
          cache.close();
        } catch (Exception ex) {

        }
      }
    };
    return (CacheSerializableRunnable) destroyR;
  }

  /**
   * The Clear operation during a NetSearchMessage.doGet() in progress can cause DiskAccessException
   * by accessing cleared oplogs and eventually destroy region. The Test verifies that fix prevents
   * this.
   */

  @Test
  public void testQueryGetWithClear() {
    IgnoredException.addIgnoredException("Entry has been cleared and is not present on disk");
    // create region in VM0
    vm0.invoke(createCacheForVM0());
    // Do puts to region.
    vm0.invoke(putSomeEntries());
    // call NetSearchMessage.doGet() after region.clear()
    vm0.invoke(concurrentNetSearchGetAndClear());
    // verify that region is not destroyed
    vm0.invoke(verifyRegionNotDestroyed());
  }
}
