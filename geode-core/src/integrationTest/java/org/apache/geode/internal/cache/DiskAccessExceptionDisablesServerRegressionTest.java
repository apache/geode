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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;

/**
 * This is a bugtest for bug 37500.
 *
 * <p>
 * TRAC #37500: A DiskAccessException brings cache server to a stall
 *
 * <pre>
 * com.gemstone.gemfire.cache.DiskAccessException:  Unable to get free space for creating an oplog after waiting for 20 seconds
 *     at com.gemstone.gemfire.internal.cache.ComplexDiskRegion.getNextDir(ComplexDiskRegion.java:150)
 *     at com.gemstone.gemfire.internal.cache.Oplog.switchOpLog(Oplog.java:2020)
 *     at com.gemstone.gemfire.internal.cache.Oplog.basicModify(Oplog.java:2423)
 *     at com.gemstone.gemfire.internal.cache.Oplog.modify(Oplog.java:2339)
 *     at com.gemstone.gemfire.internal.cache.DiskRegion.put(DiskRegion.java:321)
 *     at com.gemstone.gemfire.internal.cache.DiskEntry$Helper.writeToDisk(DiskEntry.java:362)
 *     at com.gemstone.gemfire.internal.cache.DiskEntry$Helper.overflowToDisk(DiskEntry.java:532)
 *     at com.gemstone.gemfire.internal.cache.AbstractLRURegionMap.evictEntry(AbstractLRURegionMap.java:164)
 *     at com.gemstone.gemfire.internal.cache.AbstractLRURegionMap.lruUpdateCallback(AbstractLRURegionMap.java:240)
 *     at com.gemstone.gemfire.internal.cache.AbstractRegionMap.basicPut(AbstractRegionMap.java:928)
 *     at com.gemstone.gemfire.internal.cache.LocalRegion.virtualPut(LocalRegion.java:3605)
 *     at com.gemstone.gemfire.internal.cache.DistributedRegion.virtualPut(DistributedRegion.java:151)
 *     at com.gemstone.gemfire.internal.cache.LocalRegion.basicUpdate(LocalRegion.java:3591)
 *     at com.gemstone.gemfire.internal.cache.AbstractUpdateOperation.doPutOrCreate(AbstractUpdateOperation.java:100)
 *     at com.gemstone.gemfire.internal.cache.AbstractUpdateOperation$AbstractUpdateMessage.basicOperateOnRegion(AbstractUpdateOperation.java:171)
 *     at com.gemstone.gemfire.internal.cache.AbstractUpdateOperation$AbstractUpdateMessage.operateOnRegion(AbstractUpdateOperation.java:154)
 *     at com.gemstone.gemfire.internal.cache.DistributedCacheOperation$CacheOperationMessage.basicProcess(DistributedCacheOperation.java:487)
 *     at com.gemstone.gemfire.internal.cache.DistributedCacheOperation$CacheOperationMessage.process(DistributedCacheOperation.java:404)
 *     at com.gemstone.gemfire.distributed.internal.ClusterMessage.scheduleAction(ClusterMessage.java:242)
 *     at com.gemstone.gemfire.distributed.internal.ClusterMessage.schedule(ClusterMessage.java:287)
 *     at com.gemstone.gemfire.distributed.internal.DistributionManager.scheduleIncomingMessage(DistributionManager.java:2732)
 *     at com.gemstone.gemfire.distributed.internal.DistributionManager.handleIncomingDMsg(DistributionManager.java:2424)
 *     at com.gemstone.gemfire.distributed.internal.DistributionManager$MyListener.messageReceived(DistributionManager.java:3585)
 *     at com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMembershipManager.processMessage(JGroupMembershipManager.java:1349)
 *     at com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMembershipManager.handleOrDeferMessage(JGroupMembershipManager.java:1289)
 *     at com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMembershipManager$MyDCReceiver.messageReceived(JGroupMembershipManager.java:449)
 *     at com.gemstone.gemfire.distributed.internal.direct.DirectChannel.receive(DirectChannel.java:535)
 *     at com.gemstone.gemfire.internal.tcp.TCPConduit.messageReceived(TCPConduit.java:483)
 *     at com.gemstone.gemfire.internal.tcp.Connection.dispatchMessage(Connection.java:3026)
 *     at com.gemstone.gemfire.internal.tcp.Connection.processNIOBuffer(Connection.java:2861)
 *     at com.gemstone.gemfire.internal.tcp.Connection.runNioReader(Connection.java:1332)
 *     at com.gemstone.gemfire.internal.tcp.Connection.run(Connection.java:1257)
 *     at java.lang.Thread.run(Thread.java:595)
 * </pre>
 */
public class DiskAccessExceptionDisablesServerRegressionTest {

  private static final int MAX_OPLOG_SIZE = 1000;
  private static final String KEY1 = "KEY1";
  private static final String KEY2 = "KEY2";

  private Cache cache;
  private Region<String, byte[]> region;
  private MyCacheObserver observer;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    File temporaryDirectory = temporaryFolder.newFolder(uniqueName);

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    observer = new MyCacheObserver();
    CacheObserverHolder.setInstance(observer);

    cache = new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();

    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setDiskDirsAndSizes(new File[] {temporaryDirectory}, new int[] {2000});
    ((DiskStoreFactoryImpl) dsf).setMaxOplogSizeInBytes(MAX_OPLOG_SIZE);
    ((DiskStoreFactoryImpl) dsf).setDiskDirSizesUnit((DiskDirSizesUnit.BYTES));
    DiskStore diskStore = dsf.create(uniqueName);

    RegionFactory<String, byte[]> regionFactory =
        cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT);
    regionFactory.setDiskStoreName(diskStore.getName());

    region = regionFactory.create(uniqueName);
  }

  @After
  public void tearDown() {
    CacheObserverHolder.setInstance(null);

    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

    cache.close();
  }

  /**
   * This test does the following: <br>
   * 1. Create a disk-region with following configurations:
   * <ul>
   * <li>dirSize = 2000 bytes
   * <li>maxOplogSize = 500 bytes
   * <li>rolling = true
   * <li>syncMode = true
   * <li>approx size on disk for operations = 440 bytes
   * </ul>
   *
   * <p>
   * 2.Make Roller go into WAIT state via CacheObserverAdapter.beforeGoingToCompact callback
   *
   * <p>
   * 3.Put 440 bytes , it will go in oplog1
   *
   * <p>
   * 4.Put another 440 bytes ,it will go in oplog1
   *
   * <p>
   * 5.Put 440 bytes , switching will be caused, it will go in oplog2, Roller will remained blocked
   * (step 2)
   *
   * <p>
   * 6.Put 440 bytes , it will go in oplog2, oplog2 will now be full
   *
   * <p>
   * 7.Notify the Roller and put 440 bytes , this will try further switching. The put will fail with
   * exception due to bug 37500. The put thread takes an entry level lock for entry2 ( the one with
   * KEY2) and tries to write to disk but there is no free space left, so it goes into wait,
   * expecting Roller to free up the space. The roller, which has now been notified to run, tries to
   * roll entry2 for which it seeks entry level lock which has been acquired by put-thread. So the
   * put thread eventually comes out of the wait with DiskAccessException
   *
   * <p>
   * Another scenario for this bug is, once the disk space was getting exhausted , the entry
   * operation threads which had already taken a lock on Entry got stuck trying to seek the Oplog
   * Lock. The switching thread had acquired the Oplog.lock & was waiting for the roller thread to
   * free disk space. Since the roller needed to acquire Entry lock to roll, it was unable to do so
   * because of entry operation threads. This would cause the entry operation threads to get
   * DiskAccessException after completing the stipulated wait. The Roller was able to free space
   * only when it has rolled all the relevant entries which could happen only when the entry
   * operation threads released the entry lock after getting DiskAccessException.
   */
  @Test
  public void testBug37500() throws Exception {
    // put 440 bytes , it will go in oplog1
    region.put(KEY1, new byte[420]);

    // put another 440 bytes ,it will go in oplog1
    region.put(KEY2, new byte[420]);

    // put 440 bytes , switching will be caused, it will go in oplog2 (value
    // size increased to 432 as key wont be written to disk for UPDATE)
    region.put(KEY1, new byte[432]);

    // put 440 bytes , it will go in oplog2
    region.put(KEY1, new byte[432]);

    observer.notifyRoller();

    // put 440 bytes , this will try further switching
    region.put(KEY2, new byte[432]);
  }

  private static class MyCacheObserver extends CacheObserverAdapter {

    private final Object notification = new Object();

    /**
     * Flag to decide whether we want to allow roller to run
     */
    private volatile boolean notifyRoller = false;

    private volatile boolean proceedForRolling = false;

    void notifyRoller() {
      notifyRoller = true;
    }

    @Override
    public void beforeGoingToCompact() {
      synchronized (notification) {
        while (!proceedForRolling) {
          try {
            notification.wait();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    @Override
    public void beforeSwitchingOplog() {
      if (notifyRoller) {
        synchronized (notification) {
          proceedForRolling = true;
          notification.notifyAll();
        }
      }
    }
  }
}
