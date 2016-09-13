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
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import static org.junit.Assert.*;

/**
 * This is a bugtest for bug 37500.
 * 
 * 
 */
@Category(IntegrationTest.class)
public class Bug37500JUnitTest extends DiskRegionTestingBase
{

  /** The disk region configuration object for the test */
  private DiskRegionProperties diskProps = new DiskRegionProperties();

  /** The key for entry1 */
  static final String KEY1 = "KEY1";

  /** The key for entry2 */
  static final String KEY2 = "KEY2";

  /** Boolean to indicate the roller thread to proceed */
  static volatile boolean proceedForRolling = false;

  /**
   * Boolean to decide whether we want to allow roller to run ( used via
   * CacheObserver callback
   */
  static volatile boolean notifyRoller = false;

  /**
   * This test does the following: <br>
   * 1. Create a disk-region with following configurations :
   * <li>dirSize = 2000 bytes
   * <li>maxOplogSize = 500 bytes
   * <li>rolling = true
   * <li>syncMode = true
   * <li>approx size on disk for operations = 440 bytes<br>
   * 
   * 2.Make Roller go into WAIT state via CacheObserverAdapter.beforeGoingToCompact
   * callback<br>
   * 3.Put 440 bytes , it will go in oplog1 <br>
   * 4.Put another 440 bytes ,it will go in oplog1<br>
   * 5.Put 440 bytes , switching will be caused, it will go in oplog2, Roller
   * will remained blocked (step 2)<br>
   * 6.Put 440 bytes , it will go in oplog2, oplog2 will now be full<br>
   * 7.Notify the Roller and put 440 bytes , this will try further switching.
   * The put will fail with exception due to bug 37500. The put thread takes an
   * entry level lock for entry2 ( the one with KEY2) and tries to write to disk
   * but there is no free space left, so it goes into wait, expecting Roller to
   * free up the space. The roller, which has now been notified to run, tries to
   * roll entry2 for which it seeks entry level lock which has been acquired by
   * put-thread. So the put thread eventually comes out of the wait with
   * DiskAccessException<br>
   * 
   * Another scenario for this bug is, once the disk space was getting exhausted ,
   * the entry operation threads which had already taken a lock on Entry got
   * stuck trying to seek the Oplog Lock. The switching thread had acquired the
   * Oplog.lock & was waiting for the roller thread to free disk space. Since
   * the roller needed to acquire Entry lock to roll, it was unable to do so
   * because of entry operation threads. This would cause the entry operation
   * threads to get DiskAccessException after completing the stipulated wait.
   * The Roller was able to free space only when it has rolled all the relevant
   * entries which could happen only when the entry operation threads released
   * the entry lock after getting DiskAccessException.
   * 
   * 
   * @throws Exception
   */
  @Test
  public void testBug37500() throws Exception
  {
    final int MAX_OPLOG_SIZE = 1000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setSynchronous(false);

    File testdir = new File("bug37500-diskDir");
    testdir.mkdir();
    testdir.deleteOnExit();
    diskProps.setDiskDirsAndSizes(new File[] { testdir }, new int[] { 2000 });

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    CacheObserver old = CacheObserverHolder
        .setInstance(new CacheObserverAdapter() {
          public void beforeGoingToCompact()
          {
            if (!proceedForRolling) {
              synchronized (Bug37500JUnitTest.class) {
                if (!proceedForRolling) {
                  try {
                    cache.getLogger().info(
                        "beforeGoingToCompact :: going into wait");
                    Bug37500JUnitTest.class.wait();
                  }
                  catch (InterruptedException e) {
                    cache.getLogger().info("Roller interrupted");
                    fail("interrupted");
                  }
                  cache.getLogger().info(
                      "beforeGoingToCompact :: coming out of wait");
                }
              }
            }
          }

          public void beforeSwitchingOplog()
          {
            if (notifyRoller) {
              cache.getLogger().info(
                  "beforeSwitchingOplog :: going to notify Roller");
              synchronized (Bug37500JUnitTest.class) {
                proceedForRolling = true;
                Bug37500JUnitTest.class.notify();
                cache.getLogger().info(
                    "beforeSwitchingOplog :: notified the Roller");
              }
            }

          }
        });

    cache.getLogger().info("goin to put no. 1");
    // put 440 bytes , it will go in oplog1
    region.put(KEY1, new byte[420]);

    cache.getLogger().info("goin to put no. 2");
    // put another 440 bytes ,it will go in oplog1
    region.put(KEY2, new byte[420]);

    cache.getLogger().info("goin to put no. 3");
    // put 440 bytes , switching will be caused, it will go in oplog2 (value
    // size increased to 432 as key wont be written to disk for UPDATE)
    region.put(KEY1, new byte[432]);

    cache.getLogger().info("goin to put no. 4");
    // put 440 bytes , it will go in oplog2
    region.put(KEY1, new byte[432]);

    notifyRoller = true;
    cache.getLogger().info("goin to put no. 5");
    // put 440 bytes , this will try further switching
    region.put(KEY2, new byte[432]);

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    CacheObserverHolder.setInstance(old);
    closeDown();
  }

}
