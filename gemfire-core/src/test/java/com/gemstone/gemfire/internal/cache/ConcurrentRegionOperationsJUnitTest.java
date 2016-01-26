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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This is a multi threaded tests. This test creates two regions. Region1 which
 * is persistent and Region2 which is not. There will be four sets of threads.
 * One set doing put, second one doing gets ,third one doing destroy(key) and
 * the fourth one doing force rolling.
 * 
 * The put are done for Integer Key objects which are random generated and whose
 * values are between 0-9 and the Integer value objects whose value can be
 * between -99 to 99.
 * 
 * Since the keys are only 0-9, this will ensure a high level of concurrency on
 * the same thread since there are more than 10 threads acting at the same time.
 * 
 * After all the operations are done, the two regions are checked for equality.
 * After that the persistent region is closed and recreated so that it can
 * recover the old values and again the two regions are checked for equality.
 *  * This test is run for all modes persist, persist+overflow, overflow only in
 * syn and async mode.
 * 
 * @author Mitul Bid
 *  
 */
@Category(IntegrationTest.class)
public class ConcurrentRegionOperationsJUnitTest extends DiskRegionTestingBase
{

  private int numberOfPutsThreads = 5;

  private int numberOfGetsThreads = 4;

  private int numberOfDestroysThreads = 3;

  private int numberOfClearThreads = 2;

  protected int numberOfForceRollThreads = 3;

  /**
   * ms to run concurrent ops for before signalling them to stop
   */
  protected int TIME_TO_RUN = 1000;

  private boolean exceptionOccuredInPuts = false;

  private boolean exceptionOccuredInGets = false;

  private boolean exceptionOccuredInDestroys = false;

  private boolean exceptionOccuredInClears = false;

  protected boolean exceptionOccuredInForceRolls = false;

  // if this test is to run for a longer time, make this true
  private static final boolean longTest = false;

  protected boolean failure = false;

  private boolean validate;

  protected Region region1;

  private Region region2;

  private Map<Integer, Lock> map = new ConcurrentHashMap<Integer, Lock>();

  private static int counter = 0;

  @Before
  public void setUp() throws Exception
  {
    super.setUp();
    counter++;
    if (longTest) {
      TIME_TO_RUN = 10000;
      numberOfPutsThreads = 5;
      numberOfGetsThreads = 4;
      numberOfDestroysThreads = 3;
      numberOfClearThreads = 2;
      numberOfForceRollThreads = 3;
    }
  }

  @After
  public void tearDown() throws Exception
  {
    super.tearDown();
  }

  @Test
  public void testPersistSyncConcurrency()
  {
    this.validate = true;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    region1 = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, p, Scope.LOCAL);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, p, Scope.LOCAL);
    validate(region1, region2);
  }
  
  @Test
  public void testPersistAsyncConcurrency()
  {
    this.validate = true;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    p.setBytesThreshold(0);
    p.setTimeInterval(1);
    region1 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, p);
    validate(region1, region2);
  }
  
  @Test
  public void testPersistAsyncSmallQueueConcurrency()
  {
    this.validate = true;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    p.setBytesThreshold(1);
    p.setTimeInterval(0);
    region1 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, p);
    validate(region1, region2);
  }
  
  @Test
  public void testPersistAndOverflowSyncConcurrency()
  {
    this.validate = true;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    p.setOverFlowCapacity(1);
    region1 = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        p);
    validate(region1, region2);

  }

  @Test
  public void testPersistAndOverflowAsyncConcurrency()
  {
    this.validate = true;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    p.setBytesThreshold(0);
    p.setTimeInterval(1);
    p.setOverFlowCapacity(1);
    region1 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        p);
    validate(region1, region2);
  }

  @Test
  public void testPersistAndOverflowAsyncSmallQueueConcurrency()
  {
    this.validate = true;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    p.setBytesThreshold(1);
    p.setTimeInterval(0);
    p.setOverFlowCapacity(1);
    region1 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        p);
    validate(region1, region2);
  }
  @Test
  public void testNVPersistSyncConcurrency()
  {
    this.validate = false;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    region1 = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, p,Scope.LOCAL);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, p,Scope.LOCAL);
    validate(region1, region2);
  }
  
  @Test
  public void testNVPersistAsyncConcurrency()
  {
    this.validate = false;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    p.setBytesThreshold(0);
    p.setTimeInterval(1);
    region1 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, p);
    validate(region1, region2);
  }
  
  @Test
  public void testNVPersistAsyncSmallQueueConcurrency()
  {
    this.validate = false;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    p.setBytesThreshold(1);
    p.setTimeInterval(0);
    region1 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, p);
    validate(region1, region2);
  }
  
  @Test
  public void testNVPersistAndOverflowSyncConcurrency()
  {
    this.validate = false;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(100);
    p.setOverFlowCapacity(1);
    region1 = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        p);
    validate(region1, region2);

  }

  @Test
  public void testNVPersistAndOverflowAsyncConcurrency()
  {
    this.validate = false;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    p.setBytesThreshold(0);
    p.setTimeInterval(1);
    p.setOverFlowCapacity(1);
    region1 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        p);
    validate(region1, region2);
  }

  @Test
  public void testNVPersistAndOverflowAsyncSmallQueueConcurrency()
  {
    this.validate = false;
    DiskRegionProperties p = new DiskRegionProperties();
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setCompactionThreshold(99);
    p.setBytesThreshold(1);
    p.setTimeInterval(0);
    p.setOverFlowCapacity(1);
    region1 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        p);
    region2 = concurrencyTest(region1);
    region1 = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        p);
    validate(region1, region2);
  }
  /**
   * Tests the bug where a get operation on an evicted entry fails to get value
   * as the oplog is deleted by the roller, but the entry was not rolled.
   * 
   * @author Asif
   *  
   */
  @Test
  public void testBug35048()
  {
    DiskRegionProperties p = new DiskRegionProperties();
    p.setMaxOplogSize(1000);
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setOverflow(true);
    p.setSynchronous(false);
    p.setOverFlowCapacity(5);
    p.setRolling(true);
    byte[] val = new byte[50];
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, p);
    for (int j = 1; j < 6; ++j) {
      region.put("" + j, val);
    }
    //This will overflow the first entry to disk. Its OplogKeyID will be now
    // positive & value in VM is null
    region.put("" + 6, val);
    //  Do a get opeartion on entry which has been overflown to disk. The value
    // in VM is now not null, but its Oplog KeyID is positive
    region.get("" + 1);
    //Do a force roll
    region.forceRolling();
    //Do a get on entries starting from 2 to 6 & then 1. This will ensure that
    // the entry 1 gets evicted
    //from memory but is not written to disk as its Oplog KeyId is positive.
    // then do a get operation on entry with key as 1. The vale should be
    // correctly retrieved from
    //Htree implying the roller has rolled the entry correctly
    try {
      region.get("" + 2);
      region.get("" + 3);
      region.get("" + 4);
      region.get("" + 5);
      region.get("" + 6);
      try {
        region.get("" + 1);
      }
      catch (Exception e) {
        logWriter.severe("Exception occured  ", e);
        fail("Failed to retrieve value from disk as the Oplog has been rolled but entry still references the Oplog.");
      }

    }
    catch (Exception e) {
      logWriter.severe("Exception occured  ", e);
      fail("Test failed because  of unexpected exception");
    }

    //Now force roll the oplog so that the data exists in the Oplog which has
    // been deleted

  }

  @Test
  public void testConcurrentForceRollingAndGetOperation()
  {
    DiskRegionProperties p = new DiskRegionProperties();
    p.setMaxOplogSize(1000);
    p.setRegionName(this.getName() + counter);
    p.setDiskDirs(dirs);
    p.setOverflow(true);
    p.setSynchronous(false);
    p.setOverFlowCapacity(5);
    p.setRolling(true);
    byte[] val = new byte[50];
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache, p);
    for (int j = 1; j < 101; ++j) {
      region.put("" + j, val);
    }
    Thread t1 = new Thread(new Runnable() {
      public void run()
      {
        for (int i = 0; i < 100; ++i) {
          region.forceRolling();

          try {
            Thread.sleep(TIME_TO_RUN/100);
          }
          catch (InterruptedException e) {
            fail("interrupted");
          }
        }
      }
    });
    Thread t2 = new Thread(new Runnable() {

      public void run()
      {
        try {
          for (int i = 0; i < 100; ++i) {
            for (int j = 1; j < 101; ++j) {
              region.get("" + j);

            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
          failure = true;
        }
      }
    });
    t1.start();
    t2.start();
    DistributedTestCase.join(t1, 30 * 1000, null);
    DistributedTestCase.join(t2, 30 * 1000, null);
    assertTrue(!failure);

  }

  private final AtomicBoolean timeToStop = new AtomicBoolean();
  private boolean isItTimeToStop() {
    return this.timeToStop.get();
  }

  private CyclicBarrier startLine;

  private void waitForAllStartersToBeReady() {
    try {
      startLine.await();
    } catch (InterruptedException ie) {
      fail("unexpected " + ie);
    } catch (BrokenBarrierException ex) {
      fail("unexpected " + ex);
    }
  }
  
  private Region concurrencyTest(Region r1)
  {
    if (this.validate) {
      for (int i = 0; i < 10; i++) {
        map.put(Integer.valueOf(i), new ReentrantLock());
      }
      region2 = cache.createVMRegion("testRegion2", new AttributesFactory()
          .createRegionAttributes());
    }
    this.startLine = new CyclicBarrier(numberOfPutsThreads
                                       + numberOfGetsThreads
                                       + numberOfDestroysThreads
                                       + numberOfClearThreads
                                       + numberOfForceRollThreads);
    DoesPuts doesPuts = new DoesPuts();
    DoesGets doesGets = new DoesGets();
    DoesDestroy doesDestroy = new DoesDestroy();
    DoesClear doesClear = new DoesClear();
    DoesForceRoll doesForceRoll = new DoesForceRoll();
    Thread[] putThreads = new Thread[numberOfPutsThreads];
    Thread[] getThreads = new Thread[numberOfGetsThreads];
    Thread[] destroyThreads = new Thread[numberOfDestroysThreads];
    Thread[] clearThreads = new Thread[numberOfClearThreads];
    Thread[] forceRollThreads = new Thread[numberOfForceRollThreads];
    for (int i = 0; i < numberOfPutsThreads; i++) {
      putThreads[i] = new Thread(doesPuts);
      putThreads[i].setName("PutThread" + i);
    }
    for (int i = 0; i < numberOfGetsThreads; i++) {
      getThreads[i] = new Thread(doesGets);
      getThreads[i].setName("GetThread" + i);
    }
    for (int i = 0; i < numberOfDestroysThreads; i++) {
      destroyThreads[i] = new Thread(doesDestroy);
      destroyThreads[i].setName("DelThread" + i);
    }

    for (int i = 0; i < numberOfClearThreads; i++) {
      clearThreads[i] = new Thread(doesClear);
      clearThreads[i].setName("ClearThread" + i);
    }

    for (int i = 0; i < numberOfForceRollThreads; i++) {
      forceRollThreads[i] = new Thread(doesForceRoll);
      forceRollThreads[i].setName("ForceRoll" + i);
    }
    this.timeToStop.set(false);
    try {
      for (int i = 0; i < numberOfPutsThreads; i++) {
        putThreads[i].start();
      }
      for (int i = 0; i < numberOfGetsThreads; i++) {
        getThreads[i].start();
      }
      for (int i = 0; i < numberOfDestroysThreads; i++) {
        destroyThreads[i].start();
      }
      for (int i = 0; i < numberOfClearThreads; i++) {
        clearThreads[i].start();
      }
      for (int i = 0; i < numberOfForceRollThreads; i++) {
        forceRollThreads[i].start();
      }
      try {
        Thread.sleep(TIME_TO_RUN);
      }
      catch (InterruptedException e) {
        fail("interrupted");
      }
    } finally {
      this.timeToStop.set(true);
    }
    for (int i = 0; i < numberOfPutsThreads; i++) {
      DistributedTestCase.join(putThreads[i], 10*1000, null);
    }
    for (int i = 0; i < numberOfGetsThreads; i++) {
      DistributedTestCase.join(getThreads[i], 10*1000, null);
    }
    for (int i = 0; i < numberOfDestroysThreads; i++) {
      DistributedTestCase.join(destroyThreads[i], 10*1000, null);
    }
    for (int i = 0; i < numberOfClearThreads; i++) {
      DistributedTestCase.join(clearThreads[i], 10*1000, null);
    }
    for (int i = 0; i < numberOfForceRollThreads; i++) {
      DistributedTestCase.join(forceRollThreads[i], 10*1000, null);
    }

    if (this.validate) {
      Collection entrySet = region2.entrySet();
      Iterator iterator = entrySet.iterator();
      Map.Entry mapEntry = null;
      Object key, value = null;
      ((LocalRegion)r1).getDiskRegion().forceFlush();
      while (iterator.hasNext()) {
        mapEntry = (Map.Entry)iterator.next();
        key = mapEntry.getKey();
        value = mapEntry.getValue();
        if (!(r1.containsKey(key))) {
          fail(" region1 does not contain Key " + key
               + " but was expected to be there");
        }
        if (!(((LocalRegion)r1).getValueOnDisk(key).equals(value))) {
          fail(" value for key " + key + " is " + ((LocalRegion)r1).getValueOnDisk(key)
               + " which is not consistent, it is supposed to be " + value);
        }
      }
    }
    r1.close();

    if (exceptionOccuredInDestroys) {
      fail("Exception occured while destroying");
    }
    if (exceptionOccuredInClears) {
      fail("Exception occured while clearing");
    }

    if (exceptionOccuredInForceRolls) {
      fail("Exception occured while force Rolling");
    }

    if (exceptionOccuredInGets) {
      fail("Exception occured while doing gets");
    }

    if (exceptionOccuredInPuts) {
      fail("Exception occured while doing puts");
    }

    return region2;
  }

  void validate(Region r1, Region r2)
  {
    if (!this.validate) return;

    Collection entrySet = r2.entrySet();
    Iterator iterator = entrySet.iterator();
    Map.Entry mapEntry = null;
    Object key, value = null;
    while (iterator.hasNext()) {
      mapEntry = (Map.Entry)iterator.next();
      key = mapEntry.getKey();
      value = mapEntry.getValue();
      if (!(r1.containsKey(key))) {
        fail(" region1 does not contain Key " + key
            + " but was expected to be there");
      }
      if (!(r1.get(key).equals(value))) {
        fail(" value for key " + key + " is " + r1.get(key)
            + " which is not consistent, it is supposed to be " + value);
      }
    }
    assertEquals(r2.size(), r1.size());

    r1.destroyRegion();
    r2.destroyRegion();
  }

  private Random random = new Random();

  void put()
  {
    int randomInt1 = random.nextInt() % 10;
    if (randomInt1 < 0) {
      randomInt1 = randomInt1 * (-1);
    }
    int randomInt2 = random.nextInt() % 100;
    Integer integer1 = Integer.valueOf(randomInt1);
    Integer integer2 = Integer.valueOf(randomInt2);
    Object v = null;
    Object expected = null;
    Lock lock = null;
    if (this.validate) {
      lock = map.get(integer1);
      lock.lock();
    }
    try {
      try {
        v = region1.put(integer1, integer2);
        if (this.validate) {
          expected = region2.put(integer1, integer2);
        }
      }
      catch (Exception e) {
        exceptionOccuredInPuts = true;
        logWriter.severe("Exception occured in puts ", e);
        fail(" failed during put due to " + e);
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
    if (this.validate) {
      if (v != null) {
        assertEquals(expected, v);
      }
    }
  }

  void get()
  {
    int randomInt1 = random.nextInt() % 10;
    if (randomInt1 < 0) {
      randomInt1 = randomInt1 * (-1);
    }

    Integer integer1 = Integer.valueOf(randomInt1);
    Object v = null;
    Object expected = null;
    Lock lock = null;
    if (this.validate) {
      lock = map.get(integer1);
      lock.lock();
    }
    try {
      try {
        v = region1.get(integer1);
        if (this.validate) {
          expected = region2.get(integer1);
        }
      }
      catch (Exception e) {
        exceptionOccuredInGets = true;
        logWriter.severe("Exception occured in get ", e);
        fail(" failed during get due to " + e);
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
    if (this.validate) {
      assertEquals(expected, v);
    }
  }

  void destroy()
  {
    Exception exceptionOccured1 = null;
    Exception exceptionOccured2 = null;
    int randomInt1 = random.nextInt() % 10;
    if (randomInt1 < 0) {
      randomInt1 = randomInt1 * (-1);
    }
    Integer integer1 = Integer.valueOf(randomInt1);
    Object v = null;
    Object expected = null;
    Lock lock = null;
    if (this.validate) {
      lock = map.get(integer1);
      lock.lock();
    }
    try {
      try {
        v = region1.destroy(integer1);
      }
      catch (Exception e) {
        exceptionOccured1 = e;
      }
      if (this.validate) {
        try {
          expected = region2.destroy(integer1);
        }
        catch (Exception e) {
          exceptionOccured2 = e;
        }

        if ((exceptionOccured1!=null) ^ (exceptionOccured2!=null)) {
          exceptionOccuredInDestroys = true;
          logWriter.severe("Exception occured in destroy ex1="+exceptionOccured1
                           + " ex2=" + exceptionOccured2);
          fail("Exception occured in destroy");
        }
      }
    } finally {
      if (lock != null) {
        lock.unlock();
      }
    }
    if (this.validate) {
      if (v != null) {
        assertEquals(expected, v);
      }
    }
  }

  void clear() {
    if (this.validate) {
      return; // can't do clear and validate
    }
    
    try {
      region1.clear();
    }
    catch (Exception e) {
      exceptionOccuredInClears = true;
      logWriter.severe("Exception occured in clear=",e);
      fail("Exception occured in clear");
    }
  }

  /**
   * Bug Test for bug # 35139. This bug was occuring because a clear & region
   * destroy operation occured near concurrently. The region destroy operation
   * notified the roller thread to stop & then it joined with the roller . But
   * by that time clear operation created a new instance of roller thread (
   * because a clear operation stop/starts the roller) & the destroy operation
   * actually joined with the new thread ( different from the one on which
   * notification was issued to exit).
   * 
   * @author Asif
   *  
   */
  @Test
  public void testConcurrentClearAndRegionDestroyBug()
  {
    DiskRegionProperties p = new DiskRegionProperties();
    p.setMaxOplogSize(10000);
    p.setOverflow(false);
    p.setSynchronous(true);
    p.setOverFlowCapacity(5);
    p.setRolling(true);
    byte[] val = new byte[8000];
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, p, Scope.LOCAL);
    region.put("key1", val);
    DiskStoreImpl dimpl = ((LocalRegion)region).getDiskStore();
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    final Thread th = new Thread(new Runnable() {

      public void run()
      {
        region.destroyRegion();
      }
    });

    DiskStoreImpl.DEBUG_DELAY_JOINING_WITH_COMPACTOR = 8000;
    CacheObserver old = CacheObserverHolder
        .setInstance(new CacheObserverAdapter() {
          boolean skip = false;

          public void beforeStoppingCompactor()
          {
            if (!skip) {
              skip = true;
              th.setPriority(9);
              th.start();
              Thread.yield();
            }
          }

        }

        );

    region.clear();
    DistributedTestCase.join(th, 20 * 1000, null);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    DiskStoreImpl.DEBUG_DELAY_JOINING_WITH_COMPACTOR = 500;
    CacheObserverHolder.setInstance(old);
  }

  @SuppressWarnings("synthetic-access")
  class DoesPuts implements Runnable
  {

    public void run()
    {
      waitForAllStartersToBeReady();
      while (!isItTimeToStop()) {
        put();
      }
    }

  }

  @SuppressWarnings("synthetic-access")
  class DoesGets implements Runnable
  {

    public void run()
    {
      waitForAllStartersToBeReady();
      while (!isItTimeToStop()) {
        get();
      }
    }

  }

  @SuppressWarnings("synthetic-access")
  class DoesDestroy implements Runnable
  {

    public void run()
    {
      waitForAllStartersToBeReady();
      while (!isItTimeToStop()) {
        destroy();
      }
    }

  }

  @SuppressWarnings("synthetic-access")
  class DoesClear implements Runnable  {
    public void run()
    {
      waitForAllStartersToBeReady();
      while (!isItTimeToStop()) {
        try {
          Thread.sleep(TIME_TO_RUN/100);
        }
        catch (InterruptedException e) {
          fail("interrupted");
        }
        clear();
      }
    }
  }

  @SuppressWarnings("synthetic-access")
  class DoesForceRoll implements Runnable
  {

    public void run()
    {
      waitForAllStartersToBeReady();
      while (!isItTimeToStop()) {
        try {
          Thread.sleep(20);
        }
        catch (InterruptedException e) {
          fail("interrupted");
        }
        forceRoll();
      }
    }

    private void forceRoll()
    {
      try {
        region1.forceRolling();
      }
      catch (Exception e) {
        exceptionOccuredInForceRolls = true;
        logWriter.severe("Exception occured in forceRolling ", e);
        fail(" Exception occured here");

      }
    }

  }

}
