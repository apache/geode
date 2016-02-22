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
package com.gemstone.gemfire.internal.cache.ha;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class BlockingHARegionJUnitTest
{
  static Cache cache = null;

  @Before
  public void setUp() throws Exception
  {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    if (cache != null) {
      cache.close(); // fault tolerance
    }
    cache = CacheFactory.create(DistributedSystem
      .connect(props));
  }

/**
 * This test has a scenario where the HAReqionQueue capacity is just 1. There will
 * be two thread. One doing a 1000 puts and the other doing a 1000 takes. The validation
 * for this test is that it should not encounter any exceptions
 *
 */
  @Test
  public void testBoundedPuts()
  {
    try {
      exceptionOccured = false;
      HARegionQueueAttributes harqa = new HARegionQueueAttributes();
      harqa.setBlockingQueueCapacity(1);
      HARegionQueue hrq = HARegionQueue.getHARegionQueueInstance(
          "BlockingHARegionJUnitTest_Region", cache, harqa,
          HARegionQueue.BLOCKING_HA_QUEUE, false);
      hrq.setPrimary(true);//fix for 40314 - capacity constraint is checked for primary only.
      Thread thread1 = new DoPuts(hrq,1000);
      Thread thread2 = new DoTake(hrq,1000);

      thread1.start();
      thread2.start();

      ThreadUtils.join(thread1, 30 * 1000);
      ThreadUtils.join(thread2, 30 * 1000);

      if (exceptionOccured) {
        fail(" Test failed due to " + exceptionString);
      }
      
      cache.close();

    }
    catch (Exception e) {
      fail(" Test encountered an exception "+e);
    }

  }

  /**
   * This test tests whether puts are blocked. There are two threads. One which is going
   * to do 2 puts and one which is going to do take a single take. The capacity of the
   * region is just 1. The put thread is first started and it is then ensured that only one
   * put has successfully made through and that the thread is still alive. Then the take thread
   * is started. This will cause the region size to come down by one and the put thread waiting
   * will go ahead and do the put. The thread should then die and the region size should be validated
   * to reflect that.
   *
   */
  @Test
  public void testPutBeingBlocked()
  {
    try {
      exceptionOccured = false;
      quitForLoop = false;
      HARegionQueueAttributes harqa = new HARegionQueueAttributes();
      harqa.setBlockingQueueCapacity(1);
      final HARegionQueue hrq = HARegionQueue.getHARegionQueueInstance(
          "BlockingHARegionJUnitTest_Region", cache, harqa,
          HARegionQueue.BLOCKING_HA_QUEUE, false);
      hrq.setPrimary(true);//fix for 40314 - capacity constraint is checked for primary only.
      final Thread thread1 = new DoPuts(hrq,2);
      thread1.start();
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return hrq.region.size() == 2;
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 1000, 200, true);
      assertTrue(thread1.isAlive()); //thread should still be alive (in wait state)
      
      Thread thread2 = new DoTake(hrq,1);
      thread2.start(); //start take thread
      ev = new WaitCriterion() {
        public boolean done() {
          return hrq.region.size() == 3;
        }
        public String description() {
          return null;
        }
      };
      //sleep. take will proceed and so will sleeping put
      Wait.waitForCriterion(ev, 3 * 1000, 200, true);

      // thread should have died since put should have proceeded
      ev = new WaitCriterion() {
        public boolean done() {
          return !thread1.isAlive();
        }
        public String description() {
          return "thread1 still alive";
        }
      };
      Wait.waitForCriterion(ev, 30 * 1000, 1000, true);
      
      ThreadUtils.join(thread1, 30 * 1000); // for completeness
      ThreadUtils.join(thread2, 30 * 1000);
      if (exceptionOccured) {
        fail(" Test failed due to " + exceptionString);
      }
      cache.close();
    }
    catch (Exception e) {
      fail(" Test encountered an exception "+e);
    }
  }

  
  /**
   * This test tests that the region capacity is never exceeded even in highly
   * concurrent environments. The region capacity is set to 10000. Then 5 threads start doing
   * put simultaneously. They will reach a state where the queue is full and they will all
   * go in a wait state. the region size would be verified to be 20000 (10000 puts and 10000 DACE objects).
   * then the threads are interrupted and made to quit the loop
   *
   */
  @Test
  public void testConcurrentPutsNotExceedingLimit()
  {
    try {
      exceptionOccured = false;
      quitForLoop = false;
      HARegionQueueAttributes harqa = new HARegionQueueAttributes();
      harqa.setBlockingQueueCapacity(10000);
      final HARegionQueue hrq = HARegionQueue.getHARegionQueueInstance(
          "BlockingHARegionJUnitTest_Region", cache, harqa,
          HARegionQueue.BLOCKING_HA_QUEUE, false);      
      hrq.setPrimary(true);//fix for 40314 - capacity constraint is checked for primary only.
      Thread thread1 = new DoPuts(hrq,20000,1);
      Thread thread2 = new DoPuts(hrq,20000,2);
      Thread thread3 = new DoPuts(hrq,20000,3);
      Thread thread4 = new DoPuts(hrq,20000,4);
      Thread thread5 = new DoPuts(hrq,20000,5);
      
      thread1.start();
      thread2.start();
      thread3.start();
      thread4.start();
      thread5.start();
      
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return hrq.region.size() == 20000;
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 30 * 1000, 200, true);
      
      assertTrue(thread1.isAlive());
      assertTrue(thread2.isAlive());
      assertTrue(thread3.isAlive());
      assertTrue(thread4.isAlive());
      assertTrue(thread5.isAlive());
      
      assertTrue(hrq.region.size()==20000);
      
      quitForLoop = true;
      Thread.sleep(20000);
      
      thread1.interrupt();
      thread2.interrupt();
      thread3.interrupt();
      thread4.interrupt();
      thread5.interrupt();
      
      Thread.sleep(2000);
      
      ThreadUtils.join(thread1, 5 * 60 * 1000);
      ThreadUtils.join(thread2, 5 * 60 * 1000);
      ThreadUtils.join(thread3, 5 * 60 * 1000);
      ThreadUtils.join(thread4, 5 * 60 * 1000);
      ThreadUtils.join(thread5, 5 * 60 * 1000);
      
      cache.close();
    }
    catch (Exception e) {
      fail(" Test encountered an exception "+e);
    }
  }
  
  /**
   * This test tests that the region capacity is never exceeded even in highly
   * concurrent environments. The region capacity is set to 10000. Then 5 threads start doing
   * put simultaneously. They will reach a state where the queue is full and they will all
   * go in a wait state. the region size would be verified to be 20000 (10000 puts and 10000 DACE objects).
   * then the threads are interrupted and made to quit the loop
   *
   *TODO:
   *
   */
  public void _testConcurrentPutsTakesNotExceedingLimit()
  {
    try {
      exceptionOccured = false;
      quitForLoop = false;
      HARegionQueueAttributes harqa = new HARegionQueueAttributes();
      harqa.setBlockingQueueCapacity(10000);
      final HARegionQueue hrq = HARegionQueue.getHARegionQueueInstance(
          "BlockingHARegionJUnitTest_Region", cache, harqa,
          HARegionQueue.BLOCKING_HA_QUEUE, false);
      Thread thread1 = new DoPuts(hrq,40000,1);
      Thread thread2 = new DoPuts(hrq,40000,2);
      Thread thread3 = new DoPuts(hrq,40000,3);
      Thread thread4 = new DoPuts(hrq,40000,4);
      Thread thread5 = new DoPuts(hrq,40000,5);
      
      Thread thread6 = new DoTake(hrq,5000);
      Thread thread7 = new DoTake(hrq,5000);
      Thread thread8 = new DoTake(hrq,5000);
      Thread thread9 = new DoTake(hrq,5000);
      Thread thread10 = new DoTake(hrq,5000);
      
      thread1.start();
      thread2.start();
      thread3.start();
      thread4.start();
      thread5.start();
      
      thread6.start();
      thread7.start();
      thread8.start();
      thread9.start();
      thread10.start();
      
      ThreadUtils.join(thread6, 30 * 1000);
      ThreadUtils.join(thread7, 30 * 1000);
      ThreadUtils.join(thread8, 30 * 1000);
      ThreadUtils.join(thread9, 30 * 1000);
      ThreadUtils.join(thread10, 30 * 1000);
      
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return hrq.region.size() == 20000;  
        }
        public String description() {
          return null;
        }
      };
      Wait.waitForCriterion(ev, 30 * 1000, 200, true);
      
      assertTrue(thread1.isAlive());
      assertTrue(thread2.isAlive());
      assertTrue(thread3.isAlive());
      assertTrue(thread4.isAlive());
      assertTrue(thread5.isAlive());
      
      assertTrue(hrq.region.size()==20000);
      
      quitForLoop = true;
      
      Thread.sleep(2000);
      
      thread1.interrupt();
      thread2.interrupt();
      thread3.interrupt();
      thread4.interrupt();
      thread5.interrupt();
      
      Thread.sleep(2000);
      
      
      ThreadUtils.join(thread1, 30 * 1000);
      ThreadUtils.join(thread2, 30 * 1000);
      ThreadUtils.join(thread3, 30 * 1000);
      ThreadUtils.join(thread4, 30 * 1000);
      ThreadUtils.join(thread5, 30 * 1000);
      
      cache.close();
    }
    catch (Exception e) {
      fail(" Test encountered an exception "+e);
    }
  }
  
  
  /**
   * Tests the bug in HARegionQueue where the take side put permit is not being
   * incremented   when the event arriving at the queue which has optimistically
   * decreased the put permit, is not incrementing the take permit if the event
   * has a sequence ID less than the last dispatched sequence ID. This event is
   * rightly rejected from entering the queue but the take permit also needs to
   * increase & a notify issued  
   * 
   */  
  @Test
  public void testHARQMaxCapacity_Bug37627()
  {
    try {
      exceptionOccured = false;
      quitForLoop = false;
      HARegionQueueAttributes harqa = new HARegionQueueAttributes();
      harqa.setBlockingQueueCapacity(1);
      harqa.setExpiryTime(180);
      final HARegionQueue hrq = HARegionQueue.getHARegionQueueInstance(
          "BlockingHARegionJUnitTest_Region", cache, harqa,
          HARegionQueue.BLOCKING_HA_QUEUE, false);
      hrq.setPrimary(true);//fix for 40314 - capacity constraint is checked for primary only.
      final EventID id1 = new EventID(new byte[] { 1 }, 1, 2); // violation
      final EventID ignore = new EventID(new byte[] { 1 }, 1, 1); //
      final EventID id2 = new EventID(new byte[] { 1 }, 1, 3); //
      Thread t1 = new Thread() {
        public void run()
        {
          try {
            hrq.put(new ConflatableObject("key1", "value1", id1, false,
                "region1"));
            hrq.take();
            hrq.put(new ConflatableObject("key2", "value1", ignore, false,
                "region1"));
            hrq.put(new ConflatableObject("key3", "value1", id2, false,
                "region1"));
          }
          catch (Exception e) {
            exceptionString.append("First Put in region queue failed");
            exceptionOccured = true;
          }
        }
      };
      t1.start();
      ThreadUtils.join(t1, 20 * 1000);
      if (exceptionOccured) {
        fail(" Test failed due to " + exceptionString);
      }
    }
    catch (Exception e) {
      fail(" Test failed due to " + e);
    }
    finally {
      if (cache != null) {
        cache.close();
      }
    }

  }
  
  

  /** boolean to record an exception occurence in another thread**/
  static volatile boolean exceptionOccured = false;
/** StringBuffer to store the exception**/
  static StringBuffer exceptionString = new StringBuffer();
  /** boolen to quit the for loop**/
  static volatile boolean quitForLoop = false;

  /**
   * class which does specified number of puts on the queue
   * @author mbid
   *
   */
  static class DoPuts extends Thread
  {
    HARegionQueue regionQueue = null;
    final int numberOfPuts;
    DoPuts(HARegionQueue haRegionQueue, int numberOfPuts) {
      this.regionQueue = haRegionQueue;
      this.numberOfPuts = numberOfPuts;
    }
/**
 * region id can be specified to generate Thread unique events
 */
    int regionId = 0;
    DoPuts(HARegionQueue haRegionQueue, int numberOfPuts, int regionId) {
      this.regionQueue = haRegionQueue;
      this.numberOfPuts = numberOfPuts;
      this.regionId = regionId;
    }
    
    public void run()
    {
      for (int i = 0; i < numberOfPuts; i++) {
        try {
          this.regionQueue.put(new ConflatableObject("" + i, "" + i,
              new EventID(new byte[regionId], i, i), false,
              "BlockingHARegionJUnitTest_Region"));
          if(quitForLoop){
            break;
          }
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
        }
        catch (Exception e) {
          exceptionOccured = true;
          exceptionString.append(" Exception occured due to " + e);
        }
      }
    }
  }

  /**
   * class which does a specified number of takes
   * @author mbid
   *
   */
  static class DoTake extends Thread
  {
    final HARegionQueue regionQueue;
    final int numberOfTakes;

    DoTake(HARegionQueue haRegionQueue, int numberOfTakes) {
      this.regionQueue = haRegionQueue;
      this.numberOfTakes = numberOfTakes;
    }

    public void run()
    {
      for (int i = 0; i < numberOfTakes; i++) {
        try {
          assertNotNull(this.regionQueue.take());
        }
        catch (Exception e) {
          exceptionOccured = true;
          exceptionString.append(" Exception occured due to " + e);
        }
      }
    }
  }

}
