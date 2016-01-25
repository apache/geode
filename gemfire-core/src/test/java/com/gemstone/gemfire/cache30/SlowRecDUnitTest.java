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

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;

import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.internal.tcp.Connection;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * Test to make sure slow receiver queuing is working
 *
 * @author darrel
 * @since 4.2.1
 */
@Category(DistributedTest.class)
@Ignore("Test was disabled by renaming to DisabledTest")
public class SlowRecDUnitTest extends CacheTestCase {

  public SlowRecDUnitTest(String name) {
    super(name);
  }

  // this test has special config of its distributed system so
  // the setUp and tearDown methods need to make sure we don't
  // use the ds from previous test and that we don't leave ours around
  // for the next test to use.
  
  public void setUp() throws Exception {
    try {
      disconnectAllFromDS();
    } finally {
      super.setUp();
    }
  }
  public void tearDown2() throws Exception {
    try {
      super.tearDown2();
    } finally {
      disconnectAllFromDS();
    }
  }
  
  //////////////////////  Test Methods  //////////////////////

  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }

  static protected Object lastCallback = null;

  private void doCreateOtherVm(final Properties p, final boolean addListener) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
        public void run2() throws CacheException {
          getSystem(p);
          createAckRegion(true, false);
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.DISTRIBUTED_NO_ACK);
          af.setDataPolicy(DataPolicy.REPLICATE);
          if (addListener) {
            CacheListener cl = new CacheListenerAdapter() {
                public void afterUpdate(EntryEvent event) {
                  // make the slow receiver event slower!
                  try {Thread.sleep(500);} catch (InterruptedException shuttingDown) {fail("interrupted");}
                }
              };
            af.setCacheListener(cl);
          } else {
            CacheListener cl = new CacheListenerAdapter() {
                public void afterCreate(EntryEvent event) {
//                   getLogWriter().info("afterCreate " + event.getKey());
                  if (event.getCallbackArgument() != null) {
                    lastCallback = event.getCallbackArgument();
                  }
                  if (event.getKey().equals("sleepkey")) {
                    int sleepMs = ((Integer)event.getNewValue()).intValue();
//                     getLogWriter().info("sleepkey sleeping for " + sleepMs);
                    try {Thread.sleep(sleepMs);} catch (InterruptedException ignore) {fail("interrupted");}
                  }
                }
                public void afterUpdate(EntryEvent event) {
//                   getLogWriter().info("afterUpdate " + event.getKey());
                  if (event.getCallbackArgument() != null) {
                    lastCallback = event.getCallbackArgument();
                  }
                  if (event.getKey().equals("sleepkey")) {
                    int sleepMs = ((Integer)event.getNewValue()).intValue();
//                     getLogWriter().info("sleepkey sleeping for " + sleepMs);
                    try {Thread.sleep(sleepMs);} catch (InterruptedException ignore) {fail("interrupted");}
                  }
                }
                public void afterInvalidate(EntryEvent event) {
                  if (event.getCallbackArgument() != null) {
                    lastCallback = event.getCallbackArgument();
                  }
                }
                public void afterDestroy(EntryEvent event) {
                  if (event.getCallbackArgument() != null) {
                    lastCallback = event.getCallbackArgument();
                  }
                }
              };
            af.setCacheListener(cl);
          }
          Region r1 = createRootRegion("slowrec", af.create());
          // place holder so we receive updates
          r1.create("key", "value");
        }
      });
  }
  static protected final String CHECK_INVALID = "CHECK_INVALID";
  
  private void checkLastValueInOtherVm(final String lastValue, final Object lcb) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("check last value") {
        public void run2() throws CacheException {
          Region r1 = getRootRegion("slowrec");
          if (lcb != null) {
            WaitCriterion ev = new WaitCriterion() {
              public boolean done() {
                return lcb.equals(lastCallback);
              }
              public String description() {
                return "waiting for callback";
              }
            };
            DistributedTestCase.waitForCriterion(ev, 50 * 1000, 200, true);
            assertEquals(lcb, lastCallback);
          }
          if (lastValue == null) {
            final Region r = r1;
            WaitCriterion ev = new WaitCriterion() {
              public boolean done() {
                return r.getEntry("key") == null;
              }
              public String description() {
                return "waiting for key to become null";
              }
            };
            DistributedTestCase.waitForCriterion(ev, 50 * 1000, 200, true);
            assertEquals(null, r1.getEntry("key"));
          } else if (CHECK_INVALID.equals(lastValue)) {
            // should be invalid
            {
              final Region r = r1;
              WaitCriterion ev = new WaitCriterion() {
                public boolean done() {
                  Entry e = r.getEntry("key");
                  if (e == null) {
                    return false;
                  }
                  return e.getValue() == null;
                }
                public String description() {
                  return "waiting for invalidate";
                }
              };
              DistributedTestCase.waitForCriterion(ev, 50 * 1000, 200, true);
//              assertNotNull(re);
//              assertEquals(null, value);
            }
          } else {
            {
              int retryCount = 1000;
              Region.Entry re = null;
              Object value = null;
              while (retryCount-- > 0) {
                re = r1.getEntry("key");
                if (re != null) {
                  value = re.getValue();
                  if (value != null && value.equals(lastValue)) {
                    break;
                  }
                }
                try {Thread.sleep(50);} catch (InterruptedException ignore) {fail("interrupted");}
              }
              assertNotNull(re);
              assertNotNull(value);
              assertEquals(lastValue, value);
            }
          }
        }
      });
  }

  private void forceQueueFlush() {
    Connection.FORCE_ASYNC_QUEUE=false;
    final DMStats stats = getSystem().getDistributionManager().getStats();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return stats.getAsyncThreads() == 0;
      }
      public String description() {
        return "Waiting for async threads to disappear";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
  }
  
  private void forceQueuing(final Region r) throws CacheException {
    Connection.FORCE_ASYNC_QUEUE=true;
    final DMStats stats = getSystem().getDistributionManager().getStats();
    r.put("forcekey", "forcevalue");
    
    // wait for the flusher to get its first flush in progress
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return stats.getAsyncQueueFlushesInProgress() != 0;
      }
      public String description() {
        return "waiting for flushes to start";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 2 * 1000, 200, true);
  }
  
  /**
   * Make sure that noack puts to a receiver
   * will eventually queue and then catch up.
   */
  public void testNoAck() throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    final Region r = createRootRegion("slowrec", factory.create());
    final DMStats stats = getSystem().getDistributionManager().getStats();

    // create receiver in vm0 with queuing enabled
    Properties p = new Properties();
    p.setProperty("async-distribution-timeout", "1");
    doCreateOtherVm(p, false);

    int repeatCount = 2;
    int count = 0;
    while (repeatCount-- > 0) {
      forceQueuing(r);
      final Object key = "key";
      long queuedMsgs = stats.getAsyncQueuedMsgs();
      long dequeuedMsgs = stats.getAsyncDequeuedMsgs();
//      long conflatedMsgs = stats.getAsyncConflatedMsgs();
      long queueSize = stats.getAsyncQueueSize();
      String lastValue = "";
      final long intialQueuedMsgs = queuedMsgs;
      long curQueuedMsgs = queuedMsgs - dequeuedMsgs;
      try {
        // loop while we still have queued the initially queued msgs
        // OR the cur # of queued msgs < 6
        while (dequeuedMsgs < intialQueuedMsgs || curQueuedMsgs <= 6) {
          String value = "count=" + count;
          lastValue = value;
          r.put(key, value);
          count ++;
          queueSize = stats.getAsyncQueueSize();
          queuedMsgs = stats.getAsyncQueuedMsgs();
          dequeuedMsgs = stats.getAsyncDequeuedMsgs();
          curQueuedMsgs = queuedMsgs - dequeuedMsgs;
        }
        getLogWriter().info("After " + count + " " + " puts slowrec mode kicked in by queuing " + queuedMsgs + " for a total size of " + queueSize);
      } finally {
        forceQueueFlush();
      }
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return stats.getAsyncQueueSize() == 0;
        }
        public String description() {
          return "Waiting for queues to empty";
        }
      };
      final long start = System.currentTimeMillis();
      DistributedTestCase.waitForCriterion(ev, 30 * 1000, 200, true);
      final long finish = System.currentTimeMillis();
      getLogWriter().info("After " + (finish - start) + " ms async msgs where flushed. A total of " + stats.getAsyncDequeuedMsgs() + " were flushed. lastValue=" + lastValue);
    
      checkLastValueInOtherVm(lastValue, null);
    }
  }
  /**
   * Create a region named AckRegion with ACK scope
   */
  protected Region createAckRegion(boolean mirror, boolean conflate) throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    if (mirror) {
      factory.setDataPolicy(DataPolicy.REPLICATE);
    }
    if (conflate) {
      factory.setEnableAsyncConflation(true);
    }
    final Region r = createRootRegion("AckRegion", factory.create());
    return r;
  }
  /**
   * Make sure that noack puts to a receiver
   * will eventually queue and then catch up with conflation
   */
  public void testNoAckConflation() throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setEnableAsyncConflation(true);
    final Region r = createRootRegion("slowrec", factory.create());
    final DMStats stats = getSystem().getDistributionManager().getStats();

    // create receiver in vm0 with queuing enabled
    Properties p = new Properties();
    p.setProperty("async-distribution-timeout", "1");
    doCreateOtherVm(p, false);

    forceQueuing(r);
    final Object key = "key";
    int count = 0;
//    long queuedMsgs = stats.getAsyncQueuedMsgs();
//    long dequeuedMsgs = stats.getAsyncDequeuedMsgs();
    final long initialConflatedMsgs = stats.getAsyncConflatedMsgs();
//    long queueSize = stats.getAsyncQueueSize();
    String lastValue = "";
    final long intialDeQueuedMsgs = stats.getAsyncDequeuedMsgs();
    long start = 0;
    try {
      while ((stats.getAsyncConflatedMsgs()-initialConflatedMsgs) < 1000) {
        String value = "count=" + count;
        lastValue = value;
        r.put(key, value);
        count ++;
        //       getLogWriter().info("After " + count + " "
        //                           + " puts queueSize=" + queueSize
        //                           + "    queuedMsgs=" + queuedMsgs
        //                           + "  dequeuedMsgs=" + dequeuedMsgs
        //                           + " conflatedMsgs=" + conflatedMsgs);
      }
      start = System.currentTimeMillis();
    } finally {
      forceQueueFlush();
    }
//     queueSize = stats.getAsyncQueueSize();
//     queuedMsgs = stats.getAsyncQueuedMsgs();

//     getLogWriter().info("After " + count + " "
//                         + " puts slowrec mode kicked in by queuing "
//                         + queuedMsgs + " for a total size of " + queueSize
//                         + " conflatedMsgs=" + conflatedMsgs
//                         + " dequeuedMsgs=" + dequeuedMsgs);
//     final long start = System.currentTimeMillis();
//     while (stats.getAsyncQueuedMsgs() > stats.getAsyncDequeuedMsgs()) {
//       try {Thread.sleep(100);} catch (InterruptedException ignore) {}
//       queueSize = stats.getAsyncQueueSize();
//       queuedMsgs = stats.getAsyncQueuedMsgs();
//       dequeuedMsgs = stats.getAsyncDequeuedMsgs();
//       conflatedMsgs = stats.getAsyncConflatedMsgs();
//       getLogWriter().info("After sleeping"
//                           + "     queueSize=" + queueSize
//                           + "    queuedMsgs=" + queuedMsgs
//                           + "  dequeuedMsgs=" + dequeuedMsgs
//                           + " conflatedMsgs=" + conflatedMsgs);
    final long finish = System.currentTimeMillis();
    getLogWriter().info("After " + (finish - start) + " ms async msgs where flushed. A total of " + (stats.getAsyncDequeuedMsgs()-intialDeQueuedMsgs) + " were flushed. Leaving a queue size of " + stats.getAsyncQueueSize() + ". The lastValue was " + lastValue);
    
    checkLastValueInOtherVm(lastValue, null);
  }
  /**
   * make sure ack does not hang
   * make sure two ack updates do not conflate but are both queued
   */
  public void testAckConflation() throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setEnableAsyncConflation(true);
    final Region r = createRootRegion("slowrec", factory.create());
    final Region ar = createAckRegion(false, true);
    ar.create("ackKey", "ackValue");
    
    final DMStats stats = getSystem().getDistributionManager().getStats();

    // create receiver in vm0 with queuing enabled
    Properties p = new Properties();
    p.setProperty("async-distribution-timeout", "2");
    doCreateOtherVm(p, false);

    forceQueuing(r);
    {
      // make sure ack does not hang
      // make sure two ack updates do not conflate but are both queued
      long startQueuedMsgs = stats.getAsyncQueuedMsgs();
      long startConflatedMsgs = stats.getAsyncConflatedMsgs();
      Thread t = new Thread(new Runnable() {
          public void run() {
            ar.put("ackKey", "ackValue");
          }
        });
      t.start();
      Thread t2 = new Thread(new Runnable() {
          public void run() {
            ar.put("ackKey", "ackValue");
          }
        });
      t2.start();
      // give threads a chance to get queued
      try {Thread.sleep(100);} catch (InterruptedException ignore) {fail("interrupted");}
      forceQueueFlush();
      DistributedTestCase.join(t, 2 * 1000, getLogWriter());
      DistributedTestCase.join(t2, 2 * 1000, getLogWriter());
      long endQueuedMsgs = stats.getAsyncQueuedMsgs();
      long endConflatedMsgs = stats.getAsyncConflatedMsgs();
      assertEquals(startConflatedMsgs, endConflatedMsgs);
      // queue should be flushed by the time we get an ack
      assertEquals(endQueuedMsgs, stats.getAsyncDequeuedMsgs());
      assertEquals(startQueuedMsgs+2, endQueuedMsgs);
    }
  }
  /**
   * Make sure that only sequences of updates are conflated
   * Also checks that sending to a conflating region and non-conflating region
   * does the correct thing.
   * Test disabled because it intermittently fails due to race conditions
   * in test. This has been fixed in congo's tests. See bug 35357.
   */
  public void _disabled_testConflationSequence() throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setEnableAsyncConflation(true);
    final Region r = createRootRegion("slowrec", factory.create());
    factory.setEnableAsyncConflation(false);
    final Region noConflate = createRootRegion("noConflate", factory.create());
    final DMStats stats = getSystem().getDistributionManager().getStats();

    // create receiver in vm0 with queuing enabled
    Properties p = new Properties();
    p.setProperty("async-distribution-timeout", "1");
    doCreateOtherVm(p, false);
    {
      VM vm = getOtherVm();
      vm.invoke(new CacheSerializableRunnable("create noConflate") {
          public void run2() throws CacheException {
            AttributesFactory af = new AttributesFactory();
            af.setScope(Scope.DISTRIBUTED_NO_ACK);
            af.setDataPolicy(DataPolicy.REPLICATE);
            createRootRegion("noConflate", af.create());
          }
        });
    }

    // now make sure update+destroy does not conflate
    final Object key = "key";      
    getLogWriter().info("[testConflationSequence] about to force queuing");
    forceQueuing(r);

    int count = 0;
    String value = "";
    String lastValue = value;
    Object mylcb = null;
    long initialConflatedMsgs = stats.getAsyncConflatedMsgs();
//    long initialDequeuedMsgs = stats.getAsyncDequeuedMsgs();
//    long dequeuedMsgs = stats.getAsyncDequeuedMsgs();
    int endCount = count+60;

    getLogWriter().info("[testConflationSequence] about to build up queue");
    long begin = System.currentTimeMillis();
    while (count < endCount) {
      value = "count=" + count;
      lastValue = value;
      r.create(key, value);
      count ++;
      value = "count=" + count;
      lastValue = value;
      r.put(key, value);
      count ++;
      mylcb = value;
      r.destroy(key, mylcb);
      count ++;
      lastValue = null;
//      dequeuedMsgs = stats.getAsyncDequeuedMsgs();
      assertTrue(System.currentTimeMillis() < begin+1000*60*2);
    }
    assertEquals(initialConflatedMsgs, stats.getAsyncConflatedMsgs());
    forceQueueFlush();
    checkLastValueInOtherVm(lastValue, mylcb);

    // now make sure create+update+localDestroy does not conflate
    getLogWriter().info("[testConflationSequence] force queuing create-update-destroy");
    forceQueuing(r);
    initialConflatedMsgs = stats.getAsyncConflatedMsgs();
//    initialDequeuedMsgs = stats.getAsyncDequeuedMsgs();
//    dequeuedMsgs = stats.getAsyncDequeuedMsgs();
    endCount = count + 40;
    
    getLogWriter().info("[testConflationSequence] create-update-destroy");
    begin = System.currentTimeMillis();
    while (count < endCount) {
      value = "count=" + count;
      lastValue = value;
      r.create(key, value);
      count++;
      value = "count=" + count;
      lastValue = value;
      r.put(key, value);
      count ++;
      r.localDestroy(key);
//      dequeuedMsgs = stats.getAsyncDequeuedMsgs();
      assertTrue(System.currentTimeMillis() < begin+1000*60*2);
    }
    assertEquals(initialConflatedMsgs, stats.getAsyncConflatedMsgs());
    forceQueueFlush();
    checkLastValueInOtherVm(lastValue, null);

    // now make sure update+invalidate does not conflate
    getLogWriter().info("[testConflationSequence] force queuing update-invalidate");
    forceQueuing(r);
    initialConflatedMsgs = stats.getAsyncConflatedMsgs();
//    initialDequeuedMsgs = stats.getAsyncDequeuedMsgs();
    value = "count=" + count;
    lastValue = value;
    r.create(key, value);
    count++;
//    dequeuedMsgs = stats.getAsyncDequeuedMsgs();
    endCount = count + 40;

    getLogWriter().info("[testConflationSequence] update-invalidate");
    begin = System.currentTimeMillis();
    while (count < endCount) {
      value = "count=" + count;
      lastValue = value;
      r.put(key, value);
      count ++;
      r.invalidate(key);
      count ++;
      lastValue = CHECK_INVALID;
//      dequeuedMsgs = stats.getAsyncDequeuedMsgs();
      assertTrue(System.currentTimeMillis() < begin+1000*60*2);
    }
    assertEquals(initialConflatedMsgs, stats.getAsyncConflatedMsgs());
    forceQueueFlush();
    getLogWriter().info("[testConflationSequence] assert other vm");
    checkLastValueInOtherVm(lastValue, null);

    r.destroy(key);

    // now make sure updates to a conflating region are conflated even while
    // updates to a non-conflating are not.
    getLogWriter().info("[testConflationSequence] conflate & no-conflate regions");
    forceQueuing(r);
    final int initialAsyncSocketWrites = stats.getAsyncSocketWrites();
//    initialDequeuedMsgs = stats.getAsyncDequeuedMsgs();
    
    value = "count=" + count;
    lastValue = value;
    long conflatedMsgs = stats.getAsyncConflatedMsgs();
    long queuedMsgs = stats.getAsyncQueuedMsgs();
    r.create(key, value);
    queuedMsgs++;
    assertEquals(queuedMsgs, stats.getAsyncQueuedMsgs());
    assertEquals(conflatedMsgs, stats.getAsyncConflatedMsgs());
    r.put(key, value);
    queuedMsgs++;
    assertEquals(queuedMsgs, stats.getAsyncQueuedMsgs());
    assertEquals(conflatedMsgs, stats.getAsyncConflatedMsgs());
    noConflate.create(key, value);
    queuedMsgs++;
    assertEquals(queuedMsgs, stats.getAsyncQueuedMsgs());
    assertEquals(conflatedMsgs, stats.getAsyncConflatedMsgs());
    noConflate.put(key, value);
    queuedMsgs++;
    assertEquals(queuedMsgs, stats.getAsyncQueuedMsgs());
    assertEquals(conflatedMsgs, stats.getAsyncConflatedMsgs());
    count++;
//    dequeuedMsgs = stats.getAsyncDequeuedMsgs();
    endCount = count + 80;

    begin = System.currentTimeMillis();
    getLogWriter().info("[testConflationSequence:DEBUG] count=" + count
                        + " queuedMsgs=" + stats.getAsyncQueuedMsgs()
                        + " conflatedMsgs=" + stats.getAsyncConflatedMsgs()
                        + " dequeuedMsgs=" + stats.getAsyncDequeuedMsgs()
                        + " asyncSocketWrites=" + stats.getAsyncSocketWrites()
                        );
    while (count < endCount) {
      // make sure we continue to have a flush in progress
      assertEquals(1, stats.getAsyncThreads());
      assertEquals(1, stats.getAsyncQueues());
      assertTrue(stats.getAsyncQueueFlushesInProgress() > 0);
      // make sure we are not completing any flushing while this loop is in progress
      assertEquals(initialAsyncSocketWrites, stats.getAsyncSocketWrites());
      value = "count=" + count;
      lastValue = value;
      r.put(key, value);
      count ++;
      // make sure it was conflated and not queued
      assertEquals(queuedMsgs, stats.getAsyncQueuedMsgs());
      conflatedMsgs++;
      assertEquals(conflatedMsgs, stats.getAsyncConflatedMsgs());
      noConflate.put(key, value);
      // make sure it was queued and not conflated
      queuedMsgs++;
      assertEquals(queuedMsgs, stats.getAsyncQueuedMsgs());
      assertEquals(conflatedMsgs, stats.getAsyncConflatedMsgs());
//      dequeuedMsgs = stats.getAsyncDequeuedMsgs();
      assertTrue(System.currentTimeMillis() < begin+1000*60*2);
    }

    forceQueueFlush();
    getLogWriter().info("[testConflationSequence] assert other vm");
    checkLastValueInOtherVm(lastValue, null);
  }
  /**
   * Make sure that exceeding the queue size limit causes a disconnect.
   */
  public void testSizeDisconnect() throws CacheException {
    final String expected = 
      "com.gemstone.gemfire.internal.tcp.ConnectionException: Forced disconnect sent to" +
      "||java.io.IOException: Broken pipe";
    final String addExpected = 
      "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected = 
      "<ExpectedException action=remove>" + expected + "</ExpectedException>";

    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    final Region r = createRootRegion("slowrec", factory.create());
    final DM dm = getSystem().getDistributionManager();
    final DMStats stats = dm.getStats();
    // set others before vm0 connects
    final Set others = dm.getOtherDistributionManagerIds();

    // create receiver in vm0 with queuing enabled
    Properties p = new Properties();
    p.setProperty("async-distribution-timeout", "5");
    p.setProperty("async-max-queue-size", "1"); // 1 meg
    doCreateOtherVm(p, false);

    
    final Object key = "key";
    final int VALUE_SIZE = 1024 * 100; // .1M async-max-queue-size should give us 10 of these 100K msgs before queue full
    final byte[] value = new byte[VALUE_SIZE];
    int count = 0;
    forceQueuing(r);
    long queuedMsgs = stats.getAsyncQueuedMsgs();
    long queueSize = stats.getAsyncQueueSize();
    
    getCache().getLogger().info(addExpected);
    try {    
      while (stats.getAsyncQueueSizeExceeded() == 0 && stats.getAsyncQueueTimeouts() == 0) {
        r.put(key, value);
        count ++;
        if (stats.getAsyncQueueSize() > 0) {
          queuedMsgs = stats.getAsyncQueuedMsgs();
          queueSize = stats.getAsyncQueueSize();
        }
        if (count > 100) {
          fail("should have exceeded max-queue-size by now");
        }
      }
      getLogWriter().info("After " + count + " " + VALUE_SIZE + " byte puts slowrec mode kicked in but the queue filled when its size reached " + queueSize + " with " + queuedMsgs + " msgs");
      // make sure we lost a connection to vm0
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          return dm.getOtherDistributionManagerIds().size() <= others.size()
              && stats.getAsyncQueueSize() == 0;
        }
        public String description() {
          return "waiting for connection loss";
        }
      };
      DistributedTestCase.waitForCriterion(ev, 30 * 1000, 200, true);
    }
    finally {
      forceQueueFlush();
      getCache().getLogger().info(removeExpected);
    }
    assertEquals(others, dm.getOtherDistributionManagerIds());
    assertEquals(0, stats.getAsyncQueueSize());
  }
  /**
   * Make sure that exceeding the async-queue-timeout causes a disconnect.<p>
   * [bruce] This test was disabled when the SlowRecDUnitTest was re-enabled
   * in build.xml in the splitbrainNov07 branch.  It had been disabled since
   * June 2006 due to hangs.  Some of the tests, like this one, still need
   * work because the periodically (some quite often) fail.
   */
  public void donottestTimeoutDisconnect() throws CacheException {
    final String expected = 
      "com.gemstone.gemfire.internal.tcp.ConnectionException: Forced disconnect sent to" +
      "||java.io.IOException: Broken pipe";
    final String addExpected = 
      "<ExpectedException action=add>" + expected + "</ExpectedException>";
    final String removeExpected = 
      "<ExpectedException action=remove>" + expected + "</ExpectedException>";
      
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    final Region r = createRootRegion("slowrec", factory.create());
    final DM dm = getSystem().getDistributionManager();
    final DMStats stats = dm.getStats();
    // set others before vm0 connects
    final Set others = dm.getOtherDistributionManagerIds();

    // create receiver in vm0 with queuing enabled
    Properties p = new Properties();
    p.setProperty("async-distribution-timeout", "5");
    p.setProperty("async-queue-timeout", "500"); // 500 ms
    doCreateOtherVm(p, true);

    
    final Object key = "key";
    final int VALUE_SIZE = 1024; // 1k
    final byte[] value = new byte[VALUE_SIZE];
    int count = 0;
    long queuedMsgs = stats.getAsyncQueuedMsgs();
    long queueSize = stats.getAsyncQueueSize();
    final long timeoutLimit = System.currentTimeMillis() + 5000;

    getCache().getLogger().info(addExpected);
    try {    
      while (stats.getAsyncQueueTimeouts() == 0) {
        r.put(key, value);
        count ++;
        if (stats.getAsyncQueueSize() > 0) {
          queuedMsgs = stats.getAsyncQueuedMsgs();
          queueSize = stats.getAsyncQueueSize();
        }
        if (System.currentTimeMillis() > timeoutLimit) {
          fail("should have exceeded async-queue-timeout by now");
        }
      }
      getLogWriter().info("After " + count + " " + VALUE_SIZE + " byte puts slowrec mode kicked in but the queue filled when its size reached " + queueSize + " with " + queuedMsgs + " msgs");
      // make sure we lost a connection to vm0
      WaitCriterion ev = new WaitCriterion() {
        public boolean done() {
          if (dm.getOtherDistributionManagerIds().size() > others.size()) {
            return false;
          }
          return stats.getAsyncQueueSize() == 0;
        }
        public String description() {
          return "waiting for departure";
        }
      };
      DistributedTestCase.waitForCriterion(ev, 2 * 1000, 200, true);
    }
    finally {
      getCache().getLogger().info(removeExpected);
    }
    assertEquals(others, dm.getOtherDistributionManagerIds());
    assertEquals(0, stats.getAsyncQueueSize());
  }

  // static helper methods ---------------------------------------------------
  
  private static final String KEY_SLEEP = "KEY_SLEEP";
  private static final String KEY_WAIT = "KEY_WAIT";
  private static final String KEY_DISCONNECT = "KEY_DISCONNECT";
  
  protected final static int CALLBACK_CREATE = 0;
  protected final static int CALLBACK_UPDATE = 1;
  protected final static int CALLBACK_INVALIDATE = 2;
  protected final static int CALLBACK_DESTROY = 3;
  protected final static int CALLBACK_REGION_INVALIDATE = 4;
  
  protected final static Integer CALLBACK_CREATE_INTEGER = new Integer(CALLBACK_CREATE);
  protected final static Integer CALLBACK_UPDATE_INTEGER = new Integer(CALLBACK_UPDATE);
  protected final static Integer CALLBACK_INVALIDATE_INTEGER = new Integer(CALLBACK_INVALIDATE);
  protected final static Integer CALLBACK_DESTROY_INTEGER = new Integer(CALLBACK_DESTROY);
  protected final static Integer CALLBACK_REGION_INVALIDATE_INTEGER = new Integer(CALLBACK_REGION_INVALIDATE);

  private static class CallbackWrapper {
    public final Object callbackArgument;
    public final  int callbackType;
    public CallbackWrapper(Object callbackArgument, int callbackType) {
      this.callbackArgument = callbackArgument;
      this.callbackType = callbackType;
    }
    public String toString() {
      return "CallbackWrapper: " + callbackArgument.toString() + " of type " + callbackType;
    }
  }
  
  protected static class ControlListener extends CacheListenerAdapter {
    public final LinkedList callbackArguments = new LinkedList();
    public final LinkedList callbackTypes = new LinkedList();
    public final Object CONTROL_LOCK = new Object();
    
    public void afterCreate(EntryEvent event) {
      getLogWriter().info(event.getRegion().getName() + " afterCreate " + event.getKey());
      synchronized(this.CONTROL_LOCK) {
        if (event.getCallbackArgument() != null) {
          this.callbackArguments.add(
            new CallbackWrapper(event.getCallbackArgument(), CALLBACK_CREATE));
          this.callbackTypes.add(CALLBACK_CREATE_INTEGER);
          this.CONTROL_LOCK.notifyAll();
        }
      }
      processEvent(event);
    }
    public void afterUpdate(EntryEvent event) {
      getLogWriter().info(event.getRegion().getName() + " afterUpdate " + event.getKey());
      synchronized(this.CONTROL_LOCK) {
        if (event.getCallbackArgument() != null) {
          this.callbackArguments.add(
            new CallbackWrapper(event.getCallbackArgument(), CALLBACK_UPDATE));
          this.callbackTypes.add(CALLBACK_UPDATE_INTEGER);
          this.CONTROL_LOCK.notifyAll();
        }
      }
      processEvent(event);
    }
    public void afterInvalidate(EntryEvent event) {
      synchronized(this.CONTROL_LOCK) {
        if (event.getCallbackArgument() != null) {
          this.callbackArguments.add(
            new CallbackWrapper(event.getCallbackArgument(), CALLBACK_INVALIDATE));
          this.callbackTypes.add(CALLBACK_INVALIDATE_INTEGER);
          this.CONTROL_LOCK.notifyAll();
        }
      }
    }
    public void afterDestroy(EntryEvent event) {
      synchronized(this.CONTROL_LOCK) {
        if (event.getCallbackArgument() != null) {
          this.callbackArguments.add(
            new CallbackWrapper(event.getCallbackArgument(), CALLBACK_DESTROY));
          this.callbackTypes.add(CALLBACK_DESTROY_INTEGER);
          this.CONTROL_LOCK.notifyAll();
        }
      }
    }
    public void afterRegionInvalidate(RegionEvent event) {
      synchronized(this.CONTROL_LOCK) {
        if (event.getCallbackArgument() != null) {
          this.callbackArguments.add(
            new CallbackWrapper(event.getCallbackArgument(), CALLBACK_REGION_INVALIDATE));
          this.callbackTypes.add(CALLBACK_REGION_INVALIDATE_INTEGER);
          this.CONTROL_LOCK.notifyAll();
        }
      }
    }
    private void processEvent(EntryEvent event) {
      if (event.getKey().equals(KEY_SLEEP)) {
        processSleep(event);
      }
      else if (event.getKey().equals(KEY_WAIT)) {
        processWait(event);
      }
      else if (event.getKey().equals(KEY_DISCONNECT)) {
        processDisconnect(event);
      }
    }
    private void processSleep(EntryEvent event) {
      int sleepMs = ((Integer)event.getNewValue()).intValue();
      getLogWriter().info("[processSleep] sleeping for " + sleepMs);
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException ignore) {fail("interrupted");}
    }
    private void processWait(EntryEvent event) {
      int sleepMs = ((Integer)event.getNewValue()).intValue();
      getLogWriter().info("[processWait] waiting for " + sleepMs);
      synchronized(this.CONTROL_LOCK) {
        try {
          this.CONTROL_LOCK.wait(sleepMs);
        } catch (InterruptedException ignore) {return;}
      }
    }
    private void processDisconnect(EntryEvent event) {
      getLogWriter().info("[processDisconnect] disconnecting");
      disconnectFromDS();
    }
  };

  /**
   * Make sure a multiple no ack regions conflate properly.
   * [bruce] disabled when use of this dunit test class was reenabled in
   * the splitbrainNov07 branch.  The class had been disabled since
   * June 2006 r13222 in the trunk.  This test is failing because conflation
   * isn't kicking in for some reason.
   */
  public void donottestMultipleRegionConflation() throws Throwable {
    try {
      doTestMultipleRegionConflation();
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      getLogWriter().error("Encountered exception: ", t);
      throw t;
    }
    finally {
      // make sure other vm was notified even if test failed
      getOtherVm().invoke(new SerializableRunnable("Wake up other vm") {
        public void run() {
          synchronized(doTestMultipleRegionConflation_R1_Listener.CONTROL_LOCK) {
            doTestMultipleRegionConflation_R1_Listener.CONTROL_LOCK.notifyAll();
          }
        }
      });
    }
  }
  protected static ControlListener doTestMultipleRegionConflation_R1_Listener;
  protected static ControlListener doTestMultipleRegionConflation_R2_Listener;
  private void doTestMultipleRegionConflation() throws Exception {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setEnableAsyncConflation(true);
    final Region r1 = createRootRegion("slowrec1", factory.create());
    final Region r2 = createRootRegion("slowrec2", factory.create());
    
    assertTrue(getSystem().isConnected());
    assertNotNull(r1);
    assertFalse(r1.isDestroyed());
    assertNotNull(getCache());
    assertNotNull(getCache().getRegion("slowrec1"));
    assertNotNull(r2);
    assertFalse(r2.isDestroyed());
    assertNotNull(getCache());
    assertNotNull(getCache().getRegion("slowrec2"));
    
    final DM dm = getSystem().getDistributionManager();
    final Serializable controllerVM = dm.getDistributionManagerId();
    final DMStats stats = dm.getStats();
    final int millisToWait = 1000 * 60 * 5; // 5 minutes
    
    // set others before vm0 connects
    long initialQueuedMsgs = stats.getAsyncQueuedMsgs();

    // create receiver in vm0 with queuing enabled
    final Properties p = new Properties();
    p.setProperty("async-distribution-timeout", "5");
    p.setProperty("async-queue-timeout", "86400000"); // max value
    p.setProperty("async-max-queue-size", "1024"); // max value

    getOtherVm().invoke(new CacheSerializableRunnable("Create other vm") {
      public void run2() throws CacheException {
        getSystem(p);
        
        DM dm = getSystem().getDistributionManager();
        assertTrue(dm.getDistributionManagerIds().contains(controllerVM));
        
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_NO_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        
        doTestMultipleRegionConflation_R1_Listener = new ControlListener();
        af.setCacheListener(doTestMultipleRegionConflation_R1_Listener);
        createRootRegion("slowrec1", af.create());
        
        doTestMultipleRegionConflation_R2_Listener = new ControlListener();
        af.setCacheListener(doTestMultipleRegionConflation_R2_Listener);
        createRootRegion("slowrec2", af.create());
      }
    });
    
    // put vm0 cache listener into wait
    getLogWriter().info("[doTestMultipleRegionConflation] about to put vm0 into wait");
    r1.put(KEY_WAIT, new Integer(millisToWait));

    // build up queue size
    getLogWriter().info("[doTestMultipleRegionConflation] building up queue size...");
    final Object key = "key";
    final int socketBufferSize = getSystem().getConfig().getSocketBufferSize();
    final int VALUE_SIZE = socketBufferSize*3;
    //final int VALUE_SIZE = 1024 * 1024  ; // 1 MB
    final byte[] value = new byte[VALUE_SIZE];

    int count = 0;
    while (stats.getAsyncQueuedMsgs() == initialQueuedMsgs) {
      count++;
      r1.put(key, value);
    }
    
    getLogWriter().info("[doTestMultipleRegionConflation] After " + 
      count + " puts of size " + VALUE_SIZE + 
      " slowrec mode kicked in with queue size=" + stats.getAsyncQueueSize());

    // put values that will be asserted
    final Object key1 = "key1";
    final Object key2 = "key2";
    Object putKey = key1;
    boolean flag = true;
    for (int i = 0; i < 30; i++) {
      if (i == 10) putKey = key2;
      if (flag) {
        if (i == 6) {
          r1.invalidate(putKey, new Integer(i));
        } else if (i == 24) {
          r1.invalidateRegion(new Integer(i));
        } else {
          r1.put(putKey, value, new Integer(i));
        }
      } else {
        if (i == 15) {
          r2.destroy(putKey, new Integer(i));
        } else {
          r2.put(putKey, value, new Integer(i));
        }
      }
      flag = !flag;
    }
    
    // r1: key1, 0, create
    // r1: key1, 4, update
    // r1: key1, 6, invalidate
    // r1: key1, 8, update
    
    // r1: key2, 10, create
    // r1:       24, invalidateRegion
    // r1: key2, 28, update

    // r2: key1, 1, create
    // r2: key1, 9, update
    
    // r2: key2, 11, create
    // r2: key2, 13, update
    // r2: key2, 15, destroy
    // r2: key2, 17, create
    // r2: key2, 29, update
    
    final int[] r1ExpectedArgs = new int[] { 0, 4, 6, 8, 10, 24, 28 }; 
    final int[] r1ExpectedTypes = new int[] /* 0, 1, 2, 1, 0, 4, 1 */
      { CALLBACK_CREATE, CALLBACK_UPDATE, CALLBACK_INVALIDATE, CALLBACK_UPDATE,
        CALLBACK_CREATE, CALLBACK_REGION_INVALIDATE, CALLBACK_UPDATE }; 
    
    final int[] r2ExpectedArgs = new int[] { 1, 9, 11, 13, 15, 17, 29 };
    final int[] r2ExpectedTypes = new int[] 
      { CALLBACK_CREATE, CALLBACK_UPDATE, CALLBACK_CREATE, CALLBACK_UPDATE,
        CALLBACK_DESTROY, CALLBACK_CREATE, CALLBACK_UPDATE }; 

    // send notify to vm0
    getLogWriter().info("[doTestMultipleRegionConflation] wake up vm0");
    getOtherVm().invoke(new SerializableRunnable("Wake up other vm") {
      public void run() {
        synchronized(doTestMultipleRegionConflation_R1_Listener.CONTROL_LOCK) {
          doTestMultipleRegionConflation_R1_Listener.CONTROL_LOCK.notifyAll();
        }
      }
    });
    
    // wait for queue to be flushed
    getLogWriter().info("[doTestMultipleRegionConflation] wait for vm0");
    getOtherVm().invoke(new SerializableRunnable("Wait for other vm") {
      public void run() {
        try {
          synchronized(doTestMultipleRegionConflation_R1_Listener.CONTROL_LOCK) {
            while (doTestMultipleRegionConflation_R1_Listener.callbackArguments.size() < r1ExpectedArgs.length) {
              doTestMultipleRegionConflation_R1_Listener.CONTROL_LOCK.wait(millisToWait);
            }
          }
          synchronized(doTestMultipleRegionConflation_R2_Listener.CONTROL_LOCK) {
            while (doTestMultipleRegionConflation_R2_Listener.callbackArguments.size() < r2ExpectedArgs.length) {
              doTestMultipleRegionConflation_R2_Listener.CONTROL_LOCK.wait(millisToWait);
            }
          }
        } catch (InterruptedException ignore) {fail("interrupted");}
      }
    });
    
    // assert values on both listeners
    getLogWriter().info("[doTestMultipleRegionConflation] assert callback arguments");
    getOtherVm().invoke(new SerializableRunnable("Assert callback arguments") {
      public void run() {
        synchronized(doTestMultipleRegionConflation_R1_Listener.CONTROL_LOCK) {
          getLogWriter().info("doTestMultipleRegionConflation_R1_Listener.callbackArguments=" + doTestMultipleRegionConflation_R1_Listener.callbackArguments);
          getLogWriter().info("doTestMultipleRegionConflation_R1_Listener.callbackTypes=" + doTestMultipleRegionConflation_R1_Listener.callbackTypes);
          assertEquals(doTestMultipleRegionConflation_R1_Listener.callbackArguments.size(),
                       doTestMultipleRegionConflation_R1_Listener.callbackTypes.size());
          int i = 0;
          for (Iterator iter = doTestMultipleRegionConflation_R1_Listener.callbackArguments.iterator(); iter.hasNext();) {
            CallbackWrapper wrapper = (CallbackWrapper) iter.next();
            assertEquals(new Integer(r1ExpectedArgs[i]), 
              wrapper.callbackArgument);
            assertEquals(new Integer(r1ExpectedTypes[i]), 
              doTestMultipleRegionConflation_R1_Listener.callbackTypes.get(i));
            i++;
          }
        }
        synchronized(doTestMultipleRegionConflation_R2_Listener.CONTROL_LOCK) {
          getLogWriter().info("doTestMultipleRegionConflation_R2_Listener.callbackArguments=" + doTestMultipleRegionConflation_R2_Listener.callbackArguments);
          getLogWriter().info("doTestMultipleRegionConflation_R2_Listener.callbackTypes=" + doTestMultipleRegionConflation_R2_Listener.callbackTypes);
          assertEquals(doTestMultipleRegionConflation_R2_Listener.callbackArguments.size(),
                       doTestMultipleRegionConflation_R2_Listener.callbackTypes.size());
          int i = 0;
          for (Iterator iter = doTestMultipleRegionConflation_R2_Listener.callbackArguments.iterator(); iter.hasNext();) {
            CallbackWrapper wrapper = (CallbackWrapper) iter.next();
            assertEquals(new Integer(r2ExpectedArgs[i]), 
              wrapper.callbackArgument);
            assertEquals(new Integer(r2ExpectedTypes[i]), 
              doTestMultipleRegionConflation_R2_Listener.callbackTypes.get(i));
            i++;
          }
        }
      }
    });
  }

  /**
   * Make sure a disconnect causes queue memory to be released.
   */
  public void testDisconnectCleanup() throws Throwable {
    try {
      doTestDisconnectCleanup();
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      getLogWriter().error("Encountered exception: ", t);
      throw t;
    }
    finally {
      // make sure other vm was notified even if test failed
      getOtherVm().invoke(new SerializableRunnable("Wake up other vm") {
        public void run() {
          synchronized(doTestDisconnectCleanup_Listener.CONTROL_LOCK) {
            doTestDisconnectCleanup_Listener.CONTROL_LOCK.notifyAll();
          }
        }
      });
    }
  }
  protected static ControlListener doTestDisconnectCleanup_Listener;
  private void doTestDisconnectCleanup() throws Exception {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    final Region r = createRootRegion("slowrec", factory.create());
    final DM dm = getSystem().getDistributionManager();
    final DMStats stats = dm.getStats();
    // set others before vm0 connects
    final Set others = dm.getOtherDistributionManagerIds();
    long initialQueuedMsgs = stats.getAsyncQueuedMsgs();
    final int initialQueues = stats.getAsyncQueues();

    // create receiver in vm0 with queuing enabled
    final Properties p = new Properties();
    p.setProperty("async-distribution-timeout", "5");
    p.setProperty("async-queue-timeout", "86400000"); // max value
    p.setProperty("async-max-queue-size", "1024"); // max value
    
    getOtherVm().invoke(new CacheSerializableRunnable("Create other vm") {
      public void run2() throws CacheException {
        getSystem(p);
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_NO_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        
        doTestDisconnectCleanup_Listener = new ControlListener();
        af.setCacheListener(doTestDisconnectCleanup_Listener);
        createRootRegion("slowrec", af.create());
      }
    });

    // put vm0 cache listener into wait
    getLogWriter().info("[testDisconnectCleanup] about to put vm0 into wait");
    int millisToWait = 1000 * 60 * 5; // 5 minutes
    r.put(KEY_WAIT, new Integer(millisToWait));
    r.put(KEY_DISCONNECT, KEY_DISCONNECT);

    // build up queue size
    getLogWriter().info("[testDisconnectCleanup] building up queue size...");
    final Object key = "key";
    final int socketBufferSize = getSystem().getConfig().getSocketBufferSize();
    final int VALUE_SIZE = socketBufferSize*3;
    //final int VALUE_SIZE = 1024 * 1024  ; // 1 MB
    final byte[] value = new byte[VALUE_SIZE];

    int count = 0;
    final long abortMillis = System.currentTimeMillis() + millisToWait;
    while (stats.getAsyncQueuedMsgs() == initialQueuedMsgs) {
      count++;
      r.put(key, value);
      assertFalse(System.currentTimeMillis() >= abortMillis);
    }
    
    getLogWriter().info("[testDisconnectCleanup] After " + 
      count + " puts of size " + VALUE_SIZE + 
      " slowrec mode kicked in with queue size=" + stats.getAsyncQueueSize());

    while (stats.getAsyncQueuedMsgs() < 10 ||
           stats.getAsyncQueueSize() < VALUE_SIZE*10) {
      count++;
      r.put(key, value);
      assertFalse(System.currentTimeMillis() >= abortMillis);
    }
    assertTrue(stats.getAsyncQueuedMsgs() >= 10);

    while (stats.getAsyncQueues() < 1) {
      pause(100);
      assertFalse(System.currentTimeMillis() >= abortMillis);
    }
    
    getLogWriter().info("[testDisconnectCleanup] After " + 
      count + " puts of size " + VALUE_SIZE + " queue size has reached " + 
      stats.getAsyncQueueSize() + " bytes and number of queues is " + 
      stats.getAsyncQueues() + ".");

    assertTrue(stats.getAsyncQueueSize() >= (VALUE_SIZE*5));
    assertEquals(initialQueues+1, stats.getAsyncQueues());

    // assert vm0 is still connected
    assertTrue(dm.getOtherDistributionManagerIds().size() > others.size());
    
    // send notify to vm0
    getLogWriter().info("[testDisconnectCleanup] wake up vm0");
    getOtherVm().invoke(new SerializableRunnable("Wake up other vm") {
      public void run() {
        synchronized(doTestDisconnectCleanup_Listener.CONTROL_LOCK) {
          doTestDisconnectCleanup_Listener.CONTROL_LOCK.notifyAll();
        }
      }
    });
    
    // make sure we lost a connection to vm0
    getLogWriter().info("[testDisconnectCleanup] wait for vm0 to disconnect");
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return dm.getOtherDistributionManagerIds().size() <= others.size();
      }
      public String description() {
        return "waiting for disconnect";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 2 * 1000, 200, true);
    assertEquals(others, dm.getOtherDistributionManagerIds());
    
    // check free memory... perform wait loop with System.gc
    getLogWriter().info("[testDisconnectCleanup] wait for queue cleanup");
    ev = new WaitCriterion() {
      public boolean done() {
        if (stats.getAsyncQueues() <= initialQueues) {
          return true;
        }
        Runtime.getRuntime().gc();
        return false;
      }
      public String description() {
        return "waiting for queue cleanup";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 2 * 1000, 200, true);
//    getLogWriter().info("[testDisconnectCleanup] initialQueues=" + 
//      initialQueues + " asyncQueues=" + stats.getAsyncQueues());
    assertEquals(initialQueues, stats.getAsyncQueues());
  }

  /**
   * Make sure a disconnect causes queue memory to be released.<p>
     * [bruce] This test was disabled when the SlowRecDUnitTest was re-enabled
   * in build.xml in the splitbrainNov07 branch.  It had been disabled since
   * June 2006 due to hangs.  Some of the tests, like this one, still need
   * work because the periodically (some quite often) fail.
 */
  public void donottestPartialMessage() throws Throwable {
    try {
      doTestPartialMessage();
    }
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch (Throwable t) {
      getLogWriter().error("Encountered exception: ", t);
      throw t;
    }
    finally {
      // make sure other vm was notified even if test failed
      getOtherVm().invoke(new SerializableRunnable("Wake up other vm") {
        public void run() {
          synchronized(doTestPartialMessage_Listener.CONTROL_LOCK) {
            doTestPartialMessage_Listener.CONTROL_LOCK.notifyAll();
          }
        }
      });
    }
  }
  protected static ControlListener doTestPartialMessage_Listener;
  private void doTestPartialMessage() throws Exception {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setEnableAsyncConflation(true);
    final Region r = createRootRegion("slowrec", factory.create());
    final DM dm = getSystem().getDistributionManager();
    final DMStats stats = dm.getStats();
    
    // set others before vm0 connects
//    final Set others = dm.getOtherDistributionManagerIds();
    long initialQueuedMsgs = stats.getAsyncQueuedMsgs();
//    int initialQueues = stats.getAsyncQueues();

    // create receiver in vm0 with queuing enabled
    final Properties p = new Properties();
    p.setProperty("async-distribution-timeout", String.valueOf(1000*4)); // 4 sec
    p.setProperty("async-queue-timeout", "86400000"); // max value
    p.setProperty("async-max-queue-size", "1024"); // max value
    
    getOtherVm().invoke(new CacheSerializableRunnable("Create other vm") {
      public void run2() throws CacheException {
        getSystem(p);
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_NO_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        
        doTestPartialMessage_Listener = new ControlListener();
        af.setCacheListener(doTestPartialMessage_Listener);
        createRootRegion("slowrec", af.create());
      }
    });

    // put vm0 cache listener into wait
    getLogWriter().info("[testPartialMessage] about to put vm0 into wait");
    final int millisToWait = 1000 * 60 * 5; // 5 minutes
    r.put(KEY_WAIT, new Integer(millisToWait));

    // build up queue size
    getLogWriter().info("[testPartialMessage] building up queue size...");
    final Object key = "key";
    final int socketBufferSize = getSystem().getConfig().getSocketBufferSize();
    final int VALUE_SIZE = socketBufferSize*3;
    //1024 * 20; // 20 KB
    final byte[] value = new byte[VALUE_SIZE];

    int count = 0;
    while (stats.getAsyncQueuedMsgs() == initialQueuedMsgs) {
      count++;
      r.put(key, value, new Integer(count));
    }
    
    final int partialId = count;
    assertEquals(0, stats.getAsyncConflatedMsgs());
    
    getLogWriter().info("[testPartialMessage] After " + 
      count + " puts of size " + VALUE_SIZE + 
      " slowrec mode kicked in with queue size=" + stats.getAsyncQueueSize());

    pause(2000);
      
    // conflate 10 times
    while (stats.getAsyncConflatedMsgs() < 10) {
      count++;
      r.put(key, value, new Integer(count));
      if (count == partialId+1) {
//        long begin = System.currentTimeMillis();
//        while (stats.getAsyncQueues() < 1) {
//          pause(100);
//          assertFalse(System.currentTimeMillis() > begin+1000*10);
//        }
        assertEquals(initialQueuedMsgs+2, stats.getAsyncQueuedMsgs());
        assertEquals(0, stats.getAsyncConflatedMsgs());
      } else if (count == partialId+2) {
        assertEquals(initialQueuedMsgs+2, stats.getAsyncQueuedMsgs());
        assertEquals(1, stats.getAsyncConflatedMsgs());
      }
    }
    
    final int conflateId = count;
    
    final int[] expectedArgs = { partialId, conflateId };

    // send notify to vm0
    getLogWriter().info("[testPartialMessage] wake up vm0");
    getOtherVm().invoke(new SerializableRunnable("Wake up other vm") {
      public void run() {
        synchronized(doTestPartialMessage_Listener.CONTROL_LOCK) {
          doTestPartialMessage_Listener.CONTROL_LOCK.notify();
        }
      }
    });
    
    // wait for queue to be flushed
    getLogWriter().info("[testPartialMessage] wait for vm0");
    getOtherVm().invoke(new SerializableRunnable("Wait for other vm") {
      public void run() {
        try {
          synchronized(doTestPartialMessage_Listener.CONTROL_LOCK) {
            boolean done = false;
            while (!done) {
              if (doTestPartialMessage_Listener.callbackArguments.size()> 0) {
                CallbackWrapper last = (CallbackWrapper)
                  doTestPartialMessage_Listener.callbackArguments.getLast();
                Integer lastId = (Integer) last.callbackArgument;
                if (lastId.intValue() == conflateId) {
                  done = true;
                } else {
                  doTestPartialMessage_Listener.CONTROL_LOCK.wait(millisToWait);
                }
              } else {
                doTestPartialMessage_Listener.CONTROL_LOCK.wait(millisToWait);
              }
            }
          }
        } catch (InterruptedException ignore) {fail("interrupted");}
      }
    });
    
    // assert values on both listeners
    getLogWriter().info("[testPartialMessage] assert callback arguments");
    getOtherVm().invoke(new SerializableRunnable("Assert callback arguments") {
      public void run() {
        synchronized(doTestPartialMessage_Listener.CONTROL_LOCK) {
          getLogWriter().info("[testPartialMessage] " +
              "doTestPartialMessage_Listener.callbackArguments=" + 
              doTestPartialMessage_Listener.callbackArguments);
              
          assertEquals(doTestPartialMessage_Listener.callbackArguments.size(),
                       doTestPartialMessage_Listener.callbackTypes.size());
                       
          int i = 0;
          Iterator argIter = 
            doTestPartialMessage_Listener.callbackArguments.iterator();
          Iterator typeIter = 
            doTestPartialMessage_Listener.callbackTypes.iterator();
            
          while (argIter.hasNext()) {
            CallbackWrapper wrapper = (CallbackWrapper) argIter.next();
            Integer arg = (Integer) wrapper.callbackArgument;
            typeIter.next(); // Integer type
            if (arg.intValue() < partialId) {
              continue;
            }
            assertEquals(new Integer(expectedArgs[i]), arg);
            //assertEquals(CALLBACK_UPDATE_INTEGER, type);
            i++;
          }
        }
      }
    });
    
  }
}

