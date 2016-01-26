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
package com.gemstone.gemfire.distributed.internal.locks;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;

/**
 * Tests the Collaboration Lock used internally by dlock service.
 *
 * @author Kirk Lund
 * @since 4.1.1
 */
@Category(IntegrationTest.class)
@Ignore("Test is broken and was named CollaborationJUnitDisabledTest")
public class CollaborationJUnitTest {

  protected LogWriter log = new LocalLogWriter(InternalLogWriter.INFO_LEVEL);
  protected Collaboration collaboration;
  
  @Before
  public void setUp() throws Exception {
    this.collaboration = new Collaboration(new CancelCriterion() {
      @Override
      public String cancelInProgress() {
        return null;
      }
      @Override
      public RuntimeException generateCancelledException(Throwable e) {
        return null;
      }
    });
  }
  
  @After
  public void tearDown() throws Exception {
    this.collaboration = null;
  }
  
  protected volatile boolean flagTestBlocksUntilRelease = false;
  protected volatile boolean threadBStartedTestBlocksUntilRelease = false;
  
  @Test
  public void testBlocksUntilRelease() throws Exception {
    this.log.info("[testBlocksUntilRelease]");
    Thread threadA = new Thread(group, new Runnable() {
      @Override
      public void run() {
        collaboration.acquireUninterruptibly("topicA");
        try {
          flagTestBlocksUntilRelease = true;
          while(flagTestBlocksUntilRelease) {
            try {
              Thread.sleep(10);
            }
            catch (InterruptedException ignore) {fail("interrupted");}
          }
        }
        finally {
          collaboration.release();
        }
      }
    });
    
    // thread one acquires
    threadA.start();
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return CollaborationJUnitTest.this.flagTestBlocksUntilRelease;
      }
      @Override
      public String description() {
        return "waiting for thread";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 5 * 1000, 200, true);
    assertTrue(this.collaboration.hasCurrentTopic(threadA));
    
    // thread two blocks until one releeases
    Thread threadB = new Thread(group, new Runnable() {
      @Override
      public void run() {
        threadBStartedTestBlocksUntilRelease = true;
        collaboration.acquireUninterruptibly("topicB");
        try {
          flagTestBlocksUntilRelease = true;
          WaitCriterion ev2 = new WaitCriterion() {
            @Override
            public boolean done() {
              return !flagTestBlocksUntilRelease;
            }
            @Override
            public String description() {
              return "waiting for release";
            }
          };
          DistributedTestCase.waitForCriterion(ev2, 20 * 1000, 200, true);
        }
        finally {
          collaboration.release();
        }
      }
    });
    
    // start up threadB
    threadB.start();
    ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return threadBStartedTestBlocksUntilRelease;
      }
      @Override
      public String description() {
        return "waiting for thread b";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 5 * 1000, 200, true);
    
    // threadA holds topic and threadB is waiting...
    assertTrue(this.collaboration.hasCurrentTopic(threadA));
    assertFalse(this.collaboration.hasCurrentTopic(threadB));

    // let threadA release so that threadB gets lock
    this.flagTestBlocksUntilRelease = false;
    DistributedTestCase.join(threadA, 30 * 1000, null);
    
    // make sure threadB is doing what it's supposed to do...
    ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return flagTestBlocksUntilRelease;
      }
      @Override
      public String description() {
        return "threadB";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 5 * 1000, 200, true);
    // threadB must have lock now... let threadB release
    assertTrue(this.collaboration.hasCurrentTopic(threadB));
    this.flagTestBlocksUntilRelease = false;
    DistributedTestCase.join(threadB, 30 * 1000, null);

    // collaboration should be free now    
    assertFalse(this.collaboration.hasCurrentTopic(threadA));
    assertFalse(this.collaboration.hasCurrentTopic(threadB));
    assertFalse(this.collaboration.hasCurrentTopic());
  }
  
  protected volatile boolean threadAFlag_TestLateComerJoinsIn = false;
  protected volatile boolean threadBFlag_TestLateComerJoinsIn = false;
  protected volatile boolean threadCFlag_TestLateComerJoinsIn = true;
  protected volatile boolean threadDFlag_TestLateComerJoinsIn = false;
  
  @Test
  public void testLateComerJoinsIn() throws Exception {
    this.log.info("[testLateComerJoinsIn]");
    
    final Object topicA = "topicA";
    final Object topicB = "topicB";
    
    // threads one and two acquire
    Thread threadA = new Thread(group, new Runnable() {
      @Override
      public void run() {
        collaboration.acquireUninterruptibly(topicA);
        try {
          threadAFlag_TestLateComerJoinsIn = true;
          WaitCriterion ev = new WaitCriterion() {
            @Override
            public boolean done() {
              return !threadAFlag_TestLateComerJoinsIn;
            }
            @Override
            public String description() {
              return null;
            }
          };
          DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
        }
        finally {
          collaboration.release();
        }
      }
    });
    threadA.start();
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return threadAFlag_TestLateComerJoinsIn;
      }
      @Override
      public String description() {
        return "wait for ThreadA";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 30 * 1000, 200, true);
    assertTrue(this.collaboration.hasCurrentTopic(threadA));
    assertTrue(this.collaboration.isCurrentTopic(topicA));
    
    Thread threadB = new Thread(group, new Runnable() {
      @Override
      public void run() {
        collaboration.acquireUninterruptibly(topicA);
        try {
          threadBFlag_TestLateComerJoinsIn = true;
          WaitCriterion ev2 = new WaitCriterion() {
            @Override
            public boolean done() {
              return !threadBFlag_TestLateComerJoinsIn;
            }
            @Override
            public String description() {
              return null;
            }
          };
          DistributedTestCase.waitForCriterion(ev2, 60 * 1000, 200, true);
        }
        finally {
          collaboration.release();
        }
      }
    });
    threadB.start();
    ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return threadBFlag_TestLateComerJoinsIn;
      }
      @Override
      public String description() {
        return "";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
    assertTrue(this.collaboration.hasCurrentTopic(threadB));
    
    // thread three blocks for new topic
    Thread threadC = new Thread(group, new Runnable() {
      @Override
      public void run() {
        threadCFlag_TestLateComerJoinsIn = false;
        collaboration.acquireUninterruptibly(topicB);
        try {
          threadCFlag_TestLateComerJoinsIn = true;
          WaitCriterion ev2 = new WaitCriterion() {
            @Override
            public boolean done() {
              return !threadCFlag_TestLateComerJoinsIn;
            }
            @Override
            public String description() {
              return null;
            }
          };
          DistributedTestCase.waitForCriterion(ev2, 60 * 1000, 200, true);
        }
        finally {
          collaboration.release();
        }
      }
    });
    threadC.start();
    ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return threadCFlag_TestLateComerJoinsIn;
      }
      @Override
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
    assertFalse(this.collaboration.hasCurrentTopic(threadC));
    assertFalse(this.collaboration.isCurrentTopic(topicB));
    
    // thread four (lateComer) acquires current topic immediately
    Thread threadD = new Thread(group, new Runnable() {
      @Override
      public void run() {
        collaboration.acquireUninterruptibly(topicA);
        try {
          threadDFlag_TestLateComerJoinsIn = true;
          while(threadDFlag_TestLateComerJoinsIn) {
            try {
              Thread.sleep(10);
            }
            catch (InterruptedException ignore) {fail("interrupted");}
          }
        }
        finally {
          collaboration.release();
        }
      }
    });
    threadD.start();
    ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return threadDFlag_TestLateComerJoinsIn;
      }
      @Override
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
    assertTrue(this.collaboration.hasCurrentTopic(threadD));
    
    // release threadA
    this.threadAFlag_TestLateComerJoinsIn = false;
    DistributedTestCase.join(threadA, 30 * 1000, null);
    assertFalse(this.collaboration.hasCurrentTopic(threadA));
    assertTrue(this.collaboration.hasCurrentTopic(threadB));
    assertFalse(this.collaboration.hasCurrentTopic(threadC));
    assertTrue(this.collaboration.hasCurrentTopic(threadD));
    assertTrue(this.collaboration.isCurrentTopic(topicA));
    assertFalse(this.collaboration.isCurrentTopic(topicB));
    
    // release threadB
    this.threadBFlag_TestLateComerJoinsIn = false;
    DistributedTestCase.join(threadB, 30 * 1000, null);
    assertFalse(this.collaboration.hasCurrentTopic(threadB));
    assertFalse(this.collaboration.hasCurrentTopic(threadC));
    assertTrue(this.collaboration.hasCurrentTopic(threadD));
    assertTrue(this.collaboration.isCurrentTopic(topicA));
    assertFalse(this.collaboration.isCurrentTopic(topicB));
    
    // release threadD
    this.threadDFlag_TestLateComerJoinsIn = false;
    DistributedTestCase.join(threadD, 30 * 1000, null);
    ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return threadCFlag_TestLateComerJoinsIn;
      }
      @Override
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
    assertTrue(this.collaboration.hasCurrentTopic(threadC));
    assertFalse(this.collaboration.hasCurrentTopic(threadD));
    assertFalse(this.collaboration.isCurrentTopic(topicA));
    assertTrue(this.collaboration.isCurrentTopic(topicB));
    
    // release threadC
    this.threadCFlag_TestLateComerJoinsIn = false;
    DistributedTestCase.join(threadC, 30 * 1000, null);
    assertFalse(this.collaboration.hasCurrentTopic(threadC));
    assertFalse(this.collaboration.isCurrentTopic(topicA));
    assertFalse(this.collaboration.isCurrentTopic(topicB));
  }
  
  protected List waitingList = Collections.synchronizedList(new ArrayList());
  protected List fairnessList = Collections.synchronizedList(new ArrayList());
  protected volatile boolean runTestFairnessStressfully = true;
  
  @Test
  public void testFairnessStressfully() throws Exception {
    this.log.info("[testFairnessStressfully]");
    final int numThreads = 20;
    Thread threads[] = new Thread[numThreads];
    
    Runnable run = new Runnable() {
      public void run() {
        boolean released = false;
        try {
          String uniqueTopic = Thread.currentThread().getName();
          while(runTestFairnessStressfully) {
            waitingList.add(uniqueTopic);
            collaboration.acquireUninterruptibly(uniqueTopic);
            try {
              released = false;
              fairnessList.add(uniqueTopic);
              waitingList.remove(uniqueTopic);
            }
            finally {
              // wait for the other threads to line up...
              WaitCriterion ev = new WaitCriterion() {
                @Override
                public boolean done() {
                  return !runTestFairnessStressfully || waitingList.size() >= numThreads - 1;
                }
                @Override
                public String description() {
                  return "other threads lining up";
                }
              };
              DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
              collaboration.release();
              released = true;
            }
          }
        }
        finally {
          if (!released) {
            collaboration.release();
          }
        }
      }
    };
    
    try {
      // many threads loop: acquire and release with unique topic
      for (int t = 0; t < threads.length; t++) {
        threads[t] = new Thread(group, run, String.valueOf(t));
        threads[t].start();
      }

      log.info("Started all threads... waiting for test to complete.");
            
      // wait for numThreads * 10
      WaitCriterion ev = new WaitCriterion() {
        @Override
        public boolean done() {
          return fairnessList.size() >= numThreads * 20;
        }
        @Override
        public String description() {
          return "waiting for numThreads * 10";
        }
      };
      DistributedTestCase.waitForCriterion(ev, 5 * 60 * 1000, 200, true);
    }
    finally {
      if (this.runTestFairnessStressfully) {
        this.runTestFairnessStressfully = false;
      }
    }
    
    for (int t = 0; t < threads.length; t++) {
      DistributedTestCase.join(threads[t], 30 * 1000, null);
    }
    
    // assert that all topics are acquired in order
    // count number of occurrences of each thread
    int count[] = new int[numThreads];
    for (int i = 0; i < count.length; i++) { // shouldn't be necessary
      count[i] = 0;
    }
    synchronized(this.fairnessList) {
      for (Iterator iter = this.fairnessList.iterator(); iter.hasNext();) {
        int id = Integer.valueOf((String)iter.next()).intValue();
        count[id] = count[id]+1;
      }
    }
    
    int totalLocks = 0;
    int minLocks = Integer.MAX_VALUE;
    int maxLocks = 0;
    for (int i = 0; i < count.length; i++) {
      int locks = count[i];
      this.log.fine("testFairnessStressfully thread-" + i + " acquired topic " + 
        locks + " times.");
      if (locks < minLocks) minLocks = locks;
      if (locks > maxLocks) maxLocks = locks;
      totalLocks = totalLocks + locks;
    }

    this.log.info("[testFairnessStressfully] totalLocks=" + totalLocks + 
                  " minLocks=" + minLocks +
                  " maxLocks=" + maxLocks);

    int expectedLocks = (totalLocks / numThreads) + 1;
    
    // NOTE: if you turn on fine logs, this deviation may be too small...
    // slower machines may also fail depending on thread scheduling
    int deviation = (int)(expectedLocks * 0.25);
    int lowThreshold = expectedLocks - deviation;
    int highThreshold = expectedLocks + deviation;

    this.log.info("[testFairnessStressfully] deviation=" + deviation +
                  " expectedLocks=" + expectedLocks + 
                  " lowThreshold=" + lowThreshold +
                  " highThreshold=" + highThreshold);
                        
    // if these assertions keep failing we'll have to rewrite the test
    // to handle scheduling of the threads...
                  
    assertTrue("minLocks is less than lowThreshold",
               minLocks >= lowThreshold);
    assertTrue("maxLocks is greater than highThreshold",
               maxLocks <= highThreshold);
  }
  
  @Test
  public void testHasCurrentTopic() throws Exception {
    this.log.info("[testHasCurrentTopic]");
    assertTrue(!this.collaboration.hasCurrentTopic());
    this.collaboration.acquireUninterruptibly("testHasCurrentTopic");
    try {
      assertTrue(this.collaboration.hasCurrentTopic());
    }
    finally {
      this.collaboration.release();
    }
    assertTrue(!this.collaboration.hasCurrentTopic());
  }
  
  protected volatile boolean flagTestThreadHasCurrentTopic = false;

  @Test
  public void testThreadHasCurrentTopic() throws Exception {
    this.log.info("[testThreadHasCurrentTopic]");
    Thread thread = new Thread(group, new Runnable() {
      @Override
      public void run() {
        collaboration.acquireUninterruptibly("testThreadHasCurrentTopic");
        try {
          flagTestThreadHasCurrentTopic = true;
          WaitCriterion ev = new WaitCriterion() {
            @Override
            public boolean done() {
              return !flagTestThreadHasCurrentTopic;
            }
            @Override
            public String description() {
              return null;
            }
          };
          DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
        }
        finally {
          collaboration.release();
        }
      }
    });
    
    // before starting thread, hasCurrentTopic(thread) returns false
    assertTrue(!this.collaboration.hasCurrentTopic(thread));
    thread.start();
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return flagTestThreadHasCurrentTopic;
      }
      @Override
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
    
    // after starting thread, hasCurrentTopic(thread) returns true
    assertTrue(this.collaboration.hasCurrentTopic(thread));
    this.flagTestThreadHasCurrentTopic = false;
    DistributedTestCase.join(thread, 30 * 1000, null);
    
    // after thread finishes, hasCurrentTopic(thread) returns false
    assertTrue(!this.collaboration.hasCurrentTopic(thread));
  }
  
  @Test
  public void testIsCurrentTopic() throws Exception {
    this.log.info("[testIsCurrentTopic]");
    Object topic = "testIsCurrentTopic";
    assertTrue(!this.collaboration.isCurrentTopic(topic));
    this.collaboration.acquireUninterruptibly(topic);
    try {
      assertTrue(this.collaboration.isCurrentTopic(topic));
    }
    finally {
      this.collaboration.release();
    }
    assertTrue(!this.collaboration.isCurrentTopic(topic));
  }

  protected final ThreadGroup group = 
      new ThreadGroup("CollaborationJUnitTest Threads") {
        @Override
        public void uncaughtException(Thread t, Throwable e)
        {
          if (e instanceof VirtualMachineError) {
            SystemFailure.setFailure((VirtualMachineError)e); // don't throw
          }
          String s = "Uncaught exception in thread " + t;
          log.error(s, e);
          fail(s);
        }
      };
}

