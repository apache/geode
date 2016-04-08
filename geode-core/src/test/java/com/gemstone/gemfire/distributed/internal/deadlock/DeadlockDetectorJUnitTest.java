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
package com.gemstone.gemfire.distributed.internal.deadlock;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 *
 */
@Category(UnitTest.class)
public class DeadlockDetectorJUnitTest {
  
  private final Set<Thread> stuckThreads = Collections.synchronizedSet(new HashSet<Thread>());
  
  @After
  public void tearDown() {
    for(Thread thread: stuckThreads) {
      thread.interrupt();
      try {
        thread.join(20 * 1000);
      } catch (InterruptedException e) {
        throw new RuntimeException("interrupted", e);
      }
      if(thread.isAlive()) {
        fail("Couldn't kill" + thread);
      }
    }
    stuckThreads.clear();
  }
  
  @Test
  public void testNoDeadlocks() {
    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    assertEquals(null, detector.findDeadlock());
  }
  
  //this is commented out, because we can't
  //clean up the threads deadlocked on monitors.
  @Ignore
  @Test
  public void testMonitorDeadlock() throws InterruptedException {
    final Object lock1 = new Object();
    final Object lock2 = new Object();
    Thread thread1 =  new Thread() {
      public void run() {
        stuckThreads.add(Thread.currentThread());
        synchronized(lock1) {
          Thread thread2 = new Thread() {
            public void run() {
              stuckThreads.add(Thread.currentThread());
              synchronized(lock2) {
               synchronized(lock1) {
                 System.out.println("we won't get here");
               }
              }
            }
          };
          thread2.start();
          try {
            Thread.sleep(1000);
            synchronized(lock2) {
              System.out.println("We won't get here");
            }
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      }
    };
    
    thread1.start();
    Thread.sleep(2000);
    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();
    System.out.println("deadlocks=" +  DeadlockDetector.prettyFormat(deadlocks));
    assertEquals(4, detector.findDeadlock().size());
  }

  /**
   * Make sure that we can detect a deadlock between two threads
   * that are trying to acquire a two different syncs in the different orders.
   * @throws InterruptedException
   */
  @Test
  public void testSyncDeadlock() throws InterruptedException {

    final Lock lock1 = new ReentrantLock();
    final Lock lock2 = new ReentrantLock();
    Thread thread1 =  new Thread() {
      public void run() {
        stuckThreads.add(Thread.currentThread());
        lock1.lock();
        Thread thread2 = new Thread() {
          public void run() {
            stuckThreads.add(Thread.currentThread());
            lock2.lock();
            try {
              lock1.tryLock(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              //ignore
            }
            lock2.unlock();
          }
        };
        thread2.start();
        try {
          Thread.sleep(1000);
          lock2.tryLock(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          //ignore
        }
        lock1.unlock();
      }
    };
    
    thread1.start();
    Thread.sleep(2000);
    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();
    System.out.println("deadlocks=" +  DeadlockDetector.prettyFormat(deadlocks));
    assertEquals(4, detector.findDeadlock().size());
  }
  
  //Semaphore are also not supported by the JDK
  @Ignore
  @Test
  public void testSemaphoreDeadlock() throws InterruptedException {

    final Semaphore lock1 = new Semaphore(1);
    final Semaphore lock2 = new Semaphore(1);
    Thread thread1 =  new Thread() {
      public void run() {
        stuckThreads.add(Thread.currentThread());
        try {
          lock1.acquire();
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        Thread thread2 = new Thread() {
          public void run() {
            stuckThreads.add(Thread.currentThread());
            try {
              lock2.acquire();
              lock1.tryAcquire(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              //ignore
            }
            lock2.release();
          }
        };
        thread2.start();
        try {
          Thread.sleep(1000);
          lock2.tryAcquire(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          //ignore
        }
        lock1.release();
      }
    };
    
    thread1.start();
    Thread.sleep(2000);
    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();
    System.out.println("deadlocks=" +  DeadlockDetector.prettyFormat(deadlocks));
    assertEquals(4, detector.findDeadlock().size());
  }
  
  /**
   * This type of deadlock is currently not detected
   * @throws InterruptedException
   */
  @Ignore
  @Test
  public void testReadLockDeadlock() throws InterruptedException {

    final ReadWriteLock lock1 = new ReentrantReadWriteLock();
    final ReadWriteLock lock2 = new ReentrantReadWriteLock();
    Thread thread1 =  new Thread() {
      public void run() {
        stuckThreads.add(Thread.currentThread());
        lock1.readLock().lock();
        Thread thread2 = new Thread() {
          public void run() {
            stuckThreads.add(Thread.currentThread());
            lock2.readLock().lock();
            try {
              lock1.writeLock().tryLock(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            lock2.readLock().unlock();
          }
        };
        thread2.start();
        try {
          Thread.sleep(1000);
          lock2.writeLock().tryLock(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        lock1.readLock().unlock();
      }
    };
    
    thread1.start();
    Thread.sleep(2000);
    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();
    System.out.println("deadlocks=" +  deadlocks);
    assertEquals(4, detector.findDeadlock().size());
  }
  
  /**
   * Test that the deadlock detector will find deadlocks
   * that are reported by the {@link DependencyMonitorManager}
   */
  @Test
  public void testProgramaticDependencies() {
    final CountDownLatch cdl = new CountDownLatch(1);
    MockDependencyMonitor mock = new MockDependencyMonitor();
    DependencyMonitorManager.addMonitor(mock);
    
    Thread t1 = startAThread(cdl);
    Thread t2 = startAThread(cdl);
    
    String resource1 = "one";
    String resource2 = "two";
    
    mock.addDependency(t1, resource1);
    mock.addDependency(resource1, t2);
    mock.addDependency(t2, resource2);
    mock.addDependency(resource2, t1);
    

    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();
    System.out.println("deadlocks=" + deadlocks);
    assertEquals(4, deadlocks.size());
    cdl.countDown();
    DependencyMonitorManager.removeMonitor(mock);
  }

  private Thread startAThread(final CountDownLatch cdl) {
    Thread thread = new Thread() {
      public void run() {
        try {
          cdl.await();
        } catch (InterruptedException e) {
        }
      }
    };
    thread.start();
    
    return thread;
  }
  
  /**
   * A fake dependency monitor.
   *
   */
  private static class MockDependencyMonitor implements DependencyMonitor {
    
    Set<Dependency<Thread, Serializable>> blockedThreads = new HashSet<Dependency<Thread, Serializable>>();
    Set<Dependency<Serializable, Thread>> held = new HashSet<Dependency<Serializable, Thread>>();

    public Set<Dependency<Thread, Serializable>> getBlockedThreads(
        Thread[] allThreads) {
      return blockedThreads;
    }

    public void addDependency(String resource, Thread thread) {
      held.add(new Dependency<Serializable, Thread>(resource, thread));
      
    }

    public void addDependency(Thread thread, String resource) {
      blockedThreads.add(new Dependency<Thread, Serializable>(thread, resource));
    }

    public Set<Dependency<Serializable, Thread>> getHeldResources(
        Thread[] allThreads) {
      return held;
    }
    
  }

}
