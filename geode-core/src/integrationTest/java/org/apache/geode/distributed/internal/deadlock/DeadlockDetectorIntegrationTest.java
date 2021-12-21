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
package org.apache.geode.distributed.internal.deadlock;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


/**
 * TODO: can we get rid of the Thread.sleep calls?
 */
public class DeadlockDetectorIntegrationTest {

  private volatile Set<Thread> stuckThreads;

  @Before
  public void setUp() throws Exception {
    stuckThreads = Collections.synchronizedSet(new HashSet<>());
  }

  /**
   * IntegrationTests are forkEvery 1 so cleanup is not necessary
   */
  @After
  public void tearDown() throws Exception {
    for (Thread thread : stuckThreads) {
      thread.interrupt();
    }

    stuckThreads.clear();
  }

  /**
   * must be IntegrationTest because: "we can't clean up the threads deadlocked on monitors"
   */
  @Test
  public void testMonitorDeadlock() throws Exception {
    final Object lock1 = new Object();
    final Object lock2 = new Object();

    Thread thread1 = new Thread() {
      @Override
      public void run() {
        stuckThreads.add(Thread.currentThread());

        synchronized (lock1) {
          Thread thread2 = new Thread() {
            @Override
            public void run() {
              stuckThreads.add(Thread.currentThread());
              synchronized (lock2) {
                synchronized (lock1) {
                  System.out.println("we won't get here");
                }
              }
            }
          };

          thread2.start();

          try {
            Thread.sleep(1000);
            synchronized (lock2) {
              System.out.println("We won't get here");
            }
          } catch (InterruptedException ignore) {
          }
        }
      }
    };

    thread1.start();

    Thread.sleep(2000);

    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();

    System.out.println("deadlocks=" + DeadlockDetector.prettyFormat(deadlocks));

    assertEquals(4, detector.findDeadlock().size());
  }

  /**
   * Make sure that we can detect a deadlock between two threads that are trying to acquire a two
   * different syncs in the different orders.
   */
  @Test
  public void testSyncDeadlock() throws Exception {
    final Lock lock1 = new ReentrantLock();
    final Lock lock2 = new ReentrantLock();

    Thread thread1 = new Thread() {
      @Override
      public void run() {
        stuckThreads.add(Thread.currentThread());

        lock1.lock();

        Thread thread2 = new Thread() {
          @Override
          public void run() {
            stuckThreads.add(Thread.currentThread());
            lock2.lock();
            try {
              lock1.tryLock(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              // ignore
            }
            lock2.unlock();
          }
        };

        thread2.start();

        try {
          Thread.sleep(1000);
          lock2.tryLock(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {
        }

        lock1.unlock();
      }
    };

    thread1.start();

    Thread.sleep(2000);

    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();

    System.out.println("deadlocks=" + DeadlockDetector.prettyFormat(deadlocks));

    assertEquals(4, detector.findDeadlock().size());
  }

  @Ignore("Semaphore deadlock detection is not supported by the JDK")
  @Test
  public void testSemaphoreDeadlock() throws Exception {
    final Semaphore lock1 = new Semaphore(1);
    final Semaphore lock2 = new Semaphore(1);

    Thread thread1 = new Thread() {
      @Override
      public void run() {
        stuckThreads.add(Thread.currentThread());

        try {
          lock1.acquire();
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }

        Thread thread2 = new Thread() {
          @Override
          public void run() {
            stuckThreads.add(Thread.currentThread());
            try {
              lock2.acquire();
              lock1.tryAcquire(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {
            }
            lock2.release();
          }
        };

        thread2.start();

        try {
          Thread.sleep(1000);
          lock2.tryAcquire(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {
        }

        lock1.release();
      }
    };

    thread1.start();

    Thread.sleep(2000);

    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();

    System.out.println("deadlocks=" + DeadlockDetector.prettyFormat(deadlocks));

    assertEquals(4, detector.findDeadlock().size());
  }

  @Ignore("ReadWriteLock deadlock detection is not currently supported by DeadlockDetector")
  @Test
  public void testReadLockDeadlock() throws Exception {
    final ReadWriteLock lock1 = new ReentrantReadWriteLock();
    final ReadWriteLock lock2 = new ReentrantReadWriteLock();

    Thread thread1 = new Thread() {
      @Override
      public void run() {
        stuckThreads.add(Thread.currentThread());

        lock1.readLock().lock();

        Thread thread2 = new Thread() {
          @Override
          public void run() {
            stuckThreads.add(Thread.currentThread());
            lock2.readLock().lock();
            try {
              lock1.writeLock().tryLock(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignore) {
            }
            lock2.readLock().unlock();
          }
        };

        thread2.start();

        try {
          Thread.sleep(1000);
          lock2.writeLock().tryLock(10, TimeUnit.SECONDS);
        } catch (InterruptedException ignore) {
        }

        lock1.readLock().unlock();
      }
    };

    thread1.start();

    Thread.sleep(2000);

    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();

    System.out.println("deadlocks=" + deadlocks);

    assertEquals(4, detector.findDeadlock().size());
  }

  /**
   * A fake dependency monitor.
   */
  private static class MockDependencyMonitor implements DependencyMonitor {

    Set<Dependency<Thread, Serializable>> blockedThreads = new HashSet<>();
    Set<Dependency<Serializable, Thread>> held = new HashSet<>();

    @Override
    public Set<Dependency<Thread, Serializable>> getBlockedThreads(Thread[] allThreads) {
      return blockedThreads;
    }

    public void addDependency(String resource, Thread thread) {
      held.add(new Dependency<>(resource, thread));
    }

    public void addDependency(Thread thread, String resource) {
      blockedThreads.add(new Dependency<>(thread, resource));
    }

    @Override
    public Set<Dependency<Serializable, Thread>> getHeldResources(Thread[] allThreads) {
      return held;
    }
  }
}
