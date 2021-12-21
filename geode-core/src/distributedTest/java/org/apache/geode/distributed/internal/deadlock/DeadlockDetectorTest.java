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
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class DeadlockDetectorTest {

  private volatile Set<Thread> stuckThreads;

  @Before
  public void setUp() throws Exception {
    stuckThreads = Collections.synchronizedSet(new HashSet<>());
  }

  @After
  public void tearDown() throws Exception {
    for (Thread thread : stuckThreads) {
      thread.interrupt();
      thread.join(20 * 1000);
      if (thread.isAlive()) {
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

  /**
   * Test that the deadlock detector will find deadlocks that are reported by the
   * {@link DependencyMonitorManager}
   */
  @Test
  public void testProgrammaticDependencies() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    MockDependencyMonitor mockDependencyMonitor = new MockDependencyMonitor();
    DependencyMonitorManager.addMonitor(mockDependencyMonitor);

    Thread thread1 = startAThread(latch);
    Thread thread2 = startAThread(latch);

    String resource1 = "one";
    String resource2 = "two";

    mockDependencyMonitor.addDependency(thread1, resource1);
    mockDependencyMonitor.addDependency(resource1, thread2);
    mockDependencyMonitor.addDependency(thread2, resource2);
    mockDependencyMonitor.addDependency(resource2, thread1);

    DeadlockDetector detector = new DeadlockDetector();
    detector.addDependencies(DeadlockDetector.collectAllDependencies("here"));
    LinkedList<Dependency> deadlocks = detector.findDeadlock();

    System.out.println("deadlocks=" + deadlocks);

    assertEquals(4, deadlocks.size());

    latch.countDown();
    DependencyMonitorManager.removeMonitor(mockDependencyMonitor);
  }

  private Thread startAThread(final CountDownLatch latch) {
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          latch.await();
        } catch (InterruptedException ignore) {
        }
      }
    };

    thread.start();

    return thread;
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
