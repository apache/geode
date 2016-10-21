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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class GemFireDeadlockDetectorDUnitTest extends JUnit4CacheTestCase {

  private static final Set<Thread> stuckThreads =
      Collections.synchronizedSet(new HashSet<Thread>());

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  private void stopStuckThreads() {
    Invoke.invokeInEveryVM(new SerializableRunnable() {

      public void run() {
        for (Thread thread : stuckThreads) {
          thread.interrupt();
          disconnectFromDS();
          try {
            thread.join(30000);
            assertTrue(!thread.isAlive());
          } catch (InterruptedException e) {
            Assert.fail("interrupted", e);
          }
        }
      }
    });
  }

  public GemFireDeadlockDetectorDUnitTest() {
    super();
  }

  @Test
  public void testNoDeadlock() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Make sure a deadlock from a previous test is cleared.
    disconnectAllFromDS();

    createCache(vm0);
    createCache(vm1);
    getSystem();

    GemFireDeadlockDetector detect = new GemFireDeadlockDetector();
    assertEquals(null, detect.find().findCycle());
  }

  private static final Lock lock = new ReentrantLock();

  @Category(FlakyTest.class) // GEODE-516 & GEODE-576: async actions, thread sleeps, time sensitive
  @Test
  public void testDistributedDeadlockWithFunction() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    getSystem();
    InternalDistributedMember member1 = createCache(vm0);
    final InternalDistributedMember member2 = createCache(vm1);

    // Have two threads lock locks on different members in different orders.


    // This thread locks the lock member1 first, then member2.
    AsyncInvocation async1 = lockTheLocks(vm0, member2);
    // This thread locks the lock member2 first, then member1.
    AsyncInvocation async2 = lockTheLocks(vm1, member1);

    Thread.sleep(5000);
    GemFireDeadlockDetector detect = new GemFireDeadlockDetector();
    LinkedList<Dependency> deadlock = detect.find().findCycle();
    LogWriterUtils.getLogWriter().info("Deadlock=" + DeadlockDetector.prettyFormat(deadlock));
    assertEquals(8, deadlock.size());
    stopStuckThreads();
    async1.getResult(30000);
    async2.getResult(30000);
  }



  private AsyncInvocation lockTheLocks(VM vm0, final InternalDistributedMember member) {
    return vm0.invokeAsync(new SerializableRunnable() {

      public void run() {
        lock.lock();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Assert.fail("interrupted", e);
        }
        ResultCollector collector = FunctionService.onMember(member).execute(new TestFunction());
        // wait the function to lock the lock on member.
        collector.getResult();
        lock.unlock();
      }
    });
  }

  @Category(FlakyTest.class) // GEODE-1580 uses asyncs with pauses
  @Test
  public void testDistributedDeadlockWithDLock() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    AsyncInvocation async1 = lockTheDLocks(vm0, "one", "two");
    AsyncInvocation async2 = lockTheDLocks(vm1, "two", "one");
    getSystem();
    GemFireDeadlockDetector detect = new GemFireDeadlockDetector();

    LinkedList<Dependency> deadlock = null;
    for (int i = 0; i < 60; i++) {
      deadlock = detect.find().findCycle();
      if (deadlock != null) {
        break;
      }
      Thread.sleep(1000);
    }

    assertTrue(deadlock != null);
    LogWriterUtils.getLogWriter().info("Deadlock=" + DeadlockDetector.prettyFormat(deadlock));
    assertEquals(4, deadlock.size());
    disconnectAllFromDS();
    async1.getResult(30000);
    async2.getResult(30000);
  }

  private AsyncInvocation lockTheDLocks(VM vm, final String first, final String second) {
    return vm.invokeAsync(new SerializableRunnable() {

      public void run() {
        getCache();
        DistributedLockService dls = DistributedLockService.create("deadlock_test", getSystem());
        dls.lock(first, 10 * 1000, -1);

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        try {
          dls.lock(second, 10 * 1000, -1);
        } catch (LockServiceDestroyedException expected) {
          // this is ok, the test is terminating
        } catch (DistributedSystemDisconnectedException expected) {
          // this is ok, the test is terminating
        }

      }
    });

  }

  private InternalDistributedMember createCache(VM vm) {
    return (InternalDistributedMember) vm.invoke(new SerializableCallable() {
      public Object call() {
        getCache();
        return getSystem().getDistributedMember();
      }
    });
  }

  private static class TestFunction implements Function {

    private static final int LOCK_WAIT_TIME = 1000;

    public boolean hasResult() {
      return true;
    }


    public void execute(FunctionContext context) {
      try {
        stuckThreads.add(Thread.currentThread());
        lock.tryLock(LOCK_WAIT_TIME, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // ingore
      }
      stuckThreads.remove(Thread.currentThread());
      context.getResultSender().lastResult(null);
    }

    public String getId() {
      return getClass().getCanonicalName();
    }

    public boolean optimizeForWrite() {
      return false;
    }

    public boolean isHA() {
      return false;
    }


  }

}
