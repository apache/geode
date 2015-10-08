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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * @author dsmith
 *
 */
public class GemFireDeadlockDetectorDUnitTest extends CacheTestCase {
  
  private static final Set<Thread> stuckThreads = Collections.synchronizedSet(new HashSet<Thread>());
  
  
  
  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(new SerializableRunnable() {
      
      public void run() {
        for(Thread thread: stuckThreads) {
          thread.interrupt();
        }
      }
    });
  }

  public GemFireDeadlockDetectorDUnitTest(String name) {
    super(name);
  }
  
  public void testNoDeadlock() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    //Make sure a deadlock from a previous test is cleared.
    disconnectAllFromDS();
    
    createCache(vm0);
    createCache(vm1);
    getSystem();

    GemFireDeadlockDetector detect = new GemFireDeadlockDetector();
    assertEquals(null, detect.find().findCycle());
  }
  
  private static final Lock lock = new ReentrantLock();
  
  
  public void testDistributedDeadlockWithFunction() throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    getSystem();
    InternalDistributedMember member1 = createCache(vm0);
    final InternalDistributedMember member2 = createCache(vm1);

    //Have two threads lock locks on different members in different orders.
    
    
    //This thread locks the lock member1 first, then member2.
    lockTheLocks(vm0, member2);
    //This thread locks the lock member2 first, then member1.
    lockTheLocks(vm1, member1);
    
    Thread.sleep(5000);
    GemFireDeadlockDetector detect = new GemFireDeadlockDetector();
    LinkedList<Dependency> deadlock = detect.find().findCycle();
    getLogWriter().info("Deadlock=" + DeadlockDetector.prettyFormat(deadlock));
    assertEquals(8, deadlock.size());
  }
  
  

  private void lockTheLocks(VM vm0, final InternalDistributedMember member) {
    vm0.invokeAsync(new SerializableRunnable() {

      public void run() {
        lock.lock();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          fail("interrupted", e);
        }
        ResultCollector collector = FunctionService.onMember(member).execute(new TestFunction());
        //wait the function to lock the lock on member.
        collector.getResult();
        lock.unlock();
      }
    });
  }
  
  public void testDistributedDeadlockWithDLock() throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    lockTheDLocks(vm0, "one", "two");
    lockTheDLocks(vm1, "two", "one");
    getSystem();
    GemFireDeadlockDetector detect = new GemFireDeadlockDetector();
    
    LinkedList<Dependency> deadlock = null;
    for(int i =0; i < 60; i ++) {
      deadlock = detect.find().findCycle();
      if(deadlock != null) {
        break;
      }
      Thread.sleep(1000);
    }
    
    assertTrue(deadlock != null);
    getLogWriter().info("Deadlock=" + DeadlockDetector.prettyFormat(deadlock));
    assertEquals(4, deadlock.size());
  } 

  private void lockTheDLocks(VM vm, final String first, final String second) {
    vm.invokeAsync(new SerializableRunnable() {
      
      public void run() {
        getCache();
        DistributedLockService dls = DistributedLockService.create("deadlock_test", getSystem());
        dls.lock(first, 10 * 1000, -1);
        
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        dls.lock(second, 10 * 1000, -1);
        
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
        //ingore
      }
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
