/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

import static org.junit.Assert.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.util.AbortableTaskService.AbortableTask;
import com.gemstone.junit.UnitTest;

@Category(UnitTest.class)
public class AbortableTaskServiceJUnitTest {

  private static final long TIMEOUT_SECONDS = 10;
  
  private volatile CountDownLatch delay;
  private ExecutorService executor;
  private AbortableTaskService tasks;
  
  @Before
  public void setUp() {
    this.delay = new CountDownLatch(1);
    this.tasks = new AbortableTaskService(Executors.newSingleThreadExecutor());
    this.executor = Executors.newSingleThreadExecutor();
  }
  
  @After
  public void tearDown() {
    if (this.delay != null && this.delay.getCount() > 0) {
      this.delay.countDown();
    }
    try {
      assertTrue(this.executor.shutdownNow().isEmpty());
    } finally {
      this.tasks.abortAll();
    }
  }
  
  @Test
  public void testExecute() throws Exception {
    DelayedTask dt = new DelayedTask();
    this.tasks.execute(dt);
    
    Future<Boolean> future = this.executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        tasks.waitForCompletion();
        return tasks.isCompleted();
      }
    });
    
    this.delay.countDown();

    assertTrue(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertFalse(dt.wasAborted.get());
    assertTrue(dt.wasRun.get());
    assertTrue(this.tasks.isCompleted());
  }
  
  @Test
  public void testAbortDuringExecute() throws Exception {
    DelayedTask dt = new DelayedTask();
    this.tasks.execute(dt);
    
    Future<Boolean> future = this.executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        tasks.waitForCompletion();
        return tasks.isCompleted();
      }
    });
    
    this.tasks.abortAll();
    this.delay.countDown();

    assertTrue(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(dt.wasAborted.get());
    //assertTrue(dt.wasRun.get()); -- race condition can result in true or false
    assertTrue(this.tasks.isCompleted());
  }
  
  @Test
  public void testAbortBeforeExecute() throws Exception {
    // delay underlying call to execute(Runnable) until after abortAll() is invoked
    Executor executor = (Executor) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] {Executor.class}, new DelayedExecutorHandler(Executors.newSingleThreadExecutor(), "execute"));
    this.tasks = new AbortableTaskService(executor);
    
    DelayedTask dt = new DelayedTask();
    DelayedTask dt2 = new DelayedTask();
    
    this.tasks.execute(dt);
    this.tasks.execute(dt2);
    
    Future<Boolean> future = this.executor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        tasks.waitForCompletion();
        return tasks.isCompleted();
      }
    });

    this.tasks.abortAll();
    this.delay.countDown();
    
    assertTrue(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(dt.wasAborted.get());
    assertTrue(dt2.wasAborted.get());
    assertFalse(dt.wasRun.get());
    assertFalse(dt2.wasRun.get());
    assertTrue(this.tasks.isCompleted());
  }
  
  /**
   * AbortableTask that waits on the CountDownLatch proceeding.
   */
  private class DelayedTask implements AbortableTask {
    private final AtomicBoolean wasAborted = new AtomicBoolean(false);
    private final AtomicBoolean wasRun = new AtomicBoolean(false);

    @Override
    public void runOrAbort(AtomicBoolean aborted) {
      try {
        delay.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      this.wasRun.set(true);
      this.wasAborted.set(aborted.get());
    }

    @Override
    public void abortBeforeRun() {
      this.wasAborted.set(true);
    }
  }

  /**
   * Proxy handler which invokes methodName asynchronously AND delays the 
   * invocation to the underlying methodName until after CountDownLatch is 
   * opened.
   * 
   * @author Kirk Lund
   */
  private class DelayedExecutorHandler implements InvocationHandler {
    private final Executor executor;
    private final String methodName;
    private final Executor async;
    public DelayedExecutorHandler(Executor executor, String methodName) {
      this.executor = executor;
      this.methodName = methodName;
      this.async = Executors.newSingleThreadExecutor();
    }
    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
      this.async.execute(new Runnable() {
        public void run() {
          try {
            if (method.getName().equals(methodName)) {
              delay.await();
            }
            method.invoke(executor, args);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            throw new Error(e);
          }
        }
      });
      return null;
    }
  }
}
