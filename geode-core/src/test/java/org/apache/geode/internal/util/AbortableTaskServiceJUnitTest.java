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
package org.apache.geode.internal.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.util.AbortableTaskService.AbortableTask;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class AbortableTaskServiceJUnitTest {

  private static final long TIMEOUT_SECONDS = 10;

  private volatile CountDownLatch delay;
  private AbortableTaskService tasks;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() {
    delay = new CountDownLatch(1);
    tasks = new AbortableTaskService(Executors.newSingleThreadExecutor());
  }

  @After
  public void tearDown() {
    if (delay != null && delay.getCount() > 0) {
      delay.countDown();
    }
    tasks.abortAll();
  }

  @Test
  public void testExecute() throws Exception {
    DelayedTask dt = new DelayedTask();
    tasks.execute(dt);

    Future<Boolean> future = executorServiceRule.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        tasks.waitForCompletion();
        return tasks.isCompleted();
      }
    });

    delay.countDown();

    assertTrue(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertFalse(dt.wasAborted.get());
    assertTrue(dt.wasRun.get());
    assertTrue(tasks.isCompleted());
  }

  @Test
  public void testAbortDuringExecute() throws Exception {
    DelayedTask dt = new DelayedTask();
    tasks.execute(dt);

    Future<Boolean> future = executorServiceRule.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        tasks.waitForCompletion();
        return tasks.isCompleted();
      }
    });

    tasks.abortAll();
    delay.countDown();

    assertTrue(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(dt.wasAborted.get());
    // assertTrue(dt.wasRun.get()); -- race condition can result in true or false
    assertTrue(tasks.isCompleted());
  }

  @Test
  public void testAbortBeforeExecute() throws Exception {
    // delay underlying call to execute(Runnable) until after abortAll() is invoked
    Executor executor =
        (Executor) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[] {Executor.class},
            new DelayedExecutorHandler(Executors.newSingleThreadExecutor(), "execute"));
    tasks = new AbortableTaskService(executor);

    DelayedTask dt = new DelayedTask();
    DelayedTask dt2 = new DelayedTask();

    tasks.execute(dt);
    tasks.execute(dt2);

    Future<Boolean> future = executorServiceRule.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        tasks.waitForCompletion();
        return tasks.isCompleted();
      }
    });

    tasks.abortAll();
    delay.countDown();

    assertTrue(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertTrue(dt.wasAborted.get());
    assertTrue(dt2.wasAborted.get());
    assertFalse(dt.wasRun.get());
    assertFalse(dt2.wasRun.get());
    assertTrue(tasks.isCompleted());
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
      wasRun.set(true);
      wasAborted.set(aborted.get());
    }

    @Override
    public void abortBeforeRun() {
      wasAborted.set(true);
    }
  }

  /**
   * Proxy handler which invokes methodName asynchronously AND delays the invocation to the
   * underlying methodName until after CountDownLatch is opened.
   */
  private class DelayedExecutorHandler implements InvocationHandler {
    private final Executor executor;
    private final String methodName;
    private final Executor async;

    public DelayedExecutorHandler(Executor executor, String methodName) {
      this.executor = executor;
      this.methodName = methodName;
      async = Executors.newSingleThreadExecutor();
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args)
        throws Throwable {
      async.execute(new Runnable() {
        @Override
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
