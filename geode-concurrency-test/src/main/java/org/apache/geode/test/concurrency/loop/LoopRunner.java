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

package org.apache.geode.test.concurrency.loop;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.test.concurrency.ParallelExecutor;
import org.apache.geode.test.concurrency.Runner;

/**
 * Simple runner that just runs the test in a loop
 */
public class LoopRunner implements Runner {
  private static final int DEFAULT_COUNT = 2000;

  @Override
  public List<Throwable> runTestMethod(Method child) {
    int count = getCount(child);

    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      ParallelExecutor executor = new DelegatingExecutor(executorService);
      for (int i = 0; i < count; i++) {
        try {
          Object test = child.getDeclaringClass().newInstance();
          child.invoke(test, executor);
        } catch (InvocationTargetException ex) {
          Throwable exceptionToReturn = ex.getCause();
          if (exceptionToReturn == null) {
            exceptionToReturn = ex;
          }
          return Collections.singletonList(ex.getCause());
        } catch (Exception e) {
          return Collections.singletonList(e);
        }
      }
    } finally {
      executorService.shutdown();
    }

    return Collections.emptyList();
  }

  private int getCount(Method child) {
    LoopRunnerConfig config = child.getDeclaringClass().getAnnotation(LoopRunnerConfig.class);
    if (config == null) {
      return DEFAULT_COUNT;
    }

    return config.count();
  }

  private static class DelegatingExecutor implements ParallelExecutor {
    private final ExecutorService executorService;
    private List<Future<?>> futures;
    private final AtomicInteger callablesStarting = new AtomicInteger(0);
    private final CountDownLatch start = new CountDownLatch(1);

    public DelegatingExecutor(ExecutorService executorService) {
      this.executorService = executorService;
      futures = new ArrayList<>();
    }

    @Override
    public <T> Future<T> inParallel(Callable<T> callable) {
      callablesStarting.getAndIncrement();
      Future<T> future = executorService.submit(() -> {
        callablesStarting.getAndDecrement();
        start.await();
        return callable.call();
      });
      futures.add(future);
      return future;
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException {
      while (callablesStarting.get() > 0) {
        ;
      }

      start.countDown();
      for (Future future : futures) {
        future.get();
      }
      futures.clear();
    }

  }
}
