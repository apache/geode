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
package org.apache.geode.cache.query;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class MultithreadedTester {


  public static Collection<Object> runMultithreaded(Collection<Callable> callables)
      throws InterruptedException {
    final CountDownLatch allRunnablesAreSubmitted = new CountDownLatch(callables.size());
    final CountDownLatch callablesComplete = new CountDownLatch(callables.size());
    final ExecutorService executor = Executors.newFixedThreadPool(callables.size());
    final LinkedList<Future> futures = new LinkedList<>();
    // Submit all tasks to the executor
    callables.forEach(callable -> {
      futures.add(executor.submit(() -> {
        try {
          allRunnablesAreSubmitted.countDown();
          allRunnablesAreSubmitted.await(60, TimeUnit.SECONDS);
          return callable.call();
        } catch (Throwable t) {
          return t;
        } finally {
          callablesComplete.countDown();
        }
      }));
    });
    // Wait until all tasks are complete
    callablesComplete.await(60, TimeUnit.SECONDS);
    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);
    return convertFutureToResult(futures);
  }

  private static Collection<Object> convertFutureToResult(final Collection<Future> futures) {
    List<Object> results = new LinkedList<Object>();
    futures.forEach(future -> {
      try {
        results.add(future.get());
      } catch (Exception e) {
        results.add(e);
      }
    });
    return results;
  }
}
