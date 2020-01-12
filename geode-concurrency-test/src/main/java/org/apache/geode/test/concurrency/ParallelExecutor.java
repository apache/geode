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
package org.apache.geode.test.concurrency;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Executor that executes multiple tasks in parallel as part of the main body of concurrent test.
 *
 * See {@link ConcurrentTestRunner}
 */
public interface ParallelExecutor {

  /**
   * Add a task to run in parallel
   */
  <T> Future<T> inParallel(Callable<T> callable);

  /**
   * Add a task to run in parallel
   */
  default <T> Future<T> inParallel(RunnableWithException runnable) {
    return inParallel(() -> {
      runnable.run();
      return null;
    });
  }

  default <T> Collection<Future<T>> inParallel(RunnableWithException runnable, int count) {
    ArrayList<Future<T>> futures = new ArrayList<>(count);
    for (; count > 0; count--) {
      futures.add(inParallel(runnable));
    }
    return futures;
  }

  /**
   * Execute all tasks in parallel, wait for them to complete and throw an exception if any of the
   * tasks throw an exception.
   */
  void execute() throws ExecutionException, InterruptedException;

}
