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
package org.apache.geode.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ConcurrencyTestHelper {

  /**
   * Runs a list of Runnables in a thread pool with a default timeout of 30 Seconds. Returns a list
   * of Throwables after the completion of all threads, or once the timeout is reached, whichever
   * happens first.
   *
   * @return List of Throwables of the same size and order as the List of Runnables given as input.
   *         The List of Throwables will contain null for each thread that completes successfully.
   *         The List
   *         of Throwables will contain a CancellationException for each thread that does not
   *         complete
   *         before the timeout. If a thread throws an exception, it will also be added to the List
   *         of
   *         Throwables.
   */
  public static List<Throwable> runRunnables(List<Runnable> runnables) throws InterruptedException {
    return runRunnables(runnables, 30);
  }

  /**
   * Runs a list of Runnables in a thread pool with a timeout in Seconds. Returns a list of
   * Throwables after the completion of all threads, or once the timeout is reached, whichever
   * happens first.
   *
   * @return List of Throwables of the same size and order as the list of runnables given as input.
   *         The List of Throwables will contain null for each thread that completes successfully.
   *         The List
   *         of Throwables will contain a CancellationException for each thread that does not
   *         complete
   *         before the timeout. If a thread throws an exception, it will also be added to the List
   *         of
   *         Throwables.
   */
  public static List<Throwable> runRunnables(List<Runnable> runnables, Integer timeoutSeconds)
      throws InterruptedException {
    final int numThreads = runnables.size();
    final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<Throwable>());
    final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

    List<Callable<Object>> callables = new ArrayList<>();
    for (Runnable runnable : runnables) {
      callables.add(toCallable(runnable));
    }

    List<Future<Object>> futures = new ArrayList<>();
    try {
      futures.addAll(threadPool.invokeAll(callables, timeoutSeconds, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      threadPool.shutdownNow();
      exceptions.add(e);
    }

    for (Future<Object> future : futures) {
      try {
        future.get();
        exceptions.add(null);
      } catch (CancellationException e) {
        exceptions.add(e);
      } catch (InterruptedException e) {
        throw e;
      } catch (ExecutionException e) {
        exceptions.add(e.getCause());
      }
    }

    return exceptions;
  }

  /**
   * Runs a List of Callables in a thread pool with a default timeout of 300 seconds. Returns a List
   * of results after the completion of all threads, or once the timeout is reached, whichever
   * happens first.
   *
   * @param callables of the same return type
   * @param <T>
   * @return List of results of the same type as the return statements of the Callables given as
   *         input. If there are no exceptions thrown in the threads then the list of results will
   *         be in the
   *         order of the callables passed in. If there are exceptions, the stacktrace of the
   *         exceptions
   *         will be printed and a RuntimeException will be thrown.
   * @throws RuntimeException if any of the threads throw ExecutionException or
   *         CancellationException
   */
  public static <T> List<T> runCallables(List<Callable<T>> callables) throws InterruptedException {
    return runCallables(callables, 30);
  }

  /**
   * Runs a List of Callables in a thread pool with a timeout in Seconds. Returns a List
   * of results after the completion of all threads, or once the timeout is reached, whichever
   * happens first.
   *
   * @param callables of the same return type
   * @param <T>
   * @return List of results of the same type as the return statements of the Callables given as
   *         input. If there are no exceptions thrown in the threads then the list of results will
   *         be in the
   *         order of the callables passed in. If there are exceptions, the stacktrace of the
   *         exceptions
   *         will be printed and a RuntimeException will be thrown.
   * @throws RuntimeException if any of the threads throw ExecutionException or
   *         CancellationException
   */
  public static <T> List<T> runCallables(List<Callable<T>> callables, Integer timeoutSeconds)
      throws InterruptedException {
    final int numThreads = callables.size();
    final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
    final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

    List<Future<T>> futures = new ArrayList<>();
    try {
      futures.addAll(threadPool.invokeAll(callables, timeoutSeconds, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      threadPool.shutdownNow();
      throw (e);
    }

    List<T> results = new ArrayList<>();

    for (Future<T> future : futures) {
      try {
        results.add(future.get());
      } catch (ExecutionException e) {
        exceptions.add(e);
      } catch (CancellationException e) {
        exceptions.add(e);
      }
    }

    if (exceptions.size() != 0) {
      for (Exception e : exceptions) {
        e.printStackTrace();
      }

      throw new RuntimeException("One or more threads threw exceptions");
    }

    return results;
  }

  private static Callable<Object> toCallable(final Runnable runnable) {
    return () -> {
      runnable.run();
      return null;
    };
  }
}
