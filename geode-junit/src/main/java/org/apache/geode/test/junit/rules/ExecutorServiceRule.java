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
package org.apache.geode.test.junit.rules;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

/**
 * Provides a reusable mechanism for executing tasks asynchronously in tests. This {@code Rule}
 * creates an {@code ExecutorService} which is terminated after the scope of the {@code Rule}. This
 * {@code Rule} can be used in tests for hangs, deadlocks, and infinite loops.
 *
 * <pre>
 * private CountDownLatch hangLatch = new CountDownLatch(1);
 *
 * {@literal @}Rule
 * public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();
 *
 * {@literal @}Test
 * public void doTest() throws Exception {
 *   Future<Void> result = executorServiceRule.runAsync(() -> {
 *     try {
 *       hangLatch.await();
 *     } catch (InterruptedException e) {
 *       throw new RuntimeException(e);
 *     }
 *   });
 *
 *   assertThatThrownBy(() -> result.get(1, MILLISECONDS)).isInstanceOf(TimeoutException.class);
 * }
 * </pre>
 *
 * <p>
 * The {@code Rule} can be configured to await termination by specifying
 * {@link Builder#awaitTermination(long, TimeUnit)}. If all tasks have not terminated by the
 * specified timeout, then {@code TimeoutException} will be thrown. This has the potential to
 * obscure any {@code Throwable}s thrown by the test itself.
 *
 * <p>
 * Example with awaitTermination enabled. Awaits up to timeout for all submitted tasks to terminate.
 * This causes the {@code Rule} to invoke awaitTermination during its tear down:
 *
 * <pre>
 * private CountDownLatch hangLatch = new CountDownLatch(1);
 *
 * {@literal @}Rule
 * public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.builder().awaitTermination(10, SECONDS).build();
 *
 * {@literal @}Test
 * public void doTest() throws Exception {
 *   for (int i = 0; i < 10; i++) {
 *     executorServiceRule.runAsync(() -> {
 *       hangLatch.await();
 *     });
 *   }
 * }
 * </pre>
 */
@SuppressWarnings("unused")
public class ExecutorServiceRule extends SerializableExternalResource {

  protected final boolean enableAwaitTermination;
  protected final long awaitTerminationTimeout;
  protected final TimeUnit awaitTerminationTimeUnit;
  protected final boolean awaitTerminationBeforeShutdown;
  protected final boolean useShutdown;
  protected final boolean useShutdownNow;

  protected transient volatile ExecutorService executor;

  /**
   * Returns a {@code Builder} to configure a new {@code ExecutorServiceRule}.
   */
  public static Builder builder() {
    return new Builder();
  }

  protected ExecutorServiceRule(Builder builder) {
    enableAwaitTermination = builder.enableAwaitTermination;
    awaitTerminationTimeout = builder.awaitTerminationTimeout;
    awaitTerminationTimeUnit = builder.awaitTerminationTimeUnit;
    awaitTerminationBeforeShutdown = builder.awaitTerminationBeforeShutdown;
    useShutdown = builder.useShutdown;
    useShutdownNow = builder.useShutdownNow;
  }

  /**
   * Constructs a {@code ExecutorServiceRule} which invokes {@code ExecutorService.shutdownNow()}
   * during {@code tearDown}.
   */
  public ExecutorServiceRule() {
    enableAwaitTermination = false;
    awaitTerminationTimeout = 0;
    awaitTerminationTimeUnit = TimeUnit.NANOSECONDS;
    awaitTerminationBeforeShutdown = false;
    useShutdown = false;
    useShutdownNow = true;
  }

  @Override
  public void before() {
    executor = Executors.newCachedThreadPool();
  }

  @Override
  public void after() {
    if (awaitTerminationBeforeShutdown) {
      enableAwaitTermination();
    }
    if (useShutdown) {
      executor.shutdown();
    } else if (useShutdownNow) {
      executor.shutdownNow();
    }
    if (!awaitTerminationBeforeShutdown) {
      enableAwaitTermination();
    }
  }

  private void enableAwaitTermination() {
    if (enableAwaitTermination) {
      try {
        executor.awaitTermination(awaitTerminationTimeout, awaitTerminationTimeUnit);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Returns a direct reference to the underlying {@code ExecutorService}.
   */
  public ExecutorService getExecutorService() {
    return executor;
  }

  /**
   * Executes the given command at some time in the future.
   *
   * @param command the runnable task
   * @throws RejectedExecutionException if this task cannot be accepted for execution
   * @throws NullPointerException if command is null
   */
  public void execute(Runnable command) {
    executor.execute(command);
  }

  /**
   * Submits a value-returning task for execution and returns a Future representing the pending
   * results of the task. The Future's {@code get} method will return the task's result upon
   * successful completion.
   *
   * <p>
   * If you would like to immediately block waiting for a task, you can use constructions of the
   * form {@code result = exec.submit(aCallable).get();}
   *
   * @param task the task to submit
   * @param <T> the type of the task's result
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if the task is null
   */
  public <T> Future<T> submit(Callable<T> task) {
    return executor.submit(task);
  }

  /**
   * Submits a Runnable task for execution and returns a Future representing that task. The Future's
   * {@code get} method will return the given result upon successful completion.
   *
   * @param task the task to submit
   * @param result the result to return
   * @param <T> the type of the result
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if the task is null
   */
  public <T> Future<T> submit(Runnable task, T result) {
    return executor.submit(task, result);
  }

  /**
   * Submits a Runnable task for execution and returns a Future representing that task. The Future's
   * {@code get} method will return {@code null} upon <em>successful</em> completion.
   *
   * @param task the task to submit
   * @return a Future representing pending completion of the task
   * @throws RejectedExecutionException if the task cannot be scheduled for execution
   * @throws NullPointerException if the task is null
   */
  public Future<?> submit(Runnable task) {
    return executor.submit(task);
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed by a task running in the
   * dedicated executor after it runs the given action.
   *
   * @param runnable the action to run before completing the returned CompletableFuture
   * @return the new CompletableFuture
   */
  public CompletableFuture<Void> runAsync(Runnable runnable) {
    return CompletableFuture.runAsync(runnable, executor);
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed by a task running in the
   * dedicated executor with the value obtained by calling the given Supplier.
   *
   * @param supplier a function returning the value to be used to complete the returned
   *        CompletableFuture
   * @param <U> the function's return type
   * @return the new CompletableFuture
   */
  public <U> CompletableFuture<U> supplyAsync(Supplier<U> supplier) {
    return CompletableFuture.supplyAsync(supplier, executor);
  }

  public static class Builder {

    protected boolean enableAwaitTermination;
    protected long awaitTerminationTimeout;
    protected TimeUnit awaitTerminationTimeUnit = TimeUnit.NANOSECONDS;
    protected boolean awaitTerminationBeforeShutdown = true;
    protected boolean useShutdown;
    protected boolean useShutdownNow = true;

    protected Builder() {
      // nothing
    }

    /**
     * Enables invocation of {@code awaitTermination} during {@code tearDown}. Default is disabled.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     */
    public Builder awaitTermination(long timeout, TimeUnit unit) {
      enableAwaitTermination = true;
      awaitTerminationTimeout = timeout;
      awaitTerminationTimeUnit = unit;
      return this;
    }

    /**
     * Enables invocation of {@code shutdown} during {@code tearDown}. Default is disabled.
     */
    public Builder useShutdown() {
      useShutdown = true;
      useShutdownNow = false;
      return this;
    }

    /**
     * Enables invocation of {@code shutdownNow} during {@code tearDown}. Default is enabled.
     *
     */
    public Builder useShutdownNow() {
      useShutdown = false;
      useShutdownNow = true;
      return this;
    }

    /**
     * Specifies invocation of {@code awaitTermination} before {@code shutdown} or
     * {@code shutdownNow}.
     */
    public Builder awaitTerminationBeforeShutdown() {
      awaitTerminationBeforeShutdown = true;
      return this;
    }

    /**
     * Specifies invocation of {@code awaitTermination} after {@code shutdown} or
     * {@code shutdownNow}.
     */
    public Builder awaitTerminationAfterShutdown() {
      awaitTerminationBeforeShutdown = false;
      return this;
    }

    /**
     * Builds the instance of {@code ExecutorServiceRule}.
     */
    public ExecutorServiceRule build() {
      return new ExecutorServiceRule(this);
    }
  }
}
