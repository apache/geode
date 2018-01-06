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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.extractProperty;

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
 * creates an {@code ExecutorService} which is terminated after the scope of the {@code Rule}.
 *
 * <p>
 * By default, the {@code ExecutorService} is single-threaded. You can specify the thread count by
 * using {@link Builder#threadCount(int)}.
 *
 * <p>
 * The {@code Rule} can be configured to assert that all tasks completed before the scope of the
 * {@code Rule} by specifying {@link Builder#assertTasksAreDone()}.
 *
 * <p>
 * The {@code Rule} can be configured to await termination by specifying
 * {@link Builder#awaitTermination(long, TimeUnit)}. If all tasks have not terminated by the
 * specified timeout, then {@code TimeoutException} will be thrown. This has the potential to
 * obscure any {@code Throwable}s thrown by the test itself.
 *
 * <p>
 * This can be used in tests to prove existence of and fix for hangs, deadlocks, and infinite loops.
 *
 * <p>
 * Example with default configuration (single-threaded and does not assert that tasks are done):
 *
 * <pre>
 * private CountDownLatch hangLatch = new CountDownLatch(1);
 *
 * {@literal @}Rule
 * public AsynchronousRule asynchronousRule = new AsynchronousRule();
 *
 * {@literal @}Test
 * public void doTest() throws Exception {
 *   Future<Void> result = asynchronousRule.runAsync(() -> {
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
 * Example with assertTasksAreDone enabled (asserts that no tasks are queued up, still waiting to be
 * processed after test completes). Note, this example fails:
 *
 * <pre>
 * private CountDownLatch hangLatch = new CountDownLatch(1);
 *
 * {@literal @}Rule
 * public AsynchronousRule asynchronousRule = AsynchronousRule.builder().assertTasksAreDone().build();
 *
 * {@literal @}Test
 * public void doTest() throws Exception {
 *   Future<Void> thread1 = asynchronousRule.runAsync(() -> {
 *     try {
 *       hangLatch.await();
 *     } catch (InterruptedException e) {
 *       throw new RuntimeException(e);
 *     }
 *   });
 *
 *   Future<Void> thread2 = asynchronousRule.runAsync(() -> {
 *     try {
 *       hangLatch.await();
 *     } catch (InterruptedException e) {
 *     throw new RuntimeException(e);
 *     }
 *   });
 *
 *   assertThatThrownBy(() -> thread1.get(1, MILLISECONDS)).isInstanceOf(TimeoutException.class);
 * }
 * </pre>
 *
 * <p>
 * Fix for the above example is to perform tearDown that terminates both tasks before the
 * {@code Rule} performs its own {@code after}:
 *
 * <pre>
 * private CountDownLatch hangLatch = new CountDownLatch(1);
 *
 * {@literal @}Rule
 * public AsynchronousRule asynchronousRule = AsynchronousRule.builder().assertTasksAreDone().build();
 *
 * {@literal @}After
 * public void tearDown() throws Exception {
 *   hangLatch.countDown();
 * }
 *
 * {@literal @}Test
 * public void doTest() throws Exception {
 *   Future<Void> thread1 = asynchronousRule.runAsync(() -> {
 *     try {
 *       hangLatch.await();
 *     } catch (InterruptedException e) {
 *       throw new RuntimeException(e);
 *     }
 *   });
 *
 *   Future<Void> thread2 = asynchronousRule.runAsync(() -> {
 *     try {
 *       hangLatch.await();
 *     } catch (InterruptedException e) {
 *     throw new RuntimeException(e);
 *     }
 *   });
 *
 *   assertThatThrownBy(() -> thread1.get(1, MILLISECONDS)).isInstanceOf(TimeoutException.class);
 * }
 * </pre>
 *
 * <p>
 * Example with awaitTermination enabled (awaits up to timeout for all submitted tasks to terminate
 * after being interrupted). This causes the {@code Rule} to await termination of all threads during
 * its tear down. This would only be interesting if you're trying to discover tasks that might not
 * be responsive to interrupts such as blocking IO reads on Windows:
 *
 * <pre>
 * private CountDownLatch hangLatch = new CountDownLatch(1);
 *
 * {@literal @}Rule
 * public AsynchronousRule asynchronousRule = AsynchronousRule.builder().threadCount(10).awaitTermination(10, MILLISECONDS).build();
 *
 * {@literal @}Test
 * public void doTest() throws Exception {
 *   for (int i = 0; i < 10; i++) {
 *     asynchronousRule.runAsync(() -> {
 *       try {
 *         hangLatch.await();
 *       } catch (InterruptedException e) {
 *         // do nothing
 *       }
 *     });
 *   }
 * }
 * </pre>
 */
@SuppressWarnings("unused")
public class ExecutorServiceRule extends SerializableExternalResource {

  protected final int threadCount;
  protected final boolean assertTasksAreDone;
  protected final boolean awaitTermination;
  protected final long awaitTerminationTimeout;
  protected final TimeUnit awaitTerminationTimeUnit;

  protected transient volatile ExecutorService executor;

  public static Builder builder() {
    return new Builder();
  }

  protected ExecutorServiceRule(Builder builder) {
    this.threadCount = builder.threadCount;
    this.assertTasksAreDone = builder.assertTasksAreDone;
    this.awaitTermination = builder.awaitTermination;
    this.awaitTerminationTimeout = builder.awaitTerminationTimeout;
    this.awaitTerminationTimeUnit = builder.awaitTerminationTimeUnit;
  }

  public ExecutorServiceRule() {
    this.threadCount = 1;
    this.assertTasksAreDone = false;
    this.awaitTermination = false;
    this.awaitTerminationTimeout = 0;
    this.awaitTerminationTimeUnit = TimeUnit.NANOSECONDS;
  }

  @Override
  public void before() {
    if (threadCount > 1) {
      executor = Executors.newFixedThreadPool(threadCount);
    } else {
      executor = Executors.newSingleThreadExecutor();
    }
  }

  @Override
  public void after() {
    if (assertTasksAreDone) {
      assertThat(executor.shutdownNow()).isEmpty();

    } else {
      executor.shutdownNow();
    }
    if (awaitTermination) {
      try {
        executor.awaitTermination(awaitTerminationTimeout, awaitTerminationTimeUnit);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

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

    protected int threadCount = 1;
    protected boolean assertTasksAreDone = false;
    protected boolean awaitTermination = false;
    protected long awaitTerminationTimeout = 0;
    protected TimeUnit awaitTerminationTimeUnit = TimeUnit.NANOSECONDS;

    protected Builder() {
      // nothing
    }

    /**
     * Configures the number of threads. Default is one thread.
     *
     * @param threadCount the number of threads in the pool
     */
    public Builder threadCount(int threadCount) {
      this.threadCount = threadCount;
      return this;
    }

    /**
     * Enables assertion that all tasks are done. Default is false.
     */
    public Builder assertTasksAreDone() {
      this.assertTasksAreDone = true;
      return this;
    }

    /**
     * Enables awaiting termination of all tasks. Default is disabled.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     */
    public Builder awaitTermination(long timeout, TimeUnit unit) {
      this.awaitTermination = true;
      this.awaitTerminationTimeout = timeout;
      this.awaitTerminationTimeUnit = unit;
      return this;
    }

    public ExecutorServiceRule build() {
      assertThat(threadCount).isGreaterThan(0);
      return new ExecutorServiceRule(this);
    }
  }
}
