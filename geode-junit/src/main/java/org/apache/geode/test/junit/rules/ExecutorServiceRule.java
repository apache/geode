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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
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
 *   Future&lt;Void&gt; result = executorServiceRule.runAsync(() -> {
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
  protected final int threadCount;

  protected transient volatile DedicatedThreadFactory threadFactory;
  protected transient volatile ExecutorService executor;

  public static BiFunction<Integer, Integer, String> DEFAULT_THREAD_NAME_FUNCTION =
      (poolNumber, threadNumber) -> String.format("pool-%d-thread-%d", poolNumber, threadNumber);
  private transient BiFunction<Integer, Integer, String> threadNameFunction =
      DEFAULT_THREAD_NAME_FUNCTION;

  /**
   * Returns a {@code Builder} to configure a new {@code ExecutorServiceRule}.
   */
  public static Builder builder() {
    return new Builder();
  }

  protected ExecutorServiceRule(Builder builder) {
    this(builder.enableAwaitTermination,
        builder.awaitTerminationTimeout,
        builder.awaitTerminationTimeUnit,
        builder.awaitTerminationBeforeShutdown,
        builder.useShutdown,
        builder.useShutdownNow,
        builder.threadCount);
  }

  /**
   * Constructs a {@code ExecutorServiceRule} which invokes {@code ExecutorService.shutdownNow()}
   * during {@code tearDown}.
   */
  public ExecutorServiceRule() {
    this(false, 0, TimeUnit.NANOSECONDS, false, false, true, 0);
  }

  /**
   * Constructs a {@code ExecutorServiceRule} which invokes {@code ExecutorService.shutdownNow()}
   * during {@code tearDown}.
   *
   * @param threadCount The number of threads in the pool. Creates fixed thread pool if > 0; else
   *        creates cached thread pool.
   */
  public ExecutorServiceRule(int threadCount) {
    this(false, 0, TimeUnit.NANOSECONDS, false, false, true, threadCount);
  }

  /**
   * For invocation by {@code DistributedExecutorServiceRule} which needs to subclass another class.
   */
  public ExecutorServiceRule(boolean enableAwaitTermination,
      long awaitTerminationTimeout,
      TimeUnit awaitTerminationTimeUnit,
      boolean awaitTerminationBeforeShutdown,
      boolean useShutdown,
      boolean useShutdownNow,
      int threadCount) {
    this.enableAwaitTermination = enableAwaitTermination;
    this.awaitTerminationTimeout = awaitTerminationTimeout;
    this.awaitTerminationTimeUnit = awaitTerminationTimeUnit;
    this.awaitTerminationBeforeShutdown = awaitTerminationBeforeShutdown;
    this.useShutdown = useShutdown;
    this.useShutdownNow = useShutdownNow;
    this.threadCount = threadCount;
  }

  @Override
  public void before() {
    threadFactory = new DedicatedThreadFactory(threadNameFunction);
    if (threadCount > 0) {
      executor = Executors.newFixedThreadPool(threadCount, threadFactory);
    } else {
      executor = Executors.newCachedThreadPool(threadFactory);
    }
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

  /**
   * This method can be used to customize the names of the threads created by the Executor.
   *
   * @param fn The function used to produce the thread name. The function receives the pool number
   *        and thread number as arguments. By default, these values are applied to the format
   *        string "pool-%d-thread-%d" respectively.
   * @return the name of the thread to be created
   * @see #DEFAULT_THREAD_NAME_FUNCTION
   */
  public ExecutorServiceRule withThreadNameFunction(BiFunction<Integer, Integer, String> fn) {
    threadNameFunction = fn;
    return this;
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
  public void execute(ThrowingRunnable command) {
    executor.submit((Callable<Void>) () -> {
      command.run();
      return null;
    });
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
  public <T> Future<T> submit(ThrowingRunnable task, T result) {
    FutureTask<T> futureTask = new FutureTask<>(() -> {
      task.run();
      return result;
    });
    executor.submit(futureTask);
    return futureTask;
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
  public Future<Void> submit(ThrowingRunnable task) {
    FutureTask<Void> futureTask = new FutureTask<>(() -> {
      task.run();
      return null;
    });
    executor.submit(futureTask);
    return futureTask;
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

  /**
   * Returns the {@code Thread}s that are directly in the {@code ExecutorService}'s
   * {@code ThreadGroup} excluding subgroups.
   */
  public Set<Thread> getThreads() {
    return threadFactory.getThreads();
  }

  /**
   * Returns an array of {@code Thread Ids} that are directly in the {@code ExecutorService}'s
   * {@code ThreadGroup} excluding subgroups. {@code long[]} is returned to facilitate using JDK
   * APIs such as {@code ThreadMXBean#getThreadInfo(long[], int)}.
   */
  public long[] getThreadIds() {
    Set<Thread> threads = getThreads();
    long[] threadIds = new long[threads.size()];

    int i = 0;
    for (Thread thread : threads) {
      threadIds[i++] = thread.getId();
    }

    return threadIds;
  }

  /**
   * Returns thread dumps for the {@code Thread}s that are in the {@code ExecutorService}'s
   * {@code ThreadGroup} excluding subgroups.
   */
  public String dumpThreads() {
    StringBuilder dumpWriter = new StringBuilder();

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(getThreadIds(), true, true);

    for (ThreadInfo threadInfo : threadInfos) {
      if (threadInfo == null) {
        // sometimes ThreadMXBean.getThreadInfo returns array with one or more null elements
        continue;
      }
      // ThreadInfo toString includes monitor and synchronizer details
      dumpWriter.append(threadInfo);
    }

    return dumpWriter.toString();
  }

  /**
   * This interface replaces {@link Runnable} in cases when execution of {@link #run()} method may
   * throw exception.
   *
   * <p>
   * Useful for capturing lambdas that throw exceptions.
   */
  @FunctionalInterface
  public interface ThrowingRunnable {
    /**
     * @throws Exception The exception that may be thrown
     * @see Runnable#run()
     */
    void run() throws Exception;
  }

  /**
   * Modified version of {@code java.util.concurrent.Executors$DefaultThreadFactory} that uses
   * a {@code Set<WeakReference<Thread>>} to track the {@code Thread}s in the factory's
   * {@code ThreadGroup} excluding subgroups.
   */
  static class DedicatedThreadFactory implements ThreadFactory {

    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);

    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final Set<WeakReference<Thread>> directThreads = new HashSet<>();
    private final BiFunction<Integer, Integer, String> threadNameFunction;

    private DedicatedThreadFactory(BiFunction<Integer, Integer, String> threadNameFunction) {
      this.threadNameFunction = threadNameFunction;
      group = new ThreadGroup(ExecutorServiceRule.class.getSimpleName() + "-ThreadGroup");
      POOL_NUMBER.getAndIncrement();
    }

    @Override
    public Thread newThread(Runnable r) {
      String threadName =
          threadNameFunction.apply(POOL_NUMBER.get(), threadNumber.getAndIncrement());
      Thread t = new Thread(group, r, threadName, 0);
      if (t.isDaemon()) {
        t.setDaemon(false);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      directThreads.add(new WeakReference<>(t));
      return t;
    }

    protected Set<Thread> getThreads() {
      Set<Thread> value = new HashSet<>();
      for (WeakReference<Thread> reference : directThreads) {
        Thread thread = reference.get();
        if (thread != null) {
          value.add(thread);
        }
      }
      return value;
    }
  }

  public static class Builder {

    protected boolean enableAwaitTermination;
    protected long awaitTerminationTimeout;
    protected TimeUnit awaitTerminationTimeUnit = TimeUnit.NANOSECONDS;
    protected boolean awaitTerminationBeforeShutdown = true;
    protected boolean useShutdown;
    protected boolean useShutdownNow = true;
    protected int threadCount;

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
     * Specifies the number of threads in the pool. Creates fixed thread pool if > 0. Default is 0
     * which means (non-fixed) cached thread pool.
     *
     * @param threadCount the number of threads in the pool
     */
    public Builder threadCount(int threadCount) {
      this.threadCount = threadCount;
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
