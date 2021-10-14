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
package org.apache.geode.test.dunit.rules;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule.ThrowingRunnable;

@SuppressWarnings("unused")
public class DistributedExecutorServiceRule extends AbstractDistributedRule {

  private static final AtomicReference<ExecutorServiceRule> delegate = new AtomicReference<>();

  public DistributedExecutorServiceRule() {
    // default vmCount
  }

  public DistributedExecutorServiceRule(int vmCount) {
    super(vmCount);
  }

  public ExecutorService getExecutorService() {
    return delegate.get().getExecutorService();
  }

  /**
   * Executes the given command at some time in the future.
   *
   * @param command the runnable task
   * @throws RejectedExecutionException if this task cannot be accepted for execution
   * @throws NullPointerException if command is null
   */
  public void execute(ThrowingRunnable command) {
    delegate.get().execute(command);
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
    return delegate.get().submit(task);
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
    return delegate.get().submit(task, result);
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
    return delegate.get().submit(task);
  }

  /**
   * Returns a new CompletableFuture that is asynchronously completed by a task running in the
   * dedicated executor after it runs the given action.
   *
   * @param runnable the action to run before completing the returned CompletableFuture
   * @return the new CompletableFuture
   */
  public CompletableFuture<Void> runAsync(Runnable runnable) {
    return delegate.get().runAsync(runnable);
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
    return delegate.get().supplyAsync(supplier);
  }

  /**
   * Returns the {@code Thread}s that are directly in the {@code ExecutorService}'s
   * {@code ThreadGroup} excluding subgroups.
   */
  public Set<Thread> getThreads() {
    return delegate.get().getThreads();
  }

  /**
   * Returns an array of {@code Thread Ids} that are directly in the {@code ExecutorService}'s
   * {@code ThreadGroup} excluding subgroups. {@code long[]} is returned to facilitate using JDK
   * APIs such as {@code ThreadMXBean#getThreadInfo(long[], int)}.
   */
  public long[] getThreadIds() {
    return delegate.get().getThreadIds();
  }

  /**
   * Returns thread dumps for the {@code Thread}s that are in the {@code ExecutorService}'s
   * {@code ThreadGroup} excluding subgroups.
   */
  public String dumpThreads() {
    return delegate.get().dumpThreads();
  }

  @Override
  public void before() throws Exception {
    invoker().invokeInEveryVMAndController(() -> invokeBefore());
  }

  @Override
  public void after() {
    invoker().invokeInEveryVMAndController(() -> invokeAfter());
  }

  @Override
  protected void afterCreateVM(VM vm) {
    vm.invoke(() -> invokeBefore());
  }

  @Override
  protected void afterBounceVM(VM vm) {
    vm.invoke(() -> invokeBefore());
  }

  private void invokeBefore() throws Exception {
    try {
      delegate.set(new ExecutorServiceRule());
      delegate.get().before();
    } catch (Throwable throwable) {
      if (throwable instanceof Exception) {
        throw (Exception) throwable;
      }
      throw new RuntimeException(throwable);
    }
  }

  private void invokeAfter() {
    delegate.get().after();
  }
}
