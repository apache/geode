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
package org.apache.geode.test.dunit;

import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.dumpThreads;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.geode.SystemFailure;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.internal.AsyncThreadId;
import org.apache.geode.test.dunit.internal.IdentifiableCallable;
import org.apache.geode.test.dunit.internal.IdentifiableRunnable;
import org.apache.geode.test.dunit.internal.StackTrace;

/**
 * An {@code AsyncInvocation} represents the invocation of a remote invocation that executes
 * asynchronously from its caller. An instance of {@code AsyncInvocation} provides information about
 * the invocation such as any exception that it may have thrown.
 *
 * <p>
 * {@code AsyncInvocation} can be used as follows:
 *
 * <pre>
 *   AsyncInvocation doPutsInVM0 = vm0.invokeAsync(() -> doPuts());
 *   AsyncInvocation doDestroysInVM1 = vm1.invokeAsync(() -> doDestroys());
 *
 *   doPutsInVM0.await();
 *   doDestroysInVM1.await();
 * </pre>
 *
 * @param <V> The result type returned by this AsyncInvocation's {@code get} methods
 *
 * @see VM#invokeAsync(SerializableCallableIF)
 */
public class AsyncInvocation<V> implements Future<V> {

  private static final long DEFAULT_TIMEOUT_MILLIS = getTimeout().toMillis();

  private final Thread thread;

  private final AtomicReference<V> resultValue = new AtomicReference<>();

  /** An exception thrown while this {@code AsyncInvocation} ran */
  private final AtomicReference<Throwable> resultThrowable = new AtomicReference<>();

  /** The object (or class) that is the target of this {@code AsyncInvocation} */
  private final Object target;

  /** The name of the method being invoked */
  private final String methodName;

  private final Function<TimeoutException, TimeoutException> asyncTimeoutHandler;

  /** True if this {@code AsyncInvocation} has been cancelled */
  private boolean cancelled;

  public static <V> AsyncInvocation<V> create(IdentifiableRunnable target, Callable<V> work,
      VM vm) {
    return new AsyncInvocation<>(target, target.getMethodName(), work,
        asyncTimeoutHandler(vm, target.getId()));
  }

  public static <V> AsyncInvocation<V> create(IdentifiableCallable<V> target, Callable<V> work,
      VM vm) {
    return new AsyncInvocation<>(target, target.getMethodName(), work,
        asyncTimeoutHandler(vm, target.getId()));
  }

  public static <V> AsyncInvocation<V> create(Object target, String methodName, Callable<V> work,
      VM vm) {
    return new AsyncInvocation<>(target, methodName, work, timeoutException -> timeoutException);
  }

  /**
   * Creates a new {@code AsyncInvocation}.
   *
   * @param target The object or {@link Class} on which the remote method was invoked
   * @param methodName The name of the method being invoked
   * @param work The actual invocation of the method
   */
  private AsyncInvocation(Object target, String methodName, Callable<V> work,
      Function<TimeoutException, TimeoutException> asyncTimeoutHandler) {
    this.target = target;
    this.methodName = methodName;
    thread = new Thread(new AsyncInvocationGroup(), answer(work), getName(target, methodName));
    this.asyncTimeoutHandler = asyncTimeoutHandler;
  }

  /**
   * Start this {@code AsyncInvocation}.
   *
   * @return this {@code AsyncInvocation}
   */
  public synchronized AsyncInvocation<V> start() {
    thread.start();
    return this;
  }

  /**
   * Return this {@code AsyncInvocation}'s work thread.
   *
   * @return this {@code AsyncInvocation}'s work thread.
   */
  public synchronized Thread getThread() {
    return thread;
  }

  /**
   * Tests if this {@code AsyncInvocation}'s thread is alive. A thread is alive if it has been
   * started and has not yet died.
   *
   * @return {@code true} if this {@code AsyncInvocation}'s thread is alive; {@code false}
   *         otherwise.
   */
  public synchronized boolean isAlive() {
    return thread.isAlive();
  }

  @Override
  public synchronized boolean isCancelled() {
    return cancelled;
  }

  @Override
  public synchronized boolean isDone() {
    return !thread.isAlive();
  }

  @Override
  public synchronized boolean cancel(final boolean mayInterruptIfRunning) {
    if (thread.isAlive()) {
      if (mayInterruptIfRunning) {
        cancelled = true;
        thread.interrupt();
        return true;
      }
    }
    return false;
  }

  /**
   * Waits at most {@link GeodeAwaitility#getTimeout()} for the computation to complete.
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this {@code AsyncInvocation}.
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this {@code AsyncInvocation}
   *         fails to complete within the default timeout of 60 seconds as defined by
   *         {@link #DEFAULT_TIMEOUT_MILLIS}.
   *
   * @throws CancellationException if the computation was cancelled
   *
   * @throws InterruptedException if the current thread is interrupted.
   */
  public AsyncInvocation<V> await() throws InterruptedException {
    try {
      return await(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeoutException) {
      throw new AssertionError(timeoutException);
    }
  }

  /**
   * Waits at most {@link GeodeAwaitility#getTimeout()} for the work to complete, and then returns
   * the result of this {@code AsyncInvocation}.
   *
   * @return the result of this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this {@code AsyncInvocation}.
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this {@code AsyncInvocation}
   *         fails to complete within the default timeout of 60 seconds as defined by
   *         {@link #DEFAULT_TIMEOUT_MILLIS}.
   *
   * @throws CancellationException if the computation was cancelled
   *
   * @throws InterruptedException if the current thread is interrupted.
   */
  @Override
  public V get() throws InterruptedException {
    try {
      return get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeoutException) {
      throw new AssertionError(timeoutException);
    }
  }

  /**
   * Returns the identifier of this {@code AsyncInvocation}'s thread. The thread ID is a positive
   * <tt>long</tt> number generated when this thread was created. The thread ID is unique and
   * remains unchanged during its lifetime. When a thread is terminated, this thread ID may be
   * reused.
   *
   * @return this {@code AsyncInvocation}'s thread's ID.
   */
  public long getId() {
    return thread.getId();
  }

  @Override
  public String toString() {
    return "AsyncInvocation{" + "target=" + target + ", methodName='" + methodName + '\'' + '}';
  }

  /**
   * Returns the target of this async method invocation.
   *
   * @deprecated This method is not required for anything.
   */
  @Deprecated
  public Object getTarget() {
    return target;
  }

  /**
   * Returns the name of the method being invoked remotely.
   *
   * @deprecated This method is not required for anything.
   */
  @Deprecated
  public String getMethodName() {
    return methodName;
  }

  /**
   * Returns whether or not an exception occurred during this async method invocation.
   *
   * @throws AssertionError if this {@code AsyncInvocation} is not done.
   *
   * @deprecated Please use {@link #await()} or {@link #get()} which will throw any underlying
   *             exception.
   */
  @Deprecated
  public boolean exceptionOccurred() {
    return getException() != null;
  }

  /**
   * Returns the exception that was thrown during this async method invocation.
   *
   * @throws AssertionError if this {@code AsyncInvocation} is not done.
   *
   * @deprecated Please use {@link #await()} or {@link #get()} which will throw any underlying
   *             exception.
   */
  @Deprecated
  public Throwable getException() {
    try {
      checkIsDone("Exception status not available while thread is alive.");
    } catch (IllegalStateException illegalStateException) {
      throw new AssertionError(illegalStateException);
    }

    if (resultThrowable.get() instanceof RMIException) {
      return resultThrowable.get().getCause();
    }

    return resultThrowable.get();
  }

  /**
   * Throws {@code AssertionError} wrapping any {@code Exception} thrown by this
   * {@code AsyncInvocation}.
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this {@code AsyncInvocation}.
   *
   * @deprecated Please use {@link #await()} or {@link #get()} which will throw any underlying
   *             exception.
   */
  @Deprecated
  public AsyncInvocation<V> checkException() {
    if (resultThrowable.get() != null) {
      throw new AssertionError("An exception occurred during asynchronous invocation.",
          getException());
    }
    return this;
  }

  /**
   * Returns the result of this {@code AsyncInvocation}.
   *
   * @return the result of this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this {@code AsyncInvocation}.
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this {@code AsyncInvocation}
   *         fails to complete within the default timeout of 60 seconds as defined by
   *         {@link #DEFAULT_TIMEOUT_MILLIS}.
   *
   * @throws InterruptedException if the current thread is interrupted.
   *
   * @deprecated Please use {@link #get()} instead.
   */
  @Deprecated
  public V getResult() throws InterruptedException {
    join();
    checkException();
    checkIsDone("Return value not available while thread is alive.");
    return resultValue.get();
  }

  /**
   * Returns the result of this {@code AsyncInvocation}.
   *
   * @param millis the time to wait in milliseconds
   *
   * @return the result of this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this {@code AsyncInvocation}.
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this {@code AsyncInvocation}
   *         fails to complete within the specified timeout of {@code millis}.
   *
   * @throws InterruptedException if the current thread is interrupted.
   *
   * @deprecated Please use {@link #get()} instead.
   */
  @Deprecated
  public V getResult(final long millis) throws InterruptedException {
    try {
      return get(millis, TimeUnit.MILLISECONDS);
    } catch (TimeoutException timeoutException) {
      throw new AssertionError(timeoutException);
    }
  }

  /**
   * Returns the result of this {@code AsyncInvocation}.
   *
   * @return the result of this {@code AsyncInvocation}
   *
   * @throws AssertionError if this {@code AsyncInvocation} is not done.
   *
   * @deprecated Please use {@link #get()} instead.
   */
  @Deprecated
  public V getReturnValue() {
    checkIsDone("Return value not available while thread is alive.");
    return resultValue.get();
  }

  /**
   * Waits at most {@code millis} milliseconds for this {@code AsyncInvocation} to complete. A
   * timeout of {@code 0} means to wait forever.
   *
   * @param millis the time to wait in milliseconds
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws IllegalArgumentException if the value of {@code millis} is negative.
   *
   * @throws InterruptedException if the current thread is interrupted.
   *
   * @deprecated Please use {@link #await()} or {@link #get()} instead.
   */
  @Deprecated
  public synchronized AsyncInvocation<V> join(final long millis) throws InterruptedException {
    thread.join(millis);
    return this;
  }

  /**
   * Waits at most {@code millis} milliseconds plus {@code nanos} nanoseconds for this
   * {@code AsyncInvocation} to complete.
   *
   * @param millis the time to wait in milliseconds
   * @param nanos {@code 0-999999} additional nanoseconds to wait
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws IllegalArgumentException if the value of {@code millis} is negative, or the value of
   *         {@code nanos} is not in the range {@code 0-999999}.
   *
   * @throws InterruptedException if the current thread is interrupted.
   *
   * @deprecated Please use {@link #await()} or {@link #get()} instead.
   */
  @Deprecated
  public synchronized AsyncInvocation<V> join(final long millis, final int nanos)
      throws InterruptedException {
    thread.join(millis, nanos);
    return this;
  }

  /**
   * Waits at most {@link GeodeAwaitility#getTimeout()} for this thread to die.
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws InterruptedException if the current thread is interrupted.
   *
   * @deprecated Please use {@link #await()} or {@link #get()} instead.
   */
  @Deprecated
  public AsyncInvocation<V> join() throws InterruptedException {
    // do NOT invoke Thread#join() without a timeout
    join(DEFAULT_TIMEOUT_MILLIS);
    return this;
  }

  /**
   * Waits if necessary for at most the given time for the computation to complete.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this {@code AsyncInvocation}.
   *
   * @throws CancellationException if the computation was cancelled
   *
   * @throws InterruptedException if the current thread is interrupted.
   *
   * @throws TimeoutException if the wait timed out
   *
   * @deprecated Please use {@link #await()} which uses {@link GeodeAwaitility#getTimeout()}.
   */
  @Deprecated
  public AsyncInvocation<V> await(final long timeout, final TimeUnit unit)
      throws InterruptedException, TimeoutException {
    long millis = unit.toMillis(timeout);
    join(millis);
    timeoutIfAlive(millis);
    checkException();
    return this;
  }

  /**
   * Waits at most the given time for the computation to complete, and then retrieves its result,
   * if available.
   *
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   *
   * @return the result of this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this {@code AsyncInvocation}.
   *
   * @throws CancellationException if the computation was cancelled
   *
   * @throws InterruptedException if the current thread is interrupted.
   *
   * @throws TimeoutException if the wait timed out
   *
   * @deprecated Please use {@link #get()} which uses {@link GeodeAwaitility#getTimeout()}.
   */
  @Deprecated
  @Override
  public V get(final long timeout, final TimeUnit unit)
      throws InterruptedException, TimeoutException {
    long millis = unit.toMillis(timeout);
    join(millis);
    timeoutIfAlive(millis);
    checkException();
    return resultValue.get();
  }

  /**
   * Throws {@code IllegalStateException} if this {@code AsyncInvocation} is not done.
   *
   * @param message The value to be used in constructing detail message
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws IllegalStateException if this {@code AsyncInvocation} is not done.
   */
  private AsyncInvocation<V> checkIsDone(final String message) {
    if (thread.isAlive()) {
      throw new IllegalStateException(message);
    }
    return this;
  }

  /**
   * Throws {@code AssertionError} wrapping a {@code TimeoutException} if this
   * {@code AsyncInvocation} fails to complete within {@link GeodeAwaitility#getTimeout()}.
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws TimeoutException if this {@code AsyncInvocation} fails to complete within
   *         {@link GeodeAwaitility#getTimeout()}.
   */
  private AsyncInvocation<V> timeoutIfAlive(final long timeout) throws TimeoutException {
    if (thread.isAlive()) {
      TimeoutException timeoutException = new TimeoutException(
          "Timed out waiting " + timeout + " milliseconds for AsyncInvocation to complete.");
      throw asyncTimeoutHandler.apply(timeoutException);
    }
    return this;
  }

  private Runnable answer(final Callable<V> work) {
    return () -> {
      try {
        resultValue.set(work.call());
      } catch (Throwable throwable) {
        resultThrowable.set(throwable);
      }
    };
  }

  /**
   * Returns the name of a {@code AsyncInvocation} based on its {@code targetObject} and
   * {@code methodName}.
   */
  private static String getName(final Object target, final String methodName) {
    StringBuilder sb = new StringBuilder(methodName).append(" invoked on ");
    if (target instanceof Class) {
      sb.append("class ").append(((Class) target).getName());
    } else {
      sb.append("an instance of ").append(target.getClass().getName());
    }
    return sb.toString();
  }

  private static Function<TimeoutException, TimeoutException> asyncTimeoutHandler(VM vm, long id) {
    return timeoutException -> {
      StackTrace stackTrace =
          vm.invoke(() -> {
            System.out.println(dumpThreads());
            return new StackTrace(vm.getId(), AsyncThreadId.get(id));
          });
      timeoutException.initCause(stackTrace);
      return timeoutException;
    };
  }

  /**
   * A {@code ThreadGroup} that notices when an exception occurs during an {@code AsyncInvocation}.
   */
  private class AsyncInvocationGroup extends ThreadGroup {

    private AsyncInvocationGroup() {
      super("Async Invocations");
    }

    @Override
    public void uncaughtException(Thread thread, Throwable throwable) {
      if (throwable instanceof VirtualMachineError) {
        SystemFailure.setFailure((Error) throwable);
      }
      resultThrowable.set(throwable);
    }
  }
}
