/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.test.dunit;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.gemstone.gemfire.SystemFailure;

/**
 * An {@code AsyncInvocation} represents the invocation of a remote invocation
 * that executes asynchronously from its caller.  An instance of
 * {@code AsyncInvocation} provides information about the invocation such as
 * any exception that it may have thrown.
 *
 * <p>{@code AsyncInvocation} can be used as follows:
 *
 * <pre>
 *   AsyncInvocation ai1 = vm.invokeAsync(() -> Test.method1());
 *   AsyncInvocation ai2 = vm.invokeAsync(() -> Test.method2());
 *
 *   ai1.join();
 *   ai2.join();
 *
 *   assertTrue("Exception occurred while invoking " + ai1,
 *              !ai1.exceptionOccurred());
 *   if (ai2.exceptionOccurred()) {
 *     throw ai2.getException();
 *   }
 * </pre>
 *
 * @see VM#invokeAsync(Class, String)
 */
public class AsyncInvocation<T> {
  // TODO: davidw Add the ability to get a return value back from the
  // async method call.  (Use a static ThreadLocal field that is
  // accessible from the Runnable used in VM#invoke)
  // TODO: reimplement using Futures

  private static final long DEFAULT_JOIN_MILLIS = 60 * 1000;

  private final Thread thread;

  private final AtomicReference<T> resultValue = new AtomicReference<>();

  /** An exception thrown while this {@code AsyncInvocation} ran */
  private final AtomicReference<Throwable> resultThrowable = new AtomicReference<>();

  /** The object (or class) that is the target of this {@code AsyncInvocation} */
  private Object target;

  /** The name of the method being invoked */
  private String methodName;
  
  /**
   * Creates a new {@code AsyncInvocation}.
   *
   * @param  target
   *         The object or {@link Class} on which the remote method was
   *         invoked
   * @param  methodName
   *         The name of the method being invoked
   * @param  work
   *         The actual invocation of the method
   */
  public AsyncInvocation(final Object target, final String methodName, final Callable<T> work) {
    this.target = target;
    this.methodName = methodName;
    this.thread = new Thread(new AsyncInvocationGroup(), runnable(work), getName(target, methodName));
  }

  /**
   * Returns the target of this async method invocation.
   */
  public Object getTarget() {
    return this.target;
  }

  /**
   * Returns the name of the method being invoked remotely.
   */
  public String getMethodName() {
    return this.methodName;
  }

  /**
   * Returns whether or not an exception occurred during this async
   * method invocation.
   *
   * @throws AssertionError if this {@code AsyncInvocation} is not done.
   */
  public boolean exceptionOccurred() {
    return getException() != null;
  }

  /**
   * Returns the exception that was thrown during this async method
   * invocation.
   *
   * @throws AssertionError if this {@code AsyncInvocation} is not done.
   */
  public Throwable getException() {
    checkIsDone("Exception status not available while thread is alive.");

    if (this.resultThrowable.get() instanceof RMIException) {
      return this.resultThrowable.get().getCause();

    } else {
      return this.resultThrowable.get();
    }
  }

  /**
   * Throws {@code AssertionError} wrapping any {@code Exception} thrown by
   * this {@code AsyncInvocation}.
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this
   *         {@code AsyncInvocation}.
   */
  public AsyncInvocation<T> checkException() {
    if (exceptionOccurred()) {
      throw new AssertionError("An exception occurred during asynchronous invocation.", getException());
    }
    return this;
  }

  /**
   * Returns the result of this {@code AsyncInvocation}.
   *
   * @return the result of this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this
   *         {@code AsyncInvocation}.
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this
   *         {@code AsyncInvocation} fails to complete within the default
   *         timeout of 60 seconds as defined by {@link #DEFAULT_JOIN_MILLIS}.
   *
   * @throws InterruptedException if the current thread is interrupted.
   */
  public T getResult() throws InterruptedException {
    join();
    checkException();
    return getReturnValue();
  }

  /**
   * Returns the result of this {@code AsyncInvocation}.
   *
   * @param  millis
   *         the time to wait in milliseconds
   *
   * @return the result of this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping any {@code Exception} thrown by this
   *         {@code AsyncInvocation}.
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this
   *         {@code AsyncInvocation} fails to complete within the specified
   *         timeout of {@code millis}.
   *
   * @throws InterruptedException if the current thread is interrupted.
   */
  public T getResult(final long millis) throws InterruptedException {
    join(millis);
    timeoutIfAlive(millis);
    checkException();
    return getReturnValue();
  }

  /**
   * Returns the result of this {@code AsyncInvocation}.
   *
   * @return the result of this {@code AsyncInvocation}
   *
   * @throws AssertionError if this {@code AsyncInvocation} is not done.
   *
   * @deprecated Please use {@link #getResult()} instead.
   */
  public T getReturnValue() {
    checkIsDone("Return value not available while thread is alive.");
    return this.resultValue.get();
  }

  /**
   * Waits at most {@code millis} milliseconds for this
   * {@code AsyncInvocation} to complete. A timeout of {@code 0} means to wait
   * forever.
   *
   * @param  millis
   *         the time to wait in milliseconds
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this
   *         {@code AsyncInvocation} fails to complete within the specified
   *         timeout of {@code millis}.
   *
   * @throws IllegalArgumentException if the value of {@code millis} is
   *         negative.
   *
   * @throws InterruptedException if the current thread is interrupted.
   */
  public synchronized AsyncInvocation<T> join(final long millis) throws InterruptedException {
    this.thread.join(millis);
    timeoutIfAlive(millis);
    return this;
  }

  /**
   * Waits at most {@code millis} milliseconds plus {@code nanos} nanoseconds
   * for this {@code AsyncInvocation} to complete.
   *
   * @param  millis
   *         the time to wait in milliseconds
   * @param  nanos
   *         {@code 0-999999} additional nanoseconds to wait
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this
   *         {@code AsyncInvocation} fails to complete within the specified
   *         timeout of {@code millis}.
   *
   * @throws IllegalArgumentException
   *         if the value of {@code millis} is negative, or the value
   *         of {@code nanos} is not in the range {@code 0-999999}.
   *
   * @throws InterruptedException if the current thread is interrupted.
   */
  public synchronized AsyncInvocation<T> join(final long millis, final int nanos) throws InterruptedException {
    this.thread.join(millis, nanos);
    timeoutIfAlive(millis);
    return this;
  }

  /**
   * Waits for this thread to die up to a default of 60 seconds as defined by
   * {@link #DEFAULT_JOIN_MILLIS}.
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this
   *         {@code AsyncInvocation} fails to complete within the default
   *         timeout of 60 seconds as defined by {@link #DEFAULT_JOIN_MILLIS}.
   *
   * @throws InterruptedException if the current thread is interrupted.
   */
  public AsyncInvocation<T> join() throws InterruptedException {
    // do NOT invoke Thread#join() without a timeout
    join(DEFAULT_JOIN_MILLIS);
    timeoutIfAlive(DEFAULT_JOIN_MILLIS);
    return this;
  }

  /**
   * Start this {@code AsyncInvocation}.
   *
   * @return this {@code AsyncInvocation}
   */
  public synchronized AsyncInvocation<T> start() {
    this.thread.start();
    return this;
  }

  /**
   * Return this {@code AsyncInvocation}'s work thread.
   *
   * @return this {@code AsyncInvocation}'s work thread.
   */
  public synchronized Thread getThread() {
    return this.thread;
  }

  /**
   * Tests if this {@code AsyncInvocation}'s thread is alive. A thread is alive
   * if it has been started and has not yet died.
   *
   * @return {@code true} if this {@code AsyncInvocation}'s thread is alive;
   *         {@code false} otherwise.
   */
  public synchronized boolean isAlive() {
    return this.thread.isAlive();
  }

  /**
   * Returns the identifier of this {@code AsyncInvocation}'s thread. The
   * thread ID is a positive <tt>long</tt> number generated when this thread
   * was created. The thread ID is unique and remains unchanged during its
   * lifetime. When a thread is terminated, this thread ID may be reused.
   *
   * @return this {@code AsyncInvocation}'s thread's ID.
   */
  public long getId() {
    return this.thread.getId();
  }

  @Override
  public String toString() {
    return "AsyncInvocation{" + "target=" + target + ", methodName='" + methodName + '\'' + '}';
  }

  /**
   * Throws {@code AssertionError} if this {@code AsyncInvocation} is not done.
   *
   * @param  message
   *         The value to be used in constructing detail message
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws AssertionError if this {@code AsyncInvocation} is not done.
   */
  private AsyncInvocation<T> checkIsDone(final String message) {
    if (this.thread.isAlive()) {
      throw new AssertionError(message);
    }
    return this;
  }

  /**
   * Throws {@code AssertionError} wrapping a {@code TimeoutException} if this
   * {@code AsyncInvocation} fails to complete within the default timeout of 60
   * seconds as defined by {@link #DEFAULT_JOIN_MILLIS}.
   *
   * @return this {@code AsyncInvocation}
   *
   * @throws AssertionError wrapping a {@code TimeoutException} if this
   *         {@code AsyncInvocation} fails to complete within the default
   *         timeout of 60 seconds as defined by {@link #DEFAULT_JOIN_MILLIS}.
   */
  private AsyncInvocation<T> timeoutIfAlive(final long timeout) {
    if (this.thread.isAlive()) {
      throw new AssertionError(new TimeoutException("Timed out waiting " + timeout + " milliseconds for AsyncInvocation to complete."));
    }
    return this;
  }

  private Runnable runnable(final Callable<T> work) {
    return () -> {
        try {
          resultValue.set(work.call());
        } catch (Throwable throwable) {
          resultThrowable.set(throwable);
        }
    };
  }

  /**
   * Returns the name of a {@code AsyncInvocation} based on its
   * {@code targetObject} and {@code methodName}.
   */
  private static String getName(final Object target, final String methodName) {
    StringBuilder sb = new StringBuilder(methodName);
    sb.append(" invoked on ");
    if (target instanceof Class) {
      sb.append("class ");
      sb.append(((Class) target).getName());

    } else {
      sb.append("an instance of ");
      sb.append(target.getClass().getName());
    }

    return sb.toString();
  }

  /**
   * A {@code ThreadGroup} that notices when an exception occurs
   * during an {@code AsyncInvocation}.
   */
  private class AsyncInvocationGroup extends ThreadGroup {

    private AsyncInvocationGroup() {
      super("Async Invocations");
    }

    @Override
    public void uncaughtException(Thread thread, Throwable throwable) {
      if (throwable instanceof VirtualMachineError) {
        SystemFailure.setFailure((VirtualMachineError)throwable); // don't throw
      }
      resultThrowable.set(throwable);
    }
  }
}
