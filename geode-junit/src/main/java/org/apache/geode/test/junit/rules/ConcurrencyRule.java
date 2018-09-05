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

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Stopwatch;
import org.junit.rules.ErrorCollector;

import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

/**
 * A rule for testing using multiple threads. This rule should not be used as a class rule. This
 * rule accepts threads to be run, runs them in series or parallel, and throws exceptions if any of
 * the threads threw an unexpected exception or returned an incorrect value.
 *
 * Basic Steps for Usage:
 * 1. Declare the rule as a test rule with the @Rule annotation
 * 2. Create a Callable, or create a Runnable and use toCallable(runnable) to convert it
 * 3. Add the Callable to the rule using add(callable)
 * 4. (Optional) Add expectations for the outcome of running the Callable (values or exceptions),
 * and/or any repetition of threads (for N iterations or for a duration)
 * 5. Run all submitted callables in series or in parallel
 * 6. Before re-executing within the same test, use clear() to empty the list of callables and
 * errors
 *
 * Example Usage:
 *
 * <pre>
 * <code>
 *
 * {@literal @}Rule
 * public ConcurrencyRule concurrencyRule = new ConcurrencyRule(); // step 1
 *
 * {@literal @}Test
 * public void testName() {
 *   Callable<String> c1 = () -> {
 *     return "some Value";
 *   }; // step 2
 *
 *   concurrencyRule.add(c1).expectValue("some Value").repeatForIterations(3); // steps 3&4
 *   concurrencyRule.executeInParallel(); // step 5
 *   concurrencyRule.clear(); // step 6
 *   // keep using the rule as above, or ConcurrencyRule.after() will be called for cleanup
 * }
 *
 * </code>
 * </pre>
 */
public class ConcurrencyRule extends SerializableExternalResource {

  private final ExecutorService threadPool = Executors.newCachedThreadPool();
  private final Collection<ConcurrentOperation> toInvoke;
  private final Collection<Future<Void>> futures;

  private ProtectedErrorCollector errorCollector;
  private Duration timeout;

  private final AtomicBoolean allThreadsExecuted = new AtomicBoolean(false);

  /**
   * A default constructor that sets the timeout to a default of 30 seconds
   */
  public ConcurrencyRule() {
    this(Duration.ofSeconds(300));
  }

  /**
   * A non-default constructor that sets the timeout to the given duration
   *
   * @param timeout the maximum duration that threads should execute, given that the submitted
   *        tasks respond to cancellation signals.
   */
  public ConcurrencyRule(Duration timeout) {
    toInvoke = new ArrayList<>();
    futures = new ArrayList<>();
    this.timeout = timeout;
    errorCollector = new ProtectedErrorCollector();
    allThreadsExecuted.set(true);
  }

  @Override
  protected void after() throws IllegalStateException {
    if (allThreadsExecuted.get() == Boolean.FALSE) {
      throw new IllegalStateException("Threads have been added that have not been executed.");
    }
    clear();
    stopThreadPool();
  }


  /**
   * Adds a Callable to the concurrency rule to be run. Expectations for return values and thrown
   * exceptions, as well as any repetition of the thread should be added using ConcurrentOperation.
   *
   * @param callable a Callable to be run. If the Callable throws an exception that is not expected
   *        it will be thrown up to the test that the threads are run from.
   * @return concurrentOperation, the ConcurrentOperation that has been added to the rule
   */
  public <T> ConcurrentOperation<T> add(Callable<T> callable) {
    ConcurrentOperation<T> concurrentOperation = new ConcurrentOperation(callable);
    toInvoke.add(concurrentOperation);
    allThreadsExecuted.set(false);

    return concurrentOperation;
  }


  /**
   * Runs all callables in the rule in parallel and fails if threads' conditions were not met. Each
   * thread runs until timeout has been reached. This method will not return until all
   * threads have completed or been cancelled.
   *
   * @throws InterruptedException if interrupted before timeout
   * @throws RuntimeException with cause of MultipleFailureException with a list of failures
   *         including AssertionErrors for all threads whose expectations were not met (if there are
   *         multiple failures).
   * @throws AssertionError if a single thread's expectations are not met
   * @throws Exception if a thread throws an unexpected exception
   */
  public void executeInParallel() {
    for (ConcurrentOperation op : toInvoke) {
      futures.add(threadPool.submit(op));
    }
    allThreadsExecuted.set(true);

    awaitFutures();
    errorCollector.verify();
  }

  /**
   * Runs all callables in the rule in the order that they were added and fail if threads'
   * conditions
   * are not met. Each thread runs until timeout is reached. This method will not return until all
   * threads have completed or been cancelled.
   *
   * @throws RuntimeException with cause of MultipleFailureException with a list of failures
   *         including AssertionErrors for all threads whose expectations were not met (if there are
   *         multiple failures).
   * @throws AssertionError if a single thread's expectations are not met
   * @throws Exception if a thread throws an unexpected exception
   */
  public void executeInSeries() {
    for (ConcurrentOperation op : toInvoke) {
      awaitFuture(threadPool.submit(op));
    }
    allThreadsExecuted.set(true);

    errorCollector.verify();
  }

  /**
   * Clears the lists of callables, futures, and errors. Use between calls to execute methods to
   * avoid rerunning and rethrowing callables from the previous executions.
   */
  public void clear() {
    toInvoke.clear();
    futures.clear();
    errorCollector = new ProtectedErrorCollector();
    allThreadsExecuted.set(true);
  }

  /**
   * Shuts down the thread pool. Does not need to be called if the rule's after is called
   */
  public void stopThreadPool() {
    threadPool.shutdownNow();
  }

  /**
   * Set the timeout for the threads. After the timeout is reached, the threads will be interrupted
   * and will throw a CancellationException
   */
  public void setTimeout(Duration timeout) {
    this.timeout = timeout;
  }

  /**
   * Turns a Runnable into a Void Callable in order to submit it to the rule for execution
   *
   * @param runnable a Runnable to convert to a Callable
   * @return a Callable with Void return type
   */
  public static Callable<Void> toCallable(Runnable runnable) {
    return () -> {
      runnable.run();
      return null;
    };
  }

  private void awaitFutures() {
    for (Future<Void> future : futures) {
      awaitFuture(future);
    }

    clearCompletedFutures();
  }

  private void awaitFuture(Future<Void> future) {
    try {
      future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      errorCollector.addError(e.getCause());
    } catch (Exception e) {
      errorCollector.addError(e);
    }
  }

  private static Object readField(final Class targetClass, final Object targetInstance,
      final String fieldName) {
    try {
      Field field = targetClass.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(targetInstance);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new Error(e);
    }
  }

  private void clearCompletedFutures() {
    futures.removeIf(future -> future.isCancelled() || future.isDone());
  }

  public static class ConcurrentOperation<T> implements Callable<Void> {
    private final int DEFAULT_ITERATIONS = 1;
    private final Duration DEFAULT_DURATION = Duration.ofSeconds(300);

    private Callable<T> callable;
    private int iterations;
    private Duration duration;
    private Boolean expectedResultIsSet;
    private T expectedValue;
    private Throwable expectedException;
    private Class expectedExceptionType;
    private Class expectedExceptionCauseType;

    public ConcurrentOperation() {
      callable = null;
      iterations = DEFAULT_ITERATIONS;
      duration = DEFAULT_DURATION;
      this.expectedResultIsSet = false;
      expectedException = null;
      expectedExceptionType = null;
      expectedExceptionCauseType = null;
      expectedValue = null;
    }

    public ConcurrentOperation(Callable<T> toAdd) {
      this.callable = toAdd;
      iterations = DEFAULT_ITERATIONS;
      duration = DEFAULT_DURATION;
      this.expectedResultIsSet = false;
      expectedException = null;
      expectedExceptionType = null;
      expectedExceptionCauseType = null;
      expectedValue = null;
    }

    /**
     * Sets a callable to be repeated the given number of times. If there is also an expected result
     * for the callable, that expectation must be met for each iteration of the callable.
     *
     * @param iterations the number of times to run the callable
     * @return this, the ConcurrentOperation (containing a callable) that has been set to repeat
     */
    public ConcurrentOperation repeatForIterations(int iterations) {
      if (!duration.equals(DEFAULT_DURATION)) {
        throw new IllegalArgumentException("Specify only Duration or Iterations");
      }

      this.iterations = iterations;
      return this;
    }

    /**
     * Sets a callable to be repeated for some duration. If there is also an expected result
     * for the callable, that expectation must be met for each iteration of the callable. The
     * callable will not be restarted after the duration has been met, however the current
     * iteration will be allowed to continue until the timeout is reached.
     *
     * @param duration the Duration for which to repeat the callable
     * @return this, the ConcurrentOperation (containing a callable) that has been set to repeat
     */
    public ConcurrentOperation repeatForDuration(Duration duration) {
      if (iterations != DEFAULT_ITERATIONS) {
        throw new IllegalArgumentException("Specify only Duration or Iterations");
      }

      this.duration = duration;
      return this;
    }

    /**
     * Sets the expected result of running the thread to be an exception matching the given
     * exception
     *
     * @param expectedException the expected exception. If the message is null, the message of the
     *        thrown exception will not be checked, however if the message is empty, the thrown
     *        exception
     *        must also have a null or empty message.
     * @return this, the ConcurrentOperation (containing a callable) that has been set to repeat
     */
    public ConcurrentOperation expectException(Throwable expectedException) {
      if (expectedResultIsSet) {
        throw new IllegalArgumentException("Specify only one expected outcome.");
      }

      this.expectedException = expectedException;
      this.expectedResultIsSet = true;
      return this;
    }

    /**
     * Sets the expected result of running the thread to be an exception that is an instance of the
     * given class
     *
     * @param expectedExceptionType the class of the expected exception. Causes will not be checked.
     * @return this, the ConcurrentOperation (containing a callable) that has been set to repeat
     */
    public ConcurrentOperation expectExceptionType(Class expectedExceptionType) {
      if (expectedException != null || expectedValue != null) {
        throw new IllegalArgumentException("Specify only one expected outcome.");
      }

      this.expectedExceptionType = expectedExceptionType;
      this.expectedResultIsSet = true;
      return this;
    }

    /**
     * Sets the expected result of running the thread to be an exception with a cause that is an
     * instance of the given class
     *
     * @param expectedExceptionCauseType the class of the expected exception cause. The exception
     *        itself will not be checked.
     * @return this, the ConcurrentOperation (containing a callable) that has been set to repeat
     */
    public ConcurrentOperation expectExceptionCauseType(Class expectedExceptionCauseType) {
      if (expectedException != null || expectedValue != null) {
        throw new IllegalArgumentException("Specify only one expected outcome.");
      }

      this.expectedExceptionCauseType = expectedExceptionCauseType;
      this.expectedResultIsSet = true;
      return this;
    }

    /**
     * Sets the expected result of running the thread to be a value matching the given value
     *
     * @param expectedValue the value expected to be returned from the thread. The value must
     *        implement equals
     * @return this, the ConcurrentOperation (containing a callable) that has been set to repeat
     */
    public ConcurrentOperation expectValue(T expectedValue) {
      if (this.expectedResultIsSet) {
        throw new IllegalArgumentException("Specify only one expected outcome.");
      }

      this.expectedValue = expectedValue;
      this.expectedResultIsSet = true;
      return this;
    }

    @Override
    public Void call() throws Exception {
      Stopwatch timeRun = duration != DEFAULT_DURATION ? Stopwatch.createStarted() : null;
      int numRuns = 0;

      do {
        numRuns++;
        callAndValidate();
      } while ((iterations != DEFAULT_ITERATIONS && numRuns < iterations) ||
          (duration != DEFAULT_DURATION
              && timeRun.elapsed(TimeUnit.SECONDS) <= duration.getSeconds()));
      return null;
    }

    private void callAndValidate() throws Exception {
      Exception exception = null;

      try {
        T retVal = this.callable.call();

        if (this.expectedValue != null) {
          assertThat(retVal).isEqualTo(this.expectedValue);
        }
      } catch (Exception e) {
        exception = e;
      }

      if (this.expectedExceptionCauseType != null && this.expectedExceptionType != null) {
        assertThat(exception).isInstanceOf(this.expectedExceptionType)
            .hasCauseInstanceOf(this.expectedExceptionCauseType);
      } else if (this.expectedExceptionType != null) {
        assertThat(exception).isInstanceOf(this.expectedExceptionType);
      } else if (this.expectedExceptionCauseType != null) {
        assertThat(exception).hasCauseInstanceOf(this.expectedExceptionCauseType);
      } else if (this.expectedException != null) {
        checkThrown(exception, this.expectedException);
      } else {
        if (exception != null) {
          throw exception; // rethrow if we weren't expecting any exception and got one
        }
      }

    }

    private void checkThrown(Throwable actual, Throwable expected) {
      assertThat(actual).isInstanceOf(expected.getClass());

      if (expected.getMessage() != null) {
        assertThat(actual).hasMessage(expected.getMessage());
      }

      if (expected.getCause() != null) {
        checkThrown(actual.getCause(), expected.getCause());
      }
    }
  }

  private static class ProtectedErrorCollector extends ErrorCollector {
    @Override
    protected void verify() {
      try {
        super.verify();
      } catch (Error | RuntimeException e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }

    List<Throwable> getErrors() {
      return (List<Throwable>) readField(ErrorCollector.class, this, "errors");
    }
  }
}
