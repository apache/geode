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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

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

import com.google.common.base.Stopwatch;
import org.junit.rules.ErrorCollector;

import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

public class ConcurrencyRule extends SerializableExternalResource {

  private final ExecutorService threadPool = Executors.newCachedThreadPool();
  private final Collection<Callable<Void>> toRun;
  private final Collection<Future<Void>> futures;

  private ProtectedErrorCollector errorCollector;
  private Duration timeout;

  /**
   * A default constructor that sets the timeout to a default of 30 seconds
   */
  public ConcurrencyRule() {
    toRun = new ArrayList<>();
    futures = new ArrayList<>();
    timeout = Duration.ofSeconds(300);
    errorCollector = new ProtectedErrorCollector();
  }

  /**
   * A non-default constructor that sets the timeout to the given duration
   *
   * @param duration the maximum duration that threads should execute, given that the submitted
   *        tasks respond to cancellation signals.
   */
  public ConcurrencyRule(Duration duration) {
    toRun = new ArrayList<>();
    futures = new ArrayList<>();
    timeout = duration;
    errorCollector = new ProtectedErrorCollector();
  }

  @Override
  protected void after() {
    clear();
    stopThreadPool();
  }

  /**
   * Submit the given callable to toRun and assert that an exception of the given type is thrown.
   * The thread will be cancelled when the class timeout is reached. The exception thrown will be
   * checked by class only.
   *
   * @param callable a Callable that throws an exception and responds to cancellation signal
   * @param exceptionType a Class of Exception that is expected to be thrown by the callable
   */
  public void runAndExpectException(Callable<?> callable, Class exceptionType) {
    Callable toSubmit = () -> {
      assertThatThrownBy(() -> callable.call()).isInstanceOf(exceptionType);
      return null;
    };
    toRun.add(toSubmit);
  }

  /**
   * Submit the given callable to toRun and assert that an exception of the given type is thrown.
   * The thread will be cancelled when the class timeout is reached. The exception thrown will be
   * checked by class, then message, then any nested causes.
   *
   * @param callable a Callable that throws an exception and responds to cancellation signal. The
   *        exception can't currently contain causes.
   * @param throwable a Throwable that is expected to be thrown by the callable. The
   *        exception can contain causes, but if it does the assertion will fail (until the
   *        callables can
   *        throw with causes)
   */
  public void runAndExpectException(Callable<?> callable, Throwable throwable) {
    Callable toSubmit = () -> {
      Throwable thrown = catchThrowable(() -> callable.call());

      checkThrown(throwable, thrown);

      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Submit the given Callable to toRun and assert that no exceptions are thrown. The thread will be
   * cancelled when the class timeout is reached. The Callable throwing an exception will result in
   * an assertion failure in the results.
   *
   * @param callable a Callable that does not throw an exception and responds to cancellation
   *        signal. Callable may have a return value but it will not be checked or collected.
   */
  public void runAndExpectNoException(Callable<?> callable) {
    Callable toSubmit = () -> {
      assertThatCode(() -> callable.call()).doesNotThrowAnyException();
      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Submit the given Callable to toRun and assert that the result of that callable is the given
   * expected value. The thread will be cancelled when the class timeout is reached. The Callable
   * throwing an exception will result in an assertion failure in the results.
   *
   * @param callable a Callable that returns an object, does not throw exceptions and responds to
   *        cancellation signal
   * @param value an Object that is the expected return value from the Callable.
   */
  public <T> void runAndExpectValue(Callable<T> callable, T value) {
    Callable toSubmit = () -> {
      assertThat(callable.call()).isEqualTo(value);
      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Run the given callable in a loop for some number of iterations. Fails if the callable throws an
   * exception in any iteration. After the number of iterations has been met, the loop will not
   * restart the runnable. The running iteration of the Runnable will be cancelled when the
   * timeout set on the class is reached, regardless of if the number of desired iterations has
   * been reached. A cancellation will result in a test failure.
   *
   * @param callable a Callable that does not throw an exception and responds to cancellation signal
   * @param iterations a maximum number of iterations to repeat for
   */
  public void repeatForIterations(Callable<?> callable, int iterations) {
    Callable toSubmit = () -> {
      for (int i = 0; i < iterations; i++) {
        callable.call();
      }
      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Run the given callable in a loop for some number of iterations. Fails if the callable throws an
   * exception in any iteration. After the number of iterations has been met, the loop will not
   * restart the runnable. The running iteration of the Callable will be cancelled when the
   * timeout set on the class is reached, regardless of if the number of desired iterations has
   * been reached. A cancellation will result in a test failure.
   *
   * @param callable a Callable that does not throw an exception and responds to cancellation signal
   * @param iterations a maximum number of iterations to repeat for
   * @param exceptionType the type of exception to expect
   */
  public void repeatForIterationsAndExpectExceptionForEach(Callable<?> callable, int iterations,
      Class exceptionType) {
    Callable toSubmit = () -> {
      for (int i = 0; i < iterations; i++) {
        assertThatThrownBy(() -> callable.call()).isInstanceOf(exceptionType);
      }
      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Run the given callable in a loop for some number of iterations. Fails if the callable throws an
   * exception in any iteration. After the number of iterations has been met, the loop will not
   * restart the runnable. The running iteration of the Callable will be cancelled when the
   * timeout set on the class is reached, regardless of if the number of desired iterations has
   * been reached. A cancellation will result in a test failure.
   *
   * @param callable a Callable that does not throw an exception and responds to cancellation signal
   * @param iterations a maximum number of iterations to repeat for
   * @param exception the exception to expect
   */
  public void repeatForIterationsAndExpectExceptionForEach(Callable<?> callable, int iterations,
      Throwable exception) {
    Callable toSubmit = () -> {
      for (int i = 0; i < iterations; i++) {
        Throwable thrown = catchThrowable(() -> callable.call());
        checkThrown(thrown, exception);
      }
      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Run the given callable in a loop for some number of iterations. Fails if the expected error is
   * not thrown by the final iteration or if an unexpected exception is thrown on any iteration.
   * After the number of iterations has been met, the loop will not restart the runnable. The
   * running iteration of the Callable will be cancelled when the timeout set on the class is
   * reached, regardless of if the number of desired iterations has been reached. A cancellation
   * will result in a test failure.
   *
   * @param callable a Callable that throws an exception and responds to cancellation signal
   * @param iterations a maximum number of iterations to repeat for
   * @param exceptionType the type of exception to expect
   */
  public void repeatForIterationsAndExpectException(Callable<?> callable, int iterations,
      Class exceptionType) {
    Callable toSubmit = () -> {
      Boolean foundException = Boolean.FALSE;
      try {
        for (int i = 0; i < iterations; i++) {
          callable.call();
        }
      } catch (Exception e) {
        foundException = Boolean.TRUE;
        assertThat(e).isInstanceOf(exceptionType);
      }

      assertThat(foundException).isTrue();
      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Run the given callable in a loop for some number of iterations. Fails if the expected error is
   * not thrown by the final iteration or if an unexpected exception is thrown on any iteration.
   * After the number of iterations has been met, the loop will not restart the runnable. The
   * running iteration of the Callable will be cancelled when the timeout set on the class is
   * reached, regardless of if the number of desired iterations has been reached. A cancellation
   * will result in a test failure.
   *
   * @param callable a Callable that throws an exception and responds to cancellation signal
   * @param iterations a maximum number of iterations to repeat for
   * @param exception the of exception to expect
   */
  public void repeatForIterationsAndExpectException(Callable<?> callable, int iterations,
      Throwable exception) {
    Callable toSubmit = () -> {
      Boolean foundException = Boolean.FALSE;
      try {
        for (int i = 0; i < iterations; i++) {
          callable.call();
        }
      } catch (Exception e) {
        foundException = Boolean.TRUE;
        checkThrown(e, exception);
      }

      assertThat(foundException).isTrue();
      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Run the given callable in a loop for some number of iterations. Fails if the callable throws an
   * exception in any iteration. After the number of iterations has been met, the loop will not
   * restart the callable. The running iteration of the Callable will be cancelled when the
   * timeout set on the class is reached, regardless of if the number of desired iterations has
   * been reached. A cancellation will result in a test failure. The expected value will be checked
   * for each iteration and will fail if the values are not equal.
   *
   * @param callable a Callable that does not throw an exception and responds to cancellation signal
   * @param iterations a maximum number of iterations to repeat for
   */
  public <T> void repeatForIterationsAndExpectValueForEach(Callable<T> callable, int iterations,
      T value) {
    Callable toSubmit = () -> {
      for (int i = 0; i < iterations; i++) {
        assertThat(callable.call()).isEqualTo(value);
      }
      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Run the given callable in a loop for a certain duration. Fails if the callable throws an
   * exception in any iteration. After the duration has been exceeded, the loop will not restart
   * the callable, however the current iteration will not be cancelled until the timeout value for
   * the class has been reached. A cancellation will result in a test failure.
   *
   * @param callable a Callable that does not throw exceptions and responds to cancellation signal
   * @param duration a maximum amount of time to repeat for
   */
  public void repeatForDuration(Callable callable, Duration duration) {
    Callable toSubmit = () -> {
      Stopwatch stopwatch = Stopwatch.createStarted();
      do {
        callable.call();
      } while (stopwatch.elapsed(TimeUnit.SECONDS) <= duration.getSeconds());
      return null;
    };

    toRun.add(toSubmit);
  }

  /**
   * Runs all callables in toRun in parallel and fail if threads' conditions were not met. Runs
   * until timeout is reached.
   *
   * NOTE: in order to consider the results of this execution, processResults() OR getErrors() must
   * be called.
   *
   * @throws InterruptedException if interrupted before timeout
   */
  public void executeInParallel() {
    for (Callable<Void> toSubmit : toRun) {
      futures.add(threadPool.submit(toSubmit));
    }
  }

  /**
   * Runs all callables in toRun in the order that they were added and fail if threads' conditions
   * were not met. Each thread runs until timeout is reached. This method will not return until all
   * threads in toRun have completed or been cancelled.
   *
   * @throws InterruptedException if interrupted before timeout
   * @throws RuntimeException with cause of MultipleFailureException with a list of failures
   *         including AssertionErrors for all threads whose expectations were not met.
   */
  public void executeInSeries() {
    for (Callable<Void> toSubmit : toRun) {
      awaitFuture(threadPool.submit(toSubmit));
    }

    errorCollector.verify();
  }

  /**
   * Awaits the results from parallel executions of callables and rethrows the resulting errors.
   *
   * @throws InterruptedException if interrupted before timeout
   * @throws RuntimeException with cause of MultipleFailureException with a list of failures
   *         including AssertionErrors for all threads whose expectations were not met.
   */
  public void processResults() throws AssertionError, RuntimeException {
    awaitFutures();
    errorCollector.verify();
  }

  /**
   * Awaits the results from parallel executions of callables and returns the resulting errors.
   *
   * @throws InterruptedException if interrupted before timeout
   */
  public List<Throwable> getErrors() {
    awaitFutures();
    return errorCollector.getErrors();
  }

  /**
   * Clears the lists of callables, futures, and errors. Use between calls to execute methods to
   * avoid rerunning and rethrowing callables from the previous executions.
   */
  public void clear() {
    toRun.clear();
    futures.clear();
    errorCollector = new ProtectedErrorCollector();
  }

  /**
   * Safely clears the lists of callables, futures, and errors, cancelling futures before clearing
   * the list of futures. Use between calls to execute methods to avoid rerunning and rethrowing
   * callables from the previous executions.
   */
  public void safeClear() {
    for (Future<Void> future : futures) {
      if (!future.isDone() && !future.isCancelled()) {
        future.cancel(true);
      }
    }

    clear();
  }

  /**
   * Attempts to cancel all futures from parallel execution
   */
  public void cancelThreads() {
    for (Future<Void> future : futures) {
      future.cancel(true);
    }
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
  public void setTimeout(Duration duration) {
    timeout = duration;
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
      future.get(timeout.getSeconds(), TimeUnit.SECONDS);
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


  private void checkThrown(Throwable expected, Throwable actual) {
    assertThat(actual).isInstanceOf(expected.getClass());

    if (!expected.getMessage().isEmpty()) {
      assertThat(actual).hasMessage(expected.getMessage());
    }

    if (expected.getCause() != null) {
      checkThrown(expected.getCause(), actual.getCause());
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
