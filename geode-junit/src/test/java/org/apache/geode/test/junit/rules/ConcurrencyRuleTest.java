/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.test.junit.rules;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Duration.FIVE_SECONDS;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.model.MultipleFailureException;

@RunWith(JUnitParamsRunner.class)
public class ConcurrencyRuleTest {
  private final AtomicBoolean invoked = new AtomicBoolean();
  private final AtomicInteger iterations = new AtomicInteger(0);

  private final int stopIteration = 2;
  private final Integer expectedRetVal = Integer.valueOf(72);
  private final Throwable expectedException =
      new IllegalStateException("Oh boy, here I go testin' again");

  private final Callable<Integer> callWithRetVal = () -> {
    invoked.set(Boolean.TRUE);
    return Integer.valueOf(72);
  };

  private final Callable<Integer> callWithRetValAndRepeatCount = () -> {
    iterations.incrementAndGet();
    return Integer.valueOf(72);
  };

  private final Callable<Integer> callWithRetValAndRepeatCountAndOneWrongValue = () -> {
    int currentIteration = iterations.incrementAndGet();
    if (currentIteration == stopIteration) {
      return Integer.valueOf(3);
    }
    return Integer.valueOf(72);
  };

  private final Callable<Void> callWithExceptionAndRepeatCount = () -> {
    iterations.incrementAndGet();
    throw new IllegalStateException("Oh boy, here I go testin' again");
  };

  private final Callable<Void> callWithOneExceptionAndRepeatCount = () -> {
    int currentIteration = iterations.incrementAndGet();
    if (currentIteration == stopIteration) {
      throw new IllegalStateException("Oh boy, here I go testin' again");
    }
    return null;
  };

  @Rule
  public ConcurrencyRule concurrencyRule = new ConcurrencyRule();

  @Before
  public void setUp() {
    invoked.set(false);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException(Execution execution) {
    concurrencyRule.runAndExpectException(() -> {
      throw new NullPointerException();
    }, NullPointerException.class);
    execution.execute(concurrencyRule);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException_throwableClass(Execution execution) {
    concurrencyRule.runAndExpectException(() -> {
      throw new NullPointerException("my custom message");
    }, NullPointerException.class);
    execution.execute(concurrencyRule);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException_throwableInstance(Execution execution) {
    Throwable expected = new NullPointerException("my custom message");
    concurrencyRule.runAndExpectException(() -> {
      throw new NullPointerException("my custom message");
    }, expected);
    execution.execute(concurrencyRule);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException_throwableInstanceWithCauses_failsIfCauseDoesNotMatch(
      Execution execution) {
    Throwable expectedDeepestCause = new RuntimeException("this is the first cause");
    Throwable expectedMiddleCause =
        new NullPointerException("this is the middle error").initCause(expectedDeepestCause);
    Throwable expected = new IllegalStateException("This is the expected outer error")
        .initCause(expectedMiddleCause);

    Callable<Void> callable = () -> {
      throw new IllegalStateException("This is the expected outer error");
    };

    concurrencyRule.runAndExpectException(callable, expected);

    assertThatThrownBy(() -> execution.execute(concurrencyRule))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Expecting actual not to be null");
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException_throwableInstance_wrongMessage_fails(Execution execution) {
    concurrencyRule.runAndExpectException(() -> {
      throw new NullPointerException("foo");
    }, new NullPointerException("bar"));
    assertThatThrownBy(() -> execution.execute(concurrencyRule))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException_throwableInstance_wrongClass_fails(Execution execution) {
    concurrencyRule.runAndExpectException(() -> {
      throw new IllegalArgumentException("foo");
    }, new NullPointerException("foo"));
    assertThatThrownBy(() -> execution.execute(concurrencyRule))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectNoException_withNoReturn(Execution execution) {
    concurrencyRule.runAndExpectNoException(ConcurrencyRule.toCallable(() -> invoked.set(true)));
    execution.execute(concurrencyRule);
    assertThat(invoked.get()).isTrue();
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectNoException_withReturn(Execution execution) {
    concurrencyRule.runAndExpectNoException(() -> {
      invoked.set(true);
      return true;
    });
    execution.execute(concurrencyRule);
    assertThat(invoked.get()).isTrue();
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectValue(Execution execution) {
    concurrencyRule.runAndExpectValue(callWithRetVal, expectedRetVal);
    execution.execute(concurrencyRule);
    assertThat(invoked.get()).isTrue();
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectValue_failsForWrongValue(Execution execution) {
    concurrencyRule.runAndExpectValue(callWithRetVal, Integer.valueOf(3));
    assertThatThrownBy(() -> execution.execute(concurrencyRule))
        .isInstanceOf(AssertionError.class);
    assertThat(invoked.get()).isTrue();
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterations(Execution execution) {
    int iterations = 4;
    this.iterations.set(0);

    concurrencyRule.repeatForIterations(callWithRetValAndRepeatCount, iterations);
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(iterations);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectExceptionForEach_byExceptionClass(Execution execution) {
    int iterations = 4;
    this.iterations.set(0);

    concurrencyRule.repeatForIterationsAndExpectExceptionForEach(callWithExceptionAndRepeatCount,
        iterations, expectedException.getClass());
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(iterations);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectExceptionForEach_byExceptionInstance(
      Execution execution) {
    int iterations = 4;
    this.iterations.set(0);

    concurrencyRule.repeatForIterationsAndExpectExceptionForEach(callWithExceptionAndRepeatCount,
        iterations, expectedException);
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(iterations);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectException_byExceptionType(Execution execution) {
    int iterations = 4;
    this.iterations.set(0);

    concurrencyRule.repeatForIterationsAndExpectException(callWithOneExceptionAndRepeatCount,
        iterations, expectedException.getClass());
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(2);
  }


  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectException_byExceptionInstance(Execution execution) {
    int iterations = 4;
    this.iterations.set(0);

    concurrencyRule.repeatForIterationsAndExpectException(callWithOneExceptionAndRepeatCount,
        iterations, expectedException);
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(2);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectValueForEach(Execution execution) {
    int iterations = 4;
    this.iterations.set(0);

    concurrencyRule.repeatForIterationsAndExpectValueForEach(callWithRetValAndRepeatCount,
        iterations, expectedRetVal);
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(iterations);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectValueForEach_failsWithOneWrongValue(Execution execution) {
    int iterations = 4;
    this.iterations.set(0);

    concurrencyRule.repeatForIterationsAndExpectValueForEach(
        callWithRetValAndRepeatCountAndOneWrongValue, iterations, expectedRetVal);
    assertThatThrownBy(() -> execution.execute(concurrencyRule)).isInstanceOf(AssertionError.class);
    assertThat(this.iterations.get()).isEqualTo(stopIteration);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForDuration(Execution execution) {
    Duration duration = Duration.ofMillis(200);
    this.iterations.set(0);

    concurrencyRule.repeatForDuration(callWithRetValAndRepeatCount, duration);
    Awaitility.await("Execution did not respect given duration").atMost(2, TimeUnit.SECONDS)
        .until(() -> {
          execution.execute(concurrencyRule);
          return true;
        });
    assertThat(iterations.get()).isGreaterThan(1);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void deadlocksGetResolved(Execution execution) {
    final AtomicBoolean lock1 = new AtomicBoolean();
    final AtomicBoolean lock2 = new AtomicBoolean();

    concurrencyRule.runAndExpectNoException(() -> {
      Awaitility.await().until(() -> lock2.equals(Boolean.TRUE));
      lock1.set(true);
      return null;
    });

    concurrencyRule.runAndExpectNoException(() -> {
      Awaitility.await().until(() -> lock1.equals(Boolean.TRUE));
      lock2.set(true);
      return null;
    });

    concurrencyRule.setTimeout(Duration.ofSeconds(1));

    Throwable thrown = catchThrowable(() -> execution.execute(concurrencyRule));
    Throwable cause = thrown.getCause();

    assertThat(thrown).isInstanceOf(RuntimeException.class);
    assertThat(cause).isInstanceOf(MultipleFailureException.class);
    assertThat(((MultipleFailureException) cause).getFailures())
        .hasSize(2)
        .hasOnlyElementsOfType(TimeoutException.class);
  }

  @Test
  public void clearEmptiesThreadsToRun() {
    final AtomicBoolean b1 = new AtomicBoolean(Boolean.FALSE);
    final AtomicBoolean b2 = new AtomicBoolean(Boolean.FALSE);
    final AtomicBoolean b3 = new AtomicBoolean(Boolean.FALSE);
    final AtomicBoolean b4 = new AtomicBoolean(Boolean.FALSE);

    Callable c1 = () -> {
      b1.set(true);
      return null;
    };
    Callable c2 = () -> {
      b2.set(true);
      return null;
    };
    Callable c3 = () -> {
      b3.set(true);
      return null;
    };
    Callable c4 = () -> {
      b4.set(true);
      return null;
    };

    // submit some threads and check they did what they're supposed to
    concurrencyRule.runAndExpectNoException(c1);
    concurrencyRule.runAndExpectNoException(c2);
    concurrencyRule.runAndExpectException(c3, IllegalArgumentException.class);
    concurrencyRule.executeInParallel();

    assertThat(concurrencyRule.getErrors().get(0)).isInstanceOf(AssertionError.class);
    assertThat(b1).isTrue();
    assertThat(b2).isTrue();
    assertThat(b3).isTrue();
    assertThat(b4).isFalse();

    // reset the booleans
    b1.set(false);
    b2.set(false);
    b3.set(false);
    b4.set(false);

    // empty the list
    concurrencyRule.clear();

    // submit some more threads and check that ONLY those were executed
    concurrencyRule.runAndExpectNoException(c3);
    concurrencyRule.runAndExpectNoException(c4);
    concurrencyRule.executeInParallel();

    assertThat(catchThrowable(() -> concurrencyRule.processResults())).isNull();
    assertThat(b1).isFalse();
    assertThat(b2).isFalse();
    assertThat(b3).isTrue();
    assertThat(b4).isTrue();
  }

  @Test
  public void threadPoolCanBeUsedAfterCancellingThreads() {
    final AtomicBoolean b1 = new AtomicBoolean(false);
    final AtomicBoolean b2 = new AtomicBoolean(false);

    Callable c1 = () -> {
      b1.set(true);
      Awaitility.await("Thread cancellation signal").atLeast(FIVE_SECONDS).until(() -> false);
      b2.set(true);
      return null;
    };

    Callable c2 = () -> {
      b2.set(true);
      return null;
    };

    // start a thread
    concurrencyRule.runAndExpectException(c1, new InterruptedException(null));
    concurrencyRule.executeInParallel();

    // wait a second for thread to hit its Awaitility then cancel it
    catchThrowable(() -> Awaitility.waitAtMost(org.awaitility.Duration.ONE_SECOND)
        .until(() -> b1.get() == Boolean.TRUE));
    concurrencyRule.cancelThreads();

    // check the results
    assertThat(catchThrowable(() -> concurrencyRule.processResults()))
        .isInstanceOf(CancellationException.class);
    assertThat(b1).isTrue();
    assertThat(b2).isFalse();

    // reset variables
    b1.set(false);
    b2.set(false);
    concurrencyRule.clear();

    // submit and start a new thread
    concurrencyRule.runAndExpectNoException(c2);
    concurrencyRule.executeInParallel();

    // check that it was executed successfully
    assertThat(catchThrowable(() -> concurrencyRule.processResults())).isNull();
    assertThat(b1.get()).isFalse();
    assertThat(b2.get()).isTrue();
  }

  @Test
  public void resultsCanBeGatheredAfterThreadCancellation() {
    final AtomicBoolean b1 = new AtomicBoolean(false);
    final AtomicBoolean b2 = new AtomicBoolean(false);

    Callable c1 = () -> {
      b1.set(true);
      Awaitility.await("Thread cancellation signal").atLeast(FIVE_SECONDS).until(() -> false);
      b2.set(true);
      return null;
    };

    Callable c2 = () -> {
      b2.set(true);
      return null;
    };

    // start a thread
    concurrencyRule.runAndExpectException(c1, new InterruptedException(null));
    concurrencyRule.runAndExpectException(c2, new IllegalArgumentException());
    concurrencyRule.executeInParallel();

    // wait a second for thread to hit its Awaitility then cancel it
    catchThrowable(() -> Awaitility.waitAtMost(org.awaitility.Duration.ONE_SECOND)
        .until(() -> b1.get() == Boolean.TRUE && b2.get() == Boolean.TRUE));
    concurrencyRule.cancelThreads();

    List<Throwable> errors = concurrencyRule.getErrors();
    assertThat(errors).hasSize(2);
    assertThat(errors.get(0)).isInstanceOf(CancellationException.class);
    assertThat(errors.get(1)).isInstanceOf(AssertionError.class);
  }

  @Test
  public void runManyThreads_inParallel() {
    concurrencyRule.runAndExpectException(() -> {
      throw new IllegalArgumentException("foo");
    }, new NullPointerException("foo"));
    concurrencyRule.runAndExpectException(() -> {
      throw new IOException("foo");
    }, new IOException("foo"));
    concurrencyRule.runAndExpectValue(() -> {
      return "successful value";
    }, "successful value");
    concurrencyRule.runAndExpectValue(() -> {
      return "failing value";
    }, "wrong value");
    concurrencyRule.runAndExpectNoException(() -> {
      invoked.set(true);
      return null;
    });
    concurrencyRule.runAndExpectNoException(() -> {
      throw new IllegalStateException();
    });

    concurrencyRule.executeInParallel();
    List<Throwable> errors = concurrencyRule.getErrors();
    assertThat(errors).hasSize(3).hasOnlyElementsOfType(AssertionError.class);
    assertThat(errors.get(0)).hasMessageContaining(IllegalArgumentException.class.getName());
    assertThat(errors.get(1)).hasMessageContaining("[faili]ng value")
        .hasMessageContaining("[wro]ng value");
    assertThat(errors.get(2)).hasMessageContaining("null")
        .hasMessageContaining(IllegalStateException.class.getName());
  }

  @Test
  public void runManyThreads_inSeries() {
    concurrencyRule.runAndExpectException(() -> {
      throw new IllegalArgumentException("foo");
    }, new NullPointerException("foo"));
    concurrencyRule.runAndExpectException(() -> {
      throw new IOException("foo");
    }, new IOException("foo"));
    concurrencyRule.runAndExpectValue(() -> {
      return "successful value";
    }, "successful value");
    concurrencyRule.runAndExpectValue(() -> {
      return "failing value";
    }, "wrong value");
    concurrencyRule.runAndExpectNoException(() -> {
      invoked.set(true);
      return null;
    });
    concurrencyRule.runAndExpectNoException(() -> {
      throw new IllegalStateException();
    });

    Throwable thrown = catchThrowable(() -> concurrencyRule.executeInSeries());
    List<Throwable> errors = ((MultipleFailureException) thrown.getCause()).getFailures();
    assertThat(errors).hasSize(3).hasOnlyElementsOfType(AssertionError.class);
    assertThat(errors.get(0)).hasMessageContaining(IllegalArgumentException.class.getName());
    assertThat(errors.get(1)).hasMessageContaining("[faili]ng value")
        .hasMessageContaining("[wro]ng value");
    assertThat(errors.get(2)).hasMessageContaining("null")
        .hasMessageContaining(IllegalStateException.class.getName());
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void timeoutValueIsRespected_inSeries(Execution execution) {

    Callable<Void> c1 = () -> {
      Thread.sleep(5000);
      return null;
    };

    concurrencyRule.setTimeout(Duration.ofSeconds(1));
    concurrencyRule.runAndExpectNoException(c1);
    concurrencyRule.runAndExpectNoException(c1);
    Awaitility.await("timeout is respected").atMost(3, TimeUnit.SECONDS).until(() -> {
      Throwable thrown = catchThrowable(() -> execution.execute(concurrencyRule));
      System.out.println(thrown);
      assertThat(((MultipleFailureException) thrown.getCause()).getFailures()).hasSize(2)
          .hasOnlyElementsOfType(TimeoutException.class);
      return true;
    });
  }

  @SuppressWarnings("unused")
  private enum Execution {
    EXECUTE_IN_SERIES(concurrencyRule -> {
      concurrencyRule.executeInSeries();
    }),
    EXECUTE_IN_PARALLEL(concurrencyRule -> {
      concurrencyRule.executeInParallel();
      concurrencyRule.processResults();
    });

    private final Consumer<ConcurrencyRule> execution;

    Execution(Consumer<ConcurrencyRule> execution) {
      this.execution = execution;
    }

    void execute(ConcurrencyRule concurrencyRule) {
      execution.accept(concurrencyRule);
    }
  }
}
