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

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
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
  private final IllegalStateException expectedExceptionWithCause =
      new IllegalStateException("Oh boy, here I go testin' again");
  {
    expectedExceptionWithCause.initCause(new NullPointerException());
  }

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
  public void runAndExpectExceptionType(Execution execution) {
    concurrencyRule.add(() -> {
      throw new NullPointerException();
    }).expectExceptionType(NullPointerException.class);
    execution.execute(concurrencyRule);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException(Execution execution) {
    Throwable expected = new NullPointerException("my custom message");
    concurrencyRule.add(() -> {
      throw new NullPointerException("my custom message");
    }).expectException(expected);
    execution.execute(concurrencyRule);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException_throwableInstanceWithCauses(Execution execution) {
    Callable<?> callable = () -> {
      NullPointerException cause = new NullPointerException();
      IllegalStateException toThrow = new IllegalStateException("Oh boy, here I go testin' again");
      toThrow.initCause(cause);
      throw toThrow;
    };

    concurrencyRule.add(callable).expectException(expectedExceptionWithCause);
    execution.execute(concurrencyRule);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException_throwableInstanceWithCauses_failsIfCauseDoesNotMatch(
      Execution execution) {
    Callable<Void> callable = () -> {
      throw new IllegalStateException("Oh boy, here I go testin' again");
    };

    concurrencyRule.add(callable).expectException(expectedExceptionWithCause);

    assertThatThrownBy(() -> execution.execute(concurrencyRule))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Expecting actual not to be null");
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException_throwableInstance_wrongMessage_fails(Execution execution) {
    Callable<?> callable = () -> {
      throw new NullPointerException("foo");
    };

    concurrencyRule.add(callable).expectException(new NullPointerException("bar"));
    assertThatThrownBy(() -> execution.execute(concurrencyRule))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectException_throwableInstance_wrongClass_fails(Execution execution) {
    Callable<?> callable = () -> {
      throw new IllegalArgumentException("foo");
    };

    concurrencyRule.add(callable).expectException(new NullPointerException("foo"));
    assertThatThrownBy(() -> execution.execute(concurrencyRule))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectNoException_withNoReturn(Execution execution) {
    concurrencyRule.add(ConcurrencyRule.toCallable(() -> invoked.set(true)));
    execution.execute(concurrencyRule);
    assertThat(invoked.get()).isTrue();
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectNoException_withReturn(Execution execution) {
    concurrencyRule.add(() -> {
      invoked.set(true);
      return true;
    });
    execution.execute(concurrencyRule);
    assertThat(invoked.get()).isTrue();
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectValue(Execution execution) {
    concurrencyRule.add(callWithRetVal).expectValue(expectedRetVal);
    execution.execute(concurrencyRule);
    assertThat(invoked.get()).isTrue();
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runAndExpectValue_failsForWrongValue(Execution execution) {
    concurrencyRule.add(callWithRetVal).expectValue(Integer.valueOf(3));
    assertThatThrownBy(() -> execution.execute(concurrencyRule))
        .isInstanceOf(AssertionError.class);
    assertThat(invoked.get()).isTrue();
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterations(Execution execution) {
    int expectedIterations = 4;
    this.iterations.set(0);

    concurrencyRule.add(callWithRetValAndRepeatCount).repeatForIterations(4);
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(expectedIterations);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectExceptionForEach_byExceptionClass(Execution execution) {
    int expectedIterations = 4;
    this.iterations.set(0);

    concurrencyRule.add(callWithExceptionAndRepeatCount)
        .expectExceptionType(expectedException.getClass()).repeatForIterations(4);
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(expectedIterations);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectExceptionForEach_byExceptionInstance(
      Execution execution) {
    int expectedIteration = 4;
    this.iterations.set(0);

    concurrencyRule.add(callWithExceptionAndRepeatCount).expectException(expectedException)
        .repeatForIterations(4);
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(expectedIteration);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectValueForEach(Execution execution) {
    int ExpectedIterations = 4;
    this.iterations.set(0);

    concurrencyRule.add(callWithRetValAndRepeatCount).repeatForIterations(4)
        .expectValue(expectedRetVal);
    execution.execute(concurrencyRule);
    assertThat(this.iterations.get()).isEqualTo(ExpectedIterations);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForIterationsAndExpectValueForEach_failsWithOneWrongValue(Execution execution) {
    int expectedIterations = 4;
    this.iterations.set(0);

    concurrencyRule.add(callWithRetValAndRepeatCountAndOneWrongValue).expectValue(expectedRetVal)
        .repeatForIterations(expectedIterations);
    assertThatThrownBy(() -> execution.execute(concurrencyRule)).isInstanceOf(AssertionError.class);
    assertThat(this.iterations.get()).isEqualTo(stopIteration);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void repeatForDuration(Execution execution) {
    Duration duration = Duration.ofMillis(200);
    this.iterations.set(0);

    concurrencyRule.add(callWithRetValAndRepeatCount).repeatForDuration(duration);
    Awaitility.await("Execution did not respect given duration").atMost(2, TimeUnit.MINUTES)
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

    concurrencyRule.add(() -> {
      Awaitility.await().until(() -> lock2.equals(Boolean.TRUE));
      lock1.set(true);
      return null;
    });

    concurrencyRule.add(() -> {
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
    concurrencyRule.add(c1);
    concurrencyRule.add(c2);
    concurrencyRule.add(c3).expectExceptionType(IllegalArgumentException.class);
    Throwable thrown = catchThrowable(() -> concurrencyRule.executeInParallel());

    assertThat(thrown).isInstanceOf(AssertionError.class);
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
    concurrencyRule.add(c3);
    concurrencyRule.add(c4);

    assertThat(catchThrowable(() -> concurrencyRule.executeInParallel())).isNull();
    assertThat(b1).isFalse();
    assertThat(b2).isFalse();
    assertThat(b3).isTrue();
    assertThat(b4).isTrue();
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void runManyThreads(Execution execution) {
    Callable<Void> exceptionCallable = () -> {
      throw new IOException("foo");
    };

    Callable<String> valueCallable = () -> {
      return "successful value";
    };

    Callable<Void> setInvokedCallable = () -> {
      invoked.set(true);
      return null;
    };

    concurrencyRule.add(exceptionCallable).expectException(new NullPointerException("foo"));
    concurrencyRule.add(exceptionCallable).expectException(new IOException("foo"));
    concurrencyRule.add(valueCallable).expectValue("successful value");
    concurrencyRule.add(valueCallable).expectValue("wrong value");
    concurrencyRule.add(setInvokedCallable);
    concurrencyRule.add(exceptionCallable);

    Throwable thrown = catchThrowable(() -> execution.execute(concurrencyRule));
    List<Throwable> errors = ((MultipleFailureException) thrown.getCause()).getFailures();

    assertThat(errors).hasSize(3);
    assertThat(errors.get(0)).isInstanceOf(AssertionError.class)
        .hasMessageContaining(IOException.class.getName());
    assertThat(errors.get(1)).isInstanceOf(AssertionError.class)
        .hasMessageContaining("[successful] value")
        .hasMessageContaining("[wrong] value");
    assertThat(errors.get(2)).hasMessageContaining("foo")
        .isInstanceOf(IOException.class);
  }

  @Test
  @Parameters({"EXECUTE_IN_SERIES", "EXECUTE_IN_PARALLEL"})
  public void timeoutValueIsRespected(Execution execution) {

    Callable<Void> c1 = () -> {
      Thread.sleep(5000);
      return null;
    };

    concurrencyRule.setTimeout(Duration.ofSeconds(1));
    concurrencyRule.add(c1);
    concurrencyRule.add(c1);
    Awaitility.await("timeout is respected").atMost(3, TimeUnit.SECONDS).until(() -> {
      Throwable thrown = catchThrowable(() -> execution.execute(concurrencyRule));
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
