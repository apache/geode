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
package org.apache.geode.internal.cache.execute.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.security.ResourcePermission;

public class TimingFunctionTest {

  private static final String INNER_FUNCTION_ID = "InnerFunction";

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  private Function<Void> innerFunction;

  @Mock
  private FunctionContext<Void> functionContext;

  private MeterRegistry meterRegistry;

  @Before
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();

    when(innerFunction.getId()).thenReturn(INNER_FUNCTION_ID);
  }

  @After
  public void tearDown() {
    meterRegistry.close();
  }

  @Test
  public void constructor_registersSuccessTimer() {
    new TimingFunction<>(innerFunction, meterRegistry);

    assertThat(successTimer())
        .as("geode.function.executions timer with tags function=%s, succeeded=true",
            INNER_FUNCTION_ID)
        .isNotNull();
  }

  @Test
  public void constructor_registersFailureTimer() {
    new TimingFunction<>(innerFunction, meterRegistry);

    assertThat(failureTimer())
        .as("geode.function.executions timer with tags function=%s, succeeded=false",
            INNER_FUNCTION_ID)
        .isNotNull();
  }

  @Test
  public void constructor_setsDescriptionForSuccessTimer() {
    new TimingFunction<>(innerFunction, meterRegistry);

    assertThat(successTimer().getId().getDescription())
        .as("success timer description")
        .isEqualTo("Count and total time of successful function executions");
  }

  @Test
  public void constructor_setsDescriptionForFailureTimer() {
    new TimingFunction<>(innerFunction, meterRegistry);

    assertThat(failureTimer().getId().getDescription())
        .as("failure timer description")
        .isEqualTo("Count and total time of failed function executions");
  }

  @Test
  public void close_removesSuccessTimer() {
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    timingFunction.close();

    assertThat(successTimer())
        .as("geode.function.executions timer with tags function=%s, succeeded=true",
            INNER_FUNCTION_ID)
        .isNull();
  }

  @Test
  public void close_removesFailureTimer() {
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    timingFunction.close();

    assertThat(failureTimer())
        .as("geode.function.executions timer with tags function=%s, succeeded=false",
            INNER_FUNCTION_ID)
        .isNull();
  }

  @Test
  public void execute_executesInnerFunction() {
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    timingFunction.execute(functionContext);

    verify(innerFunction).execute(same(functionContext));
  }

  @Test
  public void execute_succeeded_incrementsSuccessTimerCount() {
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    timingFunction.execute(functionContext);

    assertThat(successTimer().count())
        .as("success timer count")
        .isEqualTo(1);
  }

  @Test
  public void execute_succeeded_doesNotIncrementFailureTimerCount() {
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    timingFunction.execute(functionContext);

    assertThat(failureTimer().count())
        .as("failure timer count")
        .isEqualTo(0);
  }

  @Test
  public void execute_rethrowsRuntimeExceptionFromInnerFunction() {
    RuntimeException runtimeException = new RuntimeException("test");
    doThrow(runtimeException).when(innerFunction).execute(any());
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    Throwable thrown = catchThrowable(() -> timingFunction.execute(functionContext));

    assertThat(thrown).isSameAs(runtimeException);
  }

  @Test
  public void execute_rethrowsErrorFromInnerFunction() {
    Error error = new Error("test");
    doThrow(error).when(innerFunction).execute(any());
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    Throwable thrown = catchThrowable(() -> timingFunction.execute(functionContext));

    assertThat(thrown).isSameAs(error);
  }

  @Test
  public void execute_failed_incrementsFailureTimerCount() {
    doThrow(new RuntimeException("test")).when(innerFunction).execute(any());
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    catchThrowable(() -> timingFunction.execute(functionContext));

    assertThat(failureTimer().count())
        .as("failure timer count")
        .isEqualTo(1);
  }

  @Test
  public void execute_failed_doesNotIncrementSuccessTimerCount() {
    doThrow(new RuntimeException("test")).when(innerFunction).execute(any());
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    catchThrowable(() -> timingFunction.execute(functionContext));

    assertThat(successTimer().count())
        .as("success timer count")
        .isEqualTo(0);
  }

  @Test
  public void execute_succeeded_incrementsSuccessTimerTotalTime() {
    LongSupplier clock = mock(LongSupplier.class);
    when(clock.getAsLong()).thenReturn(0L, 42L);
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry, clock);

    timingFunction.execute(functionContext);

    assertThat(successTimer().totalTime(TimeUnit.NANOSECONDS))
        .as("success timer total time")
        .isEqualTo(42);
  }

  @Test
  public void execute_succeeded_doesNotIncrementFailureTimerTotalTime() {
    LongSupplier clock = mock(LongSupplier.class);
    when(clock.getAsLong()).thenReturn(0L, 42L);
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry, clock);

    timingFunction.execute(functionContext);

    assertThat(failureTimer().totalTime(TimeUnit.NANOSECONDS))
        .as("failure timer total time")
        .isEqualTo(0);
  }

  @Test
  public void execute_failed_incrementsFailureTimerTotalTime() {
    LongSupplier clock = mock(LongSupplier.class);
    when(clock.getAsLong()).thenReturn(0L, 42L);
    doThrow(new RuntimeException("test")).when(innerFunction).execute(any());
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry, clock);

    catchThrowable(() -> timingFunction.execute(functionContext));

    assertThat(failureTimer().totalTime(TimeUnit.NANOSECONDS))
        .as("failure timer total time")
        .isEqualTo(42);
  }

  @Test
  public void execute_failed_doesNotIncrementSuccessTimerTotalTime() {
    LongSupplier clock = mock(LongSupplier.class);
    when(clock.getAsLong()).thenReturn(0L, 42L);
    doThrow(new RuntimeException("test")).when(innerFunction).execute(any());
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry, clock);

    catchThrowable(() -> timingFunction.execute(functionContext));

    assertThat(successTimer().totalTime(TimeUnit.NANOSECONDS))
        .as("success timer total time")
        .isEqualTo(0);
  }

  @Test
  public void hasResult_delegatesToInnerFunction() {
    when(innerFunction.hasResult()).thenReturn(true);
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    boolean value = timingFunction.hasResult();

    assertThat(value).isTrue();
    verify(innerFunction).hasResult();
  }

  @Test
  public void getId_delegatesToInnerFunction() {
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    String value = timingFunction.getId();

    assertThat(value).isEqualTo(INNER_FUNCTION_ID);
    verify(innerFunction, atLeastOnce()).getId();
  }

  @Test
  public void optimizeForWrite_delegatesToInnerFunction() {
    when(innerFunction.optimizeForWrite()).thenReturn(true);
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    boolean value = timingFunction.optimizeForWrite();

    assertThat(value).isTrue();
    verify(innerFunction).optimizeForWrite();
  }

  @Test
  public void isHA_delegatesToInnerFunction() {
    when(innerFunction.isHA()).thenReturn(true);
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    boolean value = timingFunction.isHA();

    assertThat(value).isTrue();
    verify(innerFunction).isHA();
  }

  @Test
  public void getRequiredPermissions_delegatesToInnerFunction() {
    ResourcePermission resourcePermission = mock(ResourcePermission.class);
    when(innerFunction.getRequiredPermissions(any()))
        .thenReturn(Collections.singleton(resourcePermission));
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    Collection<ResourcePermission> value = timingFunction.getRequiredPermissions("foo");

    assertThat(value).containsExactly(resourcePermission);
    verify(innerFunction).getRequiredPermissions(eq("foo"));
  }

  @Test
  public void getRequiredPermissions_withArgs_delegatesToInnerFunction() {
    ResourcePermission resourcePermission = mock(ResourcePermission.class);
    when(innerFunction.getRequiredPermissions(any(), any()))
        .thenReturn(Collections.singleton(resourcePermission));
    TimingFunction<Void> timingFunction = new TimingFunction<>(innerFunction, meterRegistry);

    Collection<ResourcePermission> value =
        timingFunction.getRequiredPermissions("foo", new String[] {"bar"});

    assertThat(value).containsExactly(resourcePermission);
    verify(innerFunction).getRequiredPermissions(eq("foo"), eq(new String[] {"bar"}));
  }

  private Timer successTimer() {
    return functionExecutionsTimer(true);
  }

  private Timer failureTimer() {
    return functionExecutionsTimer(false);
  }

  private Timer functionExecutionsTimer(boolean succeededTagValue) {
    return meterRegistry
        .find("geode.function.executions")
        .tag("function", INNER_FUNCTION_ID)
        .tag("succeeded", String.valueOf(succeededTagValue))
        .timer();
  }
}
