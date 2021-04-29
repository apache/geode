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

package org.apache.geode.internal;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

public class RetryTest {
  Retry.Timer timer;

  @Before
  public void before() throws Exception {
    AtomicLong atomicLong = new AtomicLong();
    timer = mock(Retry.Timer.class);
    when(timer.nanoTime()).thenReturn(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
  }

  @Test
  public void tryForReturnsImmediatelyOnPredicateMatch()
      throws TimeoutException, InterruptedException {
    final Integer value =
        Retry.tryFor(1, NANOSECONDS, 1, NANOSECONDS, () -> 10, (v) -> v == 10, timer);
    assertThat(value).isEqualTo(10);
    // nanoTime is only called one time if predicate match immediately
    verify(timer, times(1)).nanoTime();
    // sleep is never called if predicate matches immediately
    verify(timer, never()).sleep(anyLong());
  }

  @Test
  public void tryForReturnsAfterRetries() throws TimeoutException, InterruptedException {
    final AtomicInteger shared = new AtomicInteger();
    final Integer value =
        Retry.tryFor(10, NANOSECONDS, 1, NANOSECONDS, shared::getAndIncrement, (v) -> v == 3,
            timer);
    assertThat(value).isEqualTo(3);
    verify(timer, times(4)).nanoTime();
    verify(timer, times(3)).sleep(1L);
  }

  @Test
  public void tryForThrowsAfterTimeout() throws InterruptedException {
    assertThatThrownBy(
        () -> Retry.tryFor(3, NANOSECONDS, 1, NANOSECONDS, () -> null, Objects::nonNull, timer))
            .isInstanceOf(TimeoutException.class);
    verify(timer, times(3)).nanoTime();
    verify(timer, times(1)).sleep(1L);
  }

  @Test
  public void timerSleepCanTakeNegativeArgument() throws Exception {
    Retry.SteadyTimer steadyTimer = new Retry.SteadyTimer();
    assertThatNoException().isThrownBy(() -> steadyTimer.sleep(-2));
  }

  @Test
  public void lastIterationSleepForLessThanIntervalTime() throws Exception {
    assertThatThrownBy(
        () -> Retry.tryFor(2, NANOSECONDS, 3, NANOSECONDS, () -> null, Objects::nonNull, timer))
            .isInstanceOf(TimeoutException.class);
    verify(timer, times(2)).nanoTime();
    verify(timer, never()).sleep(anyLong());
  }
}
