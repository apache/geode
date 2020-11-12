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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
  Retry.Clock clock;

  @Before
  public void before() throws Exception {
    AtomicLong atomicLong = new AtomicLong();
    clock = mock(Retry.Clock.class);
    when(clock.nanoTime()).thenReturn(1L, 2L, 3L);
  }

  @Test
  public void tryForReturnsImmediatelyOnPredicateMatch()
      throws TimeoutException, InterruptedException {
    final Integer value = Retry.tryFor(1, 1, NANOSECONDS, () -> 10, (v) -> v == 10, clock);
    assertThat(value).isEqualTo(10);
    // nanoTime is only called one time if predicate match immediately
    verify(clock, times(1)).nanoTime();
    // sleep is never called if predicate matches immediately
    verify(clock, never()).sleep(1, NANOSECONDS);
  }

  @Test
  public void tryForReturnsAfterRetries() throws TimeoutException, InterruptedException {
    final AtomicInteger shared = new AtomicInteger();
    final Integer value =
        Retry.tryFor(3, 1, NANOSECONDS, shared::getAndIncrement, (v) -> v == 3, clock);
    assertThat(value).isEqualTo(3);
    verify(clock, times(4)).nanoTime();
    verify(clock, times(3)).sleep(1, NANOSECONDS);
  }

  @Test
  public void tryForThrowsAfterTimeout() throws InterruptedException {
    assertThatThrownBy(() -> Retry.tryFor(1, 1, NANOSECONDS, () -> null, Objects::nonNull, clock))
        .isInstanceOf(TimeoutException.class);
    verify(clock, times(2)).nanoTime();
    verify(clock, times(1)).sleep(1, NANOSECONDS);
  }
}
