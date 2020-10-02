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

import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class RetryTest {

  private static class AutoIncrementingClock implements Retry.Clock {
    private final AtomicLong clock = new AtomicLong();

    @Override
    public long nanoTime() {
      return clock.incrementAndGet();
    }
  }

  @Test
  public void tryForReturnsImmediatelyOnPredicateMatch() throws TimeoutException {
    final AutoIncrementingClock clock = new AutoIncrementingClock();
    final Integer value = Retry.tryFor(() -> 10, (v) -> v == 10, 1, NANOSECONDS, clock);
    assertThat(value).isEqualTo(10);
    assertThat(clock.nanoTime()).isEqualTo(2);
  }

  @Test
  public void tryForReturnsAfterRetries() throws TimeoutException {
    final AutoIncrementingClock clock = new AutoIncrementingClock();
    final AtomicInteger shared = new AtomicInteger();
    final Integer value =
        Retry.tryFor(shared::getAndIncrement, (v) -> v == 10, 11, NANOSECONDS, clock);
    assertThat(value).isEqualTo(10);
    assertThat(clock.nanoTime()).isEqualTo(12);
  }

  @Test
  public void tryForThrowsAfterTimeout() {
    final AutoIncrementingClock clock = new AutoIncrementingClock();
    assertThatThrownBy(() -> Retry.tryFor(() -> null, Objects::nonNull, 1, NANOSECONDS, clock))
        .isInstanceOf(TimeoutException.class);
    assertThat(clock.nanoTime()).isEqualTo(3);
  }

}
