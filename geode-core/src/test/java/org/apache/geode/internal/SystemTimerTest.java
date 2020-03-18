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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.DistributedSystem;

public class SystemTimerTest {

  private DistributedSystem distributedSystem;
  private SystemTimer systemTimer;

  @Before
  public void setup() {
    this.distributedSystem = mock(DistributedSystem.class);
    this.systemTimer = new SystemTimer(distributedSystem);
  }

  @After
  public void teardown() {
    if (!systemTimer.isCancelled()) {
      systemTimer.cancel();
    }
  }

  @Test
  public void cancelTimer() {
    assertThat(systemTimer.isCancelled()).isFalse();
    int initialSystemCount = SystemTimer.distributedSystemCount();
    SystemTimer.cancelTimers(distributedSystem);
    assertThat(systemTimer.isCancelled()).isTrue();
    assertThat(SystemTimer.distributedSystemCount()).isEqualTo(initialSystemCount - 1);
  }

  @Test
  public void cancel() {
    assertThat(systemTimer.isCancelled()).isFalse();
    systemTimer.cancel();
    assertThat(systemTimer.isCancelled()).isTrue();
  }

  @Test
  public void scheduleNow() {
    AtomicBoolean hasRun = new AtomicBoolean(false);
    SystemTimer.SystemTimerTask task = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        hasRun.set(true);
      }
    };
    systemTimer.schedule(task, 0);
    await().until(() -> hasRun.get());
  }

  @Test
  public void scheduleWithDelay() {
    AtomicBoolean hasRun = new AtomicBoolean(false);
    SystemTimer.SystemTimerTask task = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        hasRun.set(true);
      }
    };
    final long millis = System.currentTimeMillis();
    final int delay = 1000;
    systemTimer.schedule(task, delay);
    await().until(() -> hasRun.get());
    assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(millis + delay);
  }

  @Test
  public void scheduleWithDate() {
    AtomicBoolean hasRun = new AtomicBoolean(false);
    SystemTimer.SystemTimerTask task = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        hasRun.set(true);
      }
    };
    final long millis = System.currentTimeMillis();
    final long delay = 1000;
    final Date scheduleTime = new Date(System.currentTimeMillis() + delay);
    systemTimer.schedule(task, scheduleTime);
    await().until(() -> hasRun.get());
    assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(millis + delay);
  }

  @Test
  public void scheduleRepeatedWithDelay() {
    AtomicInteger invocations = new AtomicInteger(0);
    SystemTimer.SystemTimerTask task = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        invocations.incrementAndGet();
      }
    };
    final long millis = System.currentTimeMillis();
    final int delay = 1000;
    final int period = 500;
    systemTimer.schedule(task, delay, period);
    await().untilAsserted(() -> assertThat(invocations.get()).isGreaterThanOrEqualTo(2));
    assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(millis + delay + period);
  }

  @Test
  public void scheduleAtFixedRate() {
    AtomicInteger invocations = new AtomicInteger(0);
    SystemTimer.SystemTimerTask task = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        invocations.incrementAndGet();
      }
    };
    final long millis = System.currentTimeMillis();
    final int delay = 1000;
    final int period = 500;
    systemTimer.scheduleAtFixedRate(task, delay, period);
    await().untilAsserted(() -> assertThat(invocations.get()).isGreaterThanOrEqualTo(2));
    assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(millis + delay + period);
  }

  @Test
  public void cancelTask() {
    AtomicInteger invocations = new AtomicInteger(0);
    SystemTimer.SystemTimerTask task = new SystemTimer.SystemTimerTask() {
      @Override
      public void run2() {
        invocations.incrementAndGet();
      }
    };
    assertThat(task.isCancelled()).isFalse();
    task.cancel();
    assertThat(task.isCancelled()).isTrue();
    assertThatThrownBy(() -> systemTimer.schedule(task, 0))
        .isInstanceOf(IllegalStateException.class);
  }

}
