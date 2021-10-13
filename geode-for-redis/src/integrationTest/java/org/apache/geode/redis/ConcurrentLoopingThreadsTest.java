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

package org.apache.geode.redis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class ConcurrentLoopingThreadsTest {

  @Test
  public void withIterations_actionDoesNotRunWhenThreadsThrowExceptions() {
    AtomicBoolean actionRan = new AtomicBoolean(false);
    AtomicInteger count = new AtomicInteger(0);

    assertThatThrownBy(() -> new ConcurrentLoopingThreads(10,
        i -> count.incrementAndGet(),
        i -> {
          throw new RuntimeException("BANG");
        })
            .runWithAction(() -> actionRan.set(true)))
                .isInstanceOf(RuntimeException.class);

    assertThat(count.get()).as("Expecting good thread to run once").isEqualTo(1);
    assertThat(actionRan.get()).as("action should not run").isFalse();
  }

  @Test
  public void withSignal_actionDoesNotRunWhenThreadsThrowExceptions() {
    AtomicBoolean actionRan = new AtomicBoolean(false);
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicInteger count = new AtomicInteger(0);

    assertThatThrownBy(() -> new ConcurrentLoopingThreads(running,
        i -> count.incrementAndGet(),
        i -> {
          throw new RuntimeException("BANG");
        })
            .runWithAction(() -> actionRan.set(true)))
                .isInstanceOf(RuntimeException.class);

    assertThat(count.get()).as("Expecting good thread to run once").isEqualTo(1);
    assertThat(actionRan.get()).as("action should not run").isFalse();
  }

  @Test
  public void withIterations_threadFailure_shouldStopAllOtherThreads() {
    AtomicInteger count = new AtomicInteger(0);

    assertThatThrownBy(() -> new ConcurrentLoopingThreads(10,
        i -> count.incrementAndGet(),
        i -> {
          throw new RuntimeException("BANG");
        }).runInLockstep());

    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  public void withIterations_failingThreadExceptionIsPropagatedBeforeActionException() {
    AtomicInteger count = new AtomicInteger(0);

    assertThatThrownBy(() -> new ConcurrentLoopingThreads(10,
        i -> count.incrementAndGet(),
        i -> {
          throw new RuntimeException("Thread BANG");
        }).runWithAction(() -> {
          throw new RuntimeException("Action BANG");
        })).hasMessageContaining("Thread BANG");

    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  public void withSignal_threadFailure_shouldStopAllOtherThreads() {
    AtomicInteger count = new AtomicInteger(0);
    AtomicBoolean running = new AtomicBoolean(true);

    assertThatThrownBy(() -> new ConcurrentLoopingThreads(running,
        i -> count.incrementAndGet(),
        i -> {
          throw new RuntimeException("BANG");
        }).runInLockstep());

    assertThat(count.get()).isEqualTo(1);
  }

  @Test
  public void withIterations_actionFailure_shouldStopImmediately() {
    AtomicInteger count = new AtomicInteger(0);

    assertThatThrownBy(() -> new ConcurrentLoopingThreads(10,
        i -> count.incrementAndGet(),
        i -> count.incrementAndGet())
            .runWithAction(() -> {
              throw new RuntimeException("BANG");
            }))
                .isInstanceOf(RuntimeException.class);

    assertThat(count.get()).isEqualTo(2);
  }

  @Test
  public void withSignal_actionFailure_shouldStopImmediately() {
    AtomicInteger count = new AtomicInteger(0);
    AtomicBoolean running = new AtomicBoolean(true);

    assertThatThrownBy(() -> new ConcurrentLoopingThreads(running,
        i -> count.incrementAndGet(),
        i -> count.incrementAndGet())
            .runWithAction(() -> {
              throw new RuntimeException("BANG");
            }))
                .isInstanceOf(RuntimeException.class);

    assertThat(count.get()).isEqualTo(2);
  }

  @Test
  public void withSignal_actionWillAlwaysRun() {
    AtomicInteger count = new AtomicInteger(0);
    AtomicBoolean running = new AtomicBoolean(true);

    new ConcurrentLoopingThreads(running,
        i -> running.set(false))
            .runWithAction(count::incrementAndGet);

    assertThat(count.get()).isEqualTo(1);
  }

}
