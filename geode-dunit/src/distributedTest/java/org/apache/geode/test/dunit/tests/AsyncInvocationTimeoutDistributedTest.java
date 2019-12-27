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
package org.apache.geode.test.dunit.tests;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.internal.StackTrace;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class AsyncInvocationTimeoutDistributedTest implements Serializable {

  private static final long TIMEOUT_MILLIS = getTimeout().toMillis();

  private static final AtomicReference<Long> threadId = new AtomicReference<>();
  private static final AtomicReference<CountDownLatch> latch = new AtomicReference<>();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @After
  public void tearDown() {
    getVM(0).invoke(() -> {
      CountDownLatch latchInVM0 = latch.get();
      while (latchInVM0 != null && latchInVM0.getCount() > 0) {
        latchInVM0.countDown();
      }
    });
  }

  @Test
  public void await_runnable_timeout_includesStackTraceAsCause() {
    AsyncInvocation<Void> hangInVM0 = getVM(0).invokeAsync(() -> {
      latch.set(new CountDownLatch(1));
      threadId.set(Thread.currentThread().getId());
      latch.get().await(TIMEOUT_MILLIS, MILLISECONDS);
    });

    long remoteThreadId = getVM(0).invoke(() -> {
      await().until(() -> threadId.get() > 0);
      return threadId.get();
    });

    Throwable thrown = catchThrowable(() -> hangInVM0.await(1, SECONDS));
    assertThat(thrown)
        .isInstanceOf(TimeoutException.class);
    Throwable cause = thrown.getCause();
    assertThat(cause)
        .isInstanceOf(StackTrace.class)
        .hasMessage("Stack trace for vm-0 thread-" + remoteThreadId);
  }

  @Test
  public void await_callable_timeout_includesStackTraceAsCause() {
    AsyncInvocation<Integer> hangInVM0 = getVM(0).invokeAsync(() -> {
      latch.set(new CountDownLatch(1));
      threadId.set(Thread.currentThread().getId());
      latch.get().await(TIMEOUT_MILLIS, MILLISECONDS);
      return 42;
    });

    long remoteThreadId = getVM(0).invoke(() -> {
      await().until(() -> threadId.get() > 0);
      return threadId.get();
    });

    Throwable thrown = catchThrowable(() -> hangInVM0.await(1, SECONDS));
    assertThat(thrown)
        .isInstanceOf(TimeoutException.class);
    Throwable cause = thrown.getCause();
    assertThat(cause)
        .isInstanceOf(StackTrace.class)
        .hasMessage("Stack trace for vm-0 thread-" + remoteThreadId);
  }

  @Test
  public void get_callable_timeout_includesStackTraceAsCause() {
    AsyncInvocation<Integer> hangInVM0 = getVM(0).invokeAsync(() -> {
      latch.set(new CountDownLatch(1));
      threadId.set(Thread.currentThread().getId());
      latch.get().await(TIMEOUT_MILLIS, MILLISECONDS);
      return 42;
    });

    long remoteThreadId = getVM(0).invoke(() -> {
      await().until(() -> threadId.get() > 0);
      return threadId.get();
    });

    Throwable thrown = catchThrowable(() -> hangInVM0.get(1, SECONDS));
    assertThat(thrown)
        .isInstanceOf(TimeoutException.class);
    Throwable cause = thrown.getCause();
    assertThat(cause)
        .isInstanceOf(StackTrace.class)
        .hasMessage("Stack trace for vm-0 thread-" + remoteThreadId);
  }
}
