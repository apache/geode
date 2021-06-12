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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.internal.StackTrace;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class AsyncInvocationTimeoutDistributedTest implements Serializable {

  private static final AtomicReference<Long> THREAD_ID =
      new AtomicReference<>(0L);
  private static final AtomicReference<CountDownLatch> LATCH =
      new AtomicReference<>(new CountDownLatch(0));

  @Rule
  public DistributedRule distributedRule = new DistributedRule(1);

  @Before
  public void setUp() {
    getVM(0).invoke(() -> {
      LATCH.set(new CountDownLatch(1));
      THREAD_ID.set(0L);
    });
  }

  @After
  public void tearDown() {
    getVM(0).invoke(() -> LATCH.get().countDown());
  }

  @Test
  public void awaitWithRunnableTimeoutExceptionIncludesRemoteStackTraceAsCause() {
    AsyncInvocation<Void> hangInVM0 = getVM(0).invokeAsync(() -> {
      THREAD_ID.set(Thread.currentThread().getId());
      LATCH.get().await(getTimeout().toMillis(), MILLISECONDS);
    });

    long remoteThreadId = getVM(0).invoke(() -> {
      await().until(() -> THREAD_ID.get() > 0);
      return THREAD_ID.get();
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
  public void awaitWithCallableTimeoutExceptionIncludesRemoteStackTraceAsCause() {
    AsyncInvocation<Integer> hangInVM0 = getVM(0).invokeAsync(() -> {
      THREAD_ID.set(Thread.currentThread().getId());
      LATCH.get().await(getTimeout().toMillis(), MILLISECONDS);
      return 42;
    });

    long remoteThreadId = getVM(0).invoke(() -> {
      await().until(() -> THREAD_ID.get() > 0);
      return THREAD_ID.get();
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
  public void getWithCallableTimeoutExceptionIncludesRemoteStackTraceAsCause() {
    AsyncInvocation<Integer> hangInVM0 = getVM(0).invokeAsync(() -> {
      THREAD_ID.set(Thread.currentThread().getId());
      LATCH.get().await(getTimeout().toMillis(), MILLISECONDS);
      return 42;
    });

    long remoteThreadId = getVM(0).invoke(() -> {
      await().until(() -> THREAD_ID.get() > 0);
      return THREAD_ID.get();
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
