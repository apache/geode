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
package org.apache.geode.test.dunit.examples;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class AsyncInvokeCallableExampleTest {

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Test
  public void invokeAsyncAsFuture() throws Exception {
    VM workerVM = getVM(0);
    boolean success = workerVM.invokeAsync(() -> longRunningWorkWithResult()).get();
    assertThat(success).isTrue();
  }

  @Test
  public void invokeAsyncAsFutureWithTimeout() throws Exception {
    VM workerVM = getVM(0);
    boolean success =
        workerVM.invokeAsync(() -> longRunningWorkWithResult()).get(1, TimeUnit.MINUTES);
    assertThat(success).isTrue();
  }

  @Test
  public void invokeAsyncWithExceptionOccurred() throws Exception {
    VM workerVM = getVM(0);

    AsyncInvocation<Boolean> asyncInvocation =
        workerVM.invokeAsync(() -> longRunningWorkThatThrowsException());
    asyncInvocation.join();

    assertThat(asyncInvocation.exceptionOccurred()).isTrue();
    assertThat(asyncInvocation.getException()).isInstanceOf(Exception.class).hasMessage("failed");
  }

  /**
   * {@link VM#invokeAsync} uses {@link AsyncInvocation} which wraps underlying Exception in
   * AssertionError.
   */
  @Test(expected = AssertionError.class)
  public void invokeAsyncWithAwait() throws Exception {
    getVM(0).invokeAsync(() -> longRunningWorkThatThrowsException()).await();
  }

  /**
   * {@link VM#invokeAsync} uses {@link AsyncInvocation} which wraps underlying Exception in
   * AssertionError.
   */
  @Test(expected = AssertionError.class)
  public void invokeAsyncWithAwaitWithTimeout() throws Exception {
    getVM(0).invokeAsync(() -> longRunningWorkThatThrowsException()).await(1, TimeUnit.MINUTES);
  }

  private static boolean longRunningWorkWithResult() {
    // perform some task that takes a while
    return true;
  }

  private static boolean longRunningWorkThatThrowsException() throws Exception {
    throw new Exception("failed");
  }
}
