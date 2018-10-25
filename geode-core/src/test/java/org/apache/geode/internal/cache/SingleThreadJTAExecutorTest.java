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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.CancelCriterion;

public class SingleThreadJTAExecutorTest {
  private SingleThreadJTAExecutor singleThreadJTAExecutor;
  private TXState txState;
  private ExecutorService executor;
  private BeforeCompletion beforeCompletion;
  private AfterCompletion afterCompletion;
  private CancelCriterion cancelCriterion;

  @Before
  public void setup() {
    txState = mock(TXState.class, RETURNS_DEEP_STUBS);
    executor = Executors.newSingleThreadExecutor();
    beforeCompletion = mock(BeforeCompletion.class);
    afterCompletion = mock(AfterCompletion.class);
    cancelCriterion = mock(CancelCriterion.class);
    singleThreadJTAExecutor = new SingleThreadJTAExecutor(beforeCompletion, afterCompletion);
  }

  @Test
  public void executeBeforeCompletionCallsDoOps() {
    InOrder inOrder = inOrder(beforeCompletion, afterCompletion);

    singleThreadJTAExecutor.executeBeforeCompletion(txState, executor, cancelCriterion);

    verify(beforeCompletion, times(1)).execute(eq(cancelCriterion));
    await()
        .untilAsserted(() -> inOrder.verify(beforeCompletion, times(1)).doOp(eq(txState)));
    await().untilAsserted(
        () -> inOrder.verify(afterCompletion, times(1)).doOp(eq(txState), eq(cancelCriterion)));
  }

  @Test
  public void cleanupInvokesCancel() {
    singleThreadJTAExecutor.cleanup();

    verify(afterCompletion, times(1)).cancel();
  }

  @Test(expected = RuntimeException.class)
  public void doOpsInvokesAfterCompletionDoOpWhenBeforeCompletionThrows() {
    doThrow(RuntimeException.class).when(beforeCompletion).doOp(txState);

    singleThreadJTAExecutor.doOps(txState, cancelCriterion);

    verify(afterCompletion, times(1)).doOp(eq(txState), eq(cancelCriterion));
  }

}
