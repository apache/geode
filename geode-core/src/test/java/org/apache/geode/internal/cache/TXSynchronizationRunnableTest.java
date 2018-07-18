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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.cache.tier.sockets.CommBufferPool;

public class TXSynchronizationRunnableTest {

  private CancelCriterion cancelCriterion;
  private CommBufferPool commBufferPool;
  private Runnable beforeCompletion;
  private CacheClosedException exception;


  @Before
  public void setUp() {
    exception = new CacheClosedException();

    cancelCriterion = mock(CancelCriterion.class);
    commBufferPool = mock(CommBufferPool.class);
    beforeCompletion = mock(Runnable.class);

    doThrow(exception).when(cancelCriterion).checkCancelInProgress(any());
  }

  @Test
  public void test() {
    TXSynchronizationRunnable runnable =
        new TXSynchronizationRunnable(cancelCriterion, commBufferPool, beforeCompletion);
    assertThatThrownBy(() -> runnable.waitForFirstExecution()).isSameAs(exception);
  }

  @Test
  public void test1() {
    TXSynchronizationRunnable runnable =
        new TXSynchronizationRunnable(cancelCriterion, commBufferPool, beforeCompletion);
    assertThatThrownBy(() -> runnable.runSecondRunnable(mock(Runnable.class))).isSameAs(exception);
  }
}
