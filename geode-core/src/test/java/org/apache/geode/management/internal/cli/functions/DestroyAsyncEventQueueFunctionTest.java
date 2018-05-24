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
package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.cli.commands.DestroyAsyncEventQueueCommand;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class DestroyAsyncEventQueueFunctionTest {

  private static final String TEST_AEQ_ID = "Test-AEQ";
  private AsyncEventQueueImpl mockAEQ;
  private FunctionContext<String> mockContext;
  private GemFireCacheImpl cache;
  private ResultSender<CliFunctionResult> resultSender;
  private ArgumentCaptor<CliFunctionResult> resultCaptor;
  private DestroyAsyncEventQueueFunction function;

  @Before
  public void setUp() throws Exception {
    mockAEQ = mock(AsyncEventQueueImpl.class);
    mockContext = mock(FunctionContext.class);
    cache = Fakes.cache();
    function = spy(DestroyAsyncEventQueueFunction.class);
    resultSender = mock(ResultSender.class);

    when(mockContext.getCache()).thenReturn(cache);
    when(mockContext.getArguments()).thenReturn(TEST_AEQ_ID);
    when(mockAEQ.getId()).thenReturn(TEST_AEQ_ID);
    when(mockContext.<CliFunctionResult>getResultSender()).thenReturn(resultSender);
    resultCaptor = ArgumentCaptor.forClass(CliFunctionResult.class);
  }

  @Test
  public void validIdReturnsOK() {
    when(cache.getAsyncEventQueue(TEST_AEQ_ID)).thenReturn(mockAEQ);

    function.execute(mockContext);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.isIgnorableFailure()).isFalse();
    assertThat(result.getStatusMessage()).containsPattern(String.format(
        DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_DESTROYED, TEST_AEQ_ID));

  }

  @Test
  public void nonexistentReturnsIgnorable() {
    when(cache.getAsyncEventQueue(TEST_AEQ_ID)).thenReturn(null);

    function.execute(mockContext);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.isIgnorableFailure()).isTrue();
    assertThat(result.getStatusMessage()).containsPattern(String.format(
        DestroyAsyncEventQueueCommand.DESTROY_ASYNC_EVENT_QUEUE__AEQ_0_NOT_FOUND, TEST_AEQ_ID));
  }

  @Test
  public void functionErrorReturnsError() {
    String mockExceptionMessage = "Some test error during shutdown occurred.";
    when(cache.getAsyncEventQueue(TEST_AEQ_ID)).thenReturn(mockAEQ);
    doThrow(new RuntimeException(mockExceptionMessage)).when(mockAEQ).stop();

    function.execute(mockContext);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.isIgnorableFailure()).isFalse();
    assertThat(result.getStatusMessage()).contains(mockExceptionMessage);
  }
}
