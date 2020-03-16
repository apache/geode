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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.fake.Fakes;

public class DestroyAsyncEventQueueFunctionTest {

  private static final String TEST_AEQ_ID = "Test-AEQ";
  private AsyncEventQueue mockAEQ;
  private FunctionContext<DestroyAsyncEventQueueFunctionArgs> mockContext;
  private DestroyAsyncEventQueueFunctionArgs mockArgs;
  private GemFireCacheImpl cache;
  private ResultSender<Object> resultSender;
  private ArgumentCaptor<CliFunctionResult> resultCaptor;
  private DestroyAsyncEventQueueFunction function;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    mockAEQ = mock(AsyncEventQueueImpl.class);
    mockContext = mock(FunctionContext.class);
    mockArgs = mock(DestroyAsyncEventQueueFunctionArgs.class);
    cache = Fakes.cache();
    function = spy(DestroyAsyncEventQueueFunction.class);
    resultSender = mock(ResultSender.class);

    when(mockContext.getCache()).thenReturn(cache);
    when(mockContext.getArguments()).thenReturn(mockArgs);
    when(mockArgs.getId()).thenReturn(TEST_AEQ_ID);
    when(mockAEQ.getId()).thenReturn(TEST_AEQ_ID);
    when(mockContext.getResultSender()).thenReturn(resultSender);
    resultCaptor = ArgumentCaptor.forClass(CliFunctionResult.class);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void execute_validAeqId_OK() {
    XmlEntity xmlEntity = mock(XmlEntity.class);
    doReturn(xmlEntity).when(function).getAEQXmlEntity(anyString(), anyString());
    when(cache.getAsyncEventQueue(TEST_AEQ_ID)).thenReturn(mockAEQ);

    function.execute(mockContext);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getXmlEntity()).isNotNull();
    assertThat(result.getThrowable()).isNull();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void execute_nonexistentAeqId_returnsError() {
    when(cache.getAsyncEventQueue(TEST_AEQ_ID)).thenReturn(null);

    function.execute(mockContext);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getMessage()).containsPattern(TEST_AEQ_ID + ".*not found");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void execute_nonexistentAeqIdIfExists_returnsSuccess() {
    when(cache.getAsyncEventQueue(TEST_AEQ_ID)).thenReturn(null);
    when(mockArgs.isIfExists()).thenReturn(true);

    function.execute(mockContext);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMessage()).containsPattern("Skipping:.*" + TEST_AEQ_ID + ".*not found");
  }
}
