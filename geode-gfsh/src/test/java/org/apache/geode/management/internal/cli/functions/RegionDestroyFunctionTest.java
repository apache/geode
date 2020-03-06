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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.management.internal.functions.CliFunctionResult;


public class RegionDestroyFunctionTest {

  private RegionDestroyFunction function;
  private FunctionContext<String> context;
  private InternalCacheForClientAccess filterCache;
  private ResultSender<Object> resultSender;
  private ArgumentCaptor<CliFunctionResult> resultCaptor;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    function = spy(RegionDestroyFunction.class);
    context = mock(FunctionContext.class);
    InternalCache cache = mock(InternalCache.class);
    filterCache = mock(InternalCacheForClientAccess.class);
    resultSender = mock(ResultSender.class);
    when(context.getCache()).thenReturn(cache);
    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getArguments()).thenReturn("testRegion");
    when(cache.getCacheForProcessingClientRequests()).thenReturn(filterCache);
    resultCaptor = ArgumentCaptor.forClass(CliFunctionResult.class);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void functionContextIsWrong() {
    function.execute(context);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getMessage()).contains("Function Id mismatch or arguments is not available");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void regionAlreadyDestroyed() {
    when(context.getFunctionId()).thenReturn(RegionDestroyFunction.class.getName());
    when(filterCache.getRegion(any())).thenReturn(null);
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).contains("Region 'testRegion' already destroyed");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void regionAlreadyDestroyed_throwException() {
    when(context.getFunctionId()).thenReturn(RegionDestroyFunction.class.getName());
    @SuppressWarnings("unchecked")
    Region<Object, Object> region = mock(Region.class);
    when(filterCache.getRegion(any())).thenReturn(region);
    doThrow(mock(RegionDestroyedException.class)).when(region).destroyRegion();
    function.execute(context);

    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).contains("Region 'testRegion' already destroyed");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void illegalStateExceptionWillNotThrowExceptionToCommand() {
    when(context.getFunctionId()).thenReturn(RegionDestroyFunction.class.getName());
    @SuppressWarnings("unchecked")
    Region<Object, Object> region = mock(Region.class);
    when(filterCache.getRegion(any())).thenReturn(region);
    doThrow(new IllegalStateException("message")).when(region).destroyRegion();

    function.execute(context);
    verify(resultSender).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    // will not populate the exception in the result, but only preserve the message
    assertThat(result.getThrowable()).isNull();
    assertThat(result.getMessage()).isEqualTo("message");
  }
}
