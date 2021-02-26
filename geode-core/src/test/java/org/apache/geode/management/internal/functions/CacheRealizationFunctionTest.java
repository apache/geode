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

package org.apache.geode.management.internal.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import com.healthmarketscience.rmiio.RemoteInputStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.internal.CacheElementOperation;

public class CacheRealizationFunctionTest {

  private CacheRealizationFunction function;
  private FunctionContext<List> context;
  private AbstractConfiguration config;
  private CacheElementOperation operation;
  private InternalCache cache;
  private List arguments;
  private ResultSender resultSender;
  private RemoteInputStream inputStream;

  @Before
  public void before() throws Exception {
    function = spy(CacheRealizationFunction.class);
    context = mock(FunctionContext.class);
    when(context.getMemberName()).thenReturn("testName");
    arguments = new ArrayList();
    when(context.getArguments()).thenReturn(arguments);
    config = mock(AbstractConfiguration.class);
    arguments.add(config);
    resultSender = mock(ResultSender.class);
    when(context.getResultSender()).thenReturn(resultSender);
    cache = mock(InternalCache.class);
    inputStream = mock(RemoteInputStream.class);
  }

  @Test
  public void GetWithCacheClosed() throws Exception {
    operation = CacheElementOperation.GET;
    arguments.add(operation);
    when(context.getCache()).thenThrow(CacheClosedException.class);

    function.execute(context);
    verify(function, never()).logError(any(), any());
    verify(resultSender).lastResult(null);
  }

  @Test
  public void GetWithOtherException() throws Exception {
    operation = CacheElementOperation.GET;
    arguments.add(operation);
    when(context.getCache()).thenReturn(cache);
    doThrow(RuntimeException.class).when(function).executeGet(context, cache, config);

    function.execute(context);
    verify(function).logError(any(), any());
    verify(resultSender).lastResult(null);
  }

  @Test
  public void CreateWithCacheClosed() throws Exception {
    operation = CacheElementOperation.CREATE;
    arguments.add(operation);
    arguments.add(inputStream);
    when(context.getCache()).thenThrow(CacheClosedException.class);

    function.execute(context);
    verify(function, never()).logError(any(), any());
    ArgumentCaptor<RealizationResult> argumentCaptor =
        ArgumentCaptor.forClass(RealizationResult.class);
    verify(resultSender).lastResult(argumentCaptor.capture());
    RealizationResult result = argumentCaptor.getValue();
    // since cache is already closed, no way to get the membername
    // in the result
    assertThat(result.getMemberName()).isNull();
  }

  @Test
  public void CreateWithOtherException() throws Exception {
    operation = CacheElementOperation.CREATE;
    arguments.add(operation);
    arguments.add(inputStream);
    when(context.getCache()).thenReturn(cache);

    doThrow(RuntimeException.class).when(function).executeUpdate("testName", cache, config,
        operation,
        inputStream);

    function.execute(context);
    verify(function).logError(any(), any());
    ArgumentCaptor<RealizationResult> argumentCaptor =
        ArgumentCaptor.forClass(RealizationResult.class);
    verify(resultSender).lastResult(argumentCaptor.capture());
    RealizationResult result = argumentCaptor.getValue();
    assertThat(result.getMemberName()).isEqualTo("testName");
  }

}
