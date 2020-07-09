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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.control.RestoreRedundancyOperation;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.management.runtime.RestoreRedundancyResults;

public class RestoreRedundancyFunctionTest {
  @SuppressWarnings("unchecked")
  private final FunctionContext<Object[]> mockContext = mock(FunctionContext.class);
  private final Cache mockCache = mock(Cache.class, RETURNS_DEEP_STUBS);
  private final RestoreRedundancyOperation mockOperation =
      mock(RestoreRedundancyOperation.class, RETURNS_DEEP_STUBS);
  private final SerializableRestoreRedundancyResultsImpl mockResults =
      mock(SerializableRestoreRedundancyResultsImpl.class);
  private final String message = "expected message";
  private RestoreRedundancyFunction function;
  private ResultSender<RestoreRedundancyResults> resultSender;
  private ArgumentCaptor<SerializableRestoreRedundancyResultsImpl> argumentCaptor;
  private RestoreRedundancyRequest request;

  @Before
  public void setUp() throws InterruptedException, ExecutionException {
    function = new RestoreRedundancyFunction();
    when(mockContext.getCache()).thenReturn(mockCache);
    request = new RestoreRedundancyRequest();

    when(mockContext.getArguments()).thenReturn(new Object[] {request, false});
    when(mockCache.getResourceManager().createRestoreRedundancyOperation())
        .thenReturn(mockOperation);
    CompletableFuture<RestoreRedundancyResults> future =
        CompletableFuture.completedFuture(mockResults);
    when(mockOperation.start()).thenReturn(future);
    when(mockResults.getRegionOperationMessage()).thenReturn(message);
    resultSender = mock(ResultSender.class);
    when(mockContext.getResultSender()).thenReturn((ResultSender) resultSender);
    argumentCaptor = ArgumentCaptor.forClass(SerializableRestoreRedundancyResultsImpl.class);
  }

  @Test
  public void executeFunctionSetsFieldsOnRestoreRedundancyOperation() {
    String[] includeRegions = {"includedRegion1", "includedRegion2"};
    String[] excludeRegions = {"excludedRegion1", "excludedRegion2"};
    request.setExcludeRegions(Arrays.asList(excludeRegions));
    request.setIncludeRegions(Arrays.asList(includeRegions));

    function.execute(mockContext);

    verify(mockOperation).includeRegions(new HashSet<>(request.getIncludeRegions()));
    verify(mockOperation).excludeRegions(new HashSet<>(request.getExcludeRegions()));
    verify(mockOperation).shouldReassignPrimaries(request.getReassignPrimaries());
  }

  @Test
  public void executeFunctionSetsIncludedAndExcludedRegionsOnRestoreRedundancyOperationWhenNull() {
    function.execute(mockContext);

    verify(mockOperation).includeRegions(null);
    verify(mockOperation).excludeRegions(null);
    verify(mockOperation).shouldReassignPrimaries(true);
  }

  @Test
  public void executeFunctionUsesStatusMethodWhenIsStatusCommandIsTrue() {
    when(mockOperation.redundancyStatus()).thenReturn(mockResults);
    when(mockResults.getRegionOperationStatus())
        .thenReturn(RestoreRedundancyResults.Status.SUCCESS);
    // isStatusCommand is the second argument passed to the function
    when(mockContext.getArguments()).thenReturn(new Object[] {request, true});

    function.execute(mockContext);

    verify(mockOperation, times(1)).redundancyStatus();
    verify(mockOperation, times(0)).start();
  }

  @Test
  // The function was able to execute successfully but redundancy was not able to be established for
  // at least one region
  public void executeFunctionReturnsOkWhenResultStatusIsFailure() {
    when(mockResults.getRegionOperationStatus())
        .thenReturn(RestoreRedundancyResults.Status.FAILURE);
    function.execute(mockContext);
    verify(resultSender).lastResult(argumentCaptor.capture());

    SerializableRestoreRedundancyResultsImpl result = argumentCaptor.getValue();
    verify(result).setSuccess(true);
    assertThat(result.getRegionOperationStatus())
        .isEqualTo(RestoreRedundancyResults.Status.FAILURE);
    assertThat(result).isSameAs(mockResults);
  }

  @Test
  public void executeFunctionReturnsOkWhenResultStatusIsSuccess() {
    when(mockResults.getRegionOperationStatus())
        .thenReturn(RestoreRedundancyResults.Status.SUCCESS);
    function.execute(mockContext);
    verify(resultSender).lastResult(argumentCaptor.capture());

    SerializableRestoreRedundancyResultsImpl result = argumentCaptor.getValue();
    verify(result).setSuccess(true);
    assertThat(result.getRegionOperationStatus())
        .isEqualTo(RestoreRedundancyResults.Status.SUCCESS);
    assertThat(result).isSameAs(mockResults);
  }

  @Test
  public void executeFunctionReturnsFailureWhenExceptionThrownDuringOperation() {
    when(mockOperation.start()).thenThrow(new RuntimeException("Any exception"));
    function.execute(mockContext);
    verify(resultSender).lastResult(argumentCaptor.capture());
    RestoreRedundancyResults result = argumentCaptor.getValue();
    assertThat(result.getSuccess()).isFalse();
    assertThat(result.getStatusMessage()).isEqualTo("Any exception");
  }
}
