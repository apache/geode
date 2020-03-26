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

import static org.apache.geode.management.internal.functions.CliFunctionResult.StatusState.ERROR;
import static org.apache.geode.management.internal.functions.CliFunctionResult.StatusState.OK;
import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.control.RestoreRedundancyOperation;
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class RedundancyCommandFunctionTest {

  @SuppressWarnings("unchecked")
  private FunctionContext<Object[]> mockContext = mock(FunctionContext.class);
  private Cache mockCache = mock(Cache.class, RETURNS_DEEP_STUBS);
  private RestoreRedundancyOperation mockOperation =
      mock(RestoreRedundancyOperation.class, RETURNS_DEEP_STUBS);
  private RestoreRedundancyResults mockResults = mock(RestoreRedundancyResults.class);
  private String message = "expected message";
  private RedundancyCommandFunction function;

  @Before
  public void setUp() throws InterruptedException, ExecutionException {
    function = new RedundancyCommandFunction();
    when(mockContext.getCache()).thenReturn(mockCache);
    when(mockContext.getArguments()).thenReturn(new Object[] {null, null, true});
    when(mockCache.getResourceManager().createRestoreRedundancyOperation())
        .thenReturn(mockOperation);
    CompletableFuture<RestoreRedundancyResults> future =
        CompletableFuture.completedFuture(mockResults);
    when(mockOperation.start()).thenReturn(future);
    when(mockResults.getMessage()).thenReturn(message);
  }

  @Test
  public void executeFunctionSetsFieldsOnRestoreRedundancyOperation() {
    String[] includeRegions = {"includedRegion1", "includedRegion2"};
    String[] excludeRegions = {"excludedRegion1", "excludedRegion2"};
    boolean shouldReassign = true;

    Set<String> expectedIncludedRegions = new HashSet<>(Arrays.asList(includeRegions));
    Set<String> expectedExcludedRegions = new HashSet<>(Arrays.asList(excludeRegions));

    when(mockContext.getArguments())
        .thenReturn(new Object[] {includeRegions, excludeRegions, shouldReassign});
    when(mockResults.getStatus()).thenReturn(RestoreRedundancyResults.Status.SUCCESS);

    function.executeFunction(mockContext);

    verify(mockOperation).includeRegions(expectedIncludedRegions);
    verify(mockOperation).excludeRegions(expectedExcludedRegions);
    verify(mockOperation).shouldReassignPrimaries(shouldReassign);
  }

  @Test
  public void executeFunctionSetsIncludedAndExcludedRegionsOnRestoreRedundancyOperationWhenNull() {
    when(mockResults.getStatus()).thenReturn(RestoreRedundancyResults.Status.SUCCESS);

    function.executeFunction(mockContext);

    verify(mockOperation).includeRegions(null);
    verify(mockOperation).excludeRegions(null);
  }

  @Test
  public void executeFunctionUsesStatusMethodWhenIsStatusCommandIsTrue() {
    when(mockOperation.redundancyStatus()).thenReturn(mockResults);
    when(mockResults.getStatus()).thenReturn(RestoreRedundancyResults.Status.SUCCESS);
    // isStatusCommand is the fourth argument passed to the function
    when(mockContext.getArguments()).thenReturn(new Object[] {null, null, true, true});

    function.executeFunction(mockContext);

    verify(mockOperation, times(1)).redundancyStatus();
    verify(mockOperation, times(0)).start();
  }

  @Test
  public void executeFunctionReturnsErrorWhenRestoreRedundancyThrowsException() {
    String exceptionMessage = "Expected exception message";
    CacheClosedException exception = new CacheClosedException(exceptionMessage);
    when(mockContext.getCache()).thenThrow(exception);

    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.getStatus(), is(ERROR.name()));
    assertThat(result.getStatusMessage(), both(containsString(exception.getClass().getName()))
        .and(containsString(exception.getMessage())));
  }

  @Test
  public void executeFunctionReturnsErrorWhenResultStatusIsError() {
    when(mockResults.getStatus()).thenReturn(RestoreRedundancyResults.Status.ERROR);
    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.getStatus(), is(ERROR.name()));
    assertThat(result.getStatusMessage(), is(message));
  }

  @Test
  // The function was able to execute successfully but redundancy was not able to be established for
  // at least one region
  public void executeFunctionReturnsOkWhenResultStatusIsFailure() {
    when(mockResults.getStatus()).thenReturn(RestoreRedundancyResults.Status.FAILURE);
    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.getStatus(), is(OK.name()));
    assertThat(result.getResultObject(), is(mockResults));
  }

  @Test
  public void executeFunctionReturnsOkWhenResultStatusIsSuccess() {
    when(mockResults.getStatus()).thenReturn(RestoreRedundancyResults.Status.SUCCESS);
    CliFunctionResult result = function.executeFunction(mockContext);

    assertThat(result.getStatus(), is(OK.name()));
    assertThat(result.getResultObject(), is(mockResults));
  }
}
