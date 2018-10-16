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
package org.apache.geode.internal.cache.tier.sockets.command;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ExecuteRegionFunction66Test {
  private static final String FUNCTION_ID = "function_id";

  @Mock
  private Function functionObject;

  private ExecuteRegionFunction66 executeRegionFunction66;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    this.executeRegionFunction66 = (ExecuteRegionFunction66) ExecuteRegionFunction66.getCommand();

    this.functionObject = mock(Function.class);
    when(this.functionObject.getId()).thenReturn(FUNCTION_ID);
    doCallRealMethod().when(this.functionObject).getRequiredPermissions(any());
  }

  @Test
  public void executingFunctionByStringWithNoHAShouldSetWaitOnException() throws Exception {
    AbstractExecution execution = mock(AbstractExecution.class);
    String functionName = "functionName";
    when (execution.execute(functionName)).thenReturn(mock(ResultCollector.class));
    this.executeRegionFunction66.executeFunctionWithResult(functionName, AbstractExecution.NO_HA_HASRESULT_NO_OPTIMIZEFORWRITE, functionObject, execution);
    verify(execution, times(1)).setWaitOnExceptionFlag(true);
  }

  @Test
  public void executingFunctionByStringWithNoHAWithOptimizeForWriteShouldSetWaitOnException() throws Exception {
    AbstractExecution execution = mock(AbstractExecution.class);
    String functionName = "functionName";
    when (execution.execute(functionName)).thenReturn(mock(ResultCollector.class));
    this.executeRegionFunction66.executeFunctionWithResult(functionName, AbstractExecution.NO_HA_HASRESULT_OPTIMIZEFORWRITE, functionObject, execution);
    verify(execution, times(1)).setWaitOnExceptionFlag(true);
  }

  @Test
  public void executeFunctionObjectShouldSetWaitOnException() throws Exception {
    AbstractExecution execution = mock(AbstractExecution.class);
    when (execution.execute(functionObject)).thenReturn(mock(ResultCollector.class));
    this.executeRegionFunction66.executeFunctionWithResult(functionObject, AbstractExecution.NO_HA_HASRESULT_OPTIMIZEFORWRITE, functionObject, execution);
    verify(execution, times(1)).setWaitOnExceptionFlag(true);
  }



}
