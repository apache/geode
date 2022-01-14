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

import static junit.framework.TestCase.assertEquals;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.operations.ExecuteFunctionOperationContext;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ExecuteRegionFunction66Test {
  private static final String FUNCTION_ID = "function_id";

  private final String functionName = "functionName";

  private final Function<?> functionObject = mock(Function.class);

  private final AbstractExecution<?, ?, ?> execution = mock(AbstractExecution.class);

  private final ExecuteRegionFunction66 executeRegionFunction66 =
      (ExecuteRegionFunction66) ExecuteRegionFunction66.getCommand();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    when(functionObject.getId()).thenReturn(FUNCTION_ID);
    doCallRealMethod().when(functionObject).getRequiredPermissions(any());

    when(execution.execute(functionObject)).thenReturn(uncheckedCast(mock(ResultCollector.class)));
    when(execution.execute(functionName)).thenReturn(uncheckedCast(mock(ResultCollector.class)));
  }

  @Test
  public void executingFunctionInPreGeode18ByStringWithNoHAShouldNotSetWaitOnException() {
    executeRegionFunction66.executeFunctionWithResult(functionName,
        AbstractExecution.NO_HA_HASRESULT_NO_OPTIMIZEFORWRITE, functionObject, execution);
    verify(execution, times(0)).setWaitOnExceptionFlag(true);
  }

  @Test
  public void executingFunctionInPreGeode18ByStringWithNoHAWithOptimizeForWriteShouldNotSetWaitOnException() {
    executeRegionFunction66.executeFunctionWithResult(functionName,
        AbstractExecution.NO_HA_HASRESULT_OPTIMIZEFORWRITE, functionObject, execution);
    verify(execution, times(0)).setWaitOnExceptionFlag(true);
  }

  @Test
  public void executingFunctionObjectInPreGeode18ShouldNotSetWaitOnException() {
    executeRegionFunction66.executeFunctionWithResult(functionObject,
        AbstractExecution.NO_HA_HASRESULT_OPTIMIZEFORWRITE, functionObject, execution);
    verify(execution, times(0)).setWaitOnExceptionFlag(true);
  }

  @Test
  public void generateNullArgumentMessageIfRegionIsNull() {
    assertEquals("The input region for the execute function request is null",
        executeRegionFunction66.generateNullArgumentMessage(null, null));
  }

  @Test
  public void generateNullArgumentMessageIfFunctionIsNullAndRegionIsNotNull() {
    assertEquals("The input function for the execute function request is null",
        executeRegionFunction66.generateNullArgumentMessage("someRegion", null));
  }

  @Test
  public void populateFiltersWillReturnFiltersReadFromClientMessage() throws Exception {
    Message clientMessage = mock(Message.class);
    Part part1 = mock(Part.class);
    Object object1 = new Object();
    when(part1.getStringOrObject()).thenReturn(object1);
    Part part2 = mock(Part.class);
    Object object2 = new Object();
    when(part2.getStringOrObject()).thenReturn(object2);
    Part part3 = mock(Part.class);
    Object object3 = new Object();
    when(part3.getStringOrObject()).thenReturn(object3);

    when(clientMessage.getPart(7)).thenReturn(part1);
    when(clientMessage.getPart(8)).thenReturn(part2);
    when(clientMessage.getPart(9)).thenReturn(part3);
    int filterSize = 3;
    Set<Object> filter = executeRegionFunction66.populateFilters(clientMessage, filterSize);
    assertSame(filterSize, filter.size());
    assertTrue(filter.contains(object1));
    assertTrue(filter.contains(object2));
    assertTrue(filter.contains(object3));
  }

  @Test
  public void populateRemovedNodesWillReturnNodesReadFromClient() throws Exception {
    Message clientMessage = mock(Message.class);
    Part part1 = mock(Part.class);
    String node1 = "node1";
    when(part1.getStringOrObject()).thenReturn(node1);
    Part part2 = mock(Part.class);
    String node2 = "node2";
    when(part2.getStringOrObject()).thenReturn(node2);
    Part part3 = mock(Part.class);
    String node3 = "node3";
    when(part3.getStringOrObject()).thenReturn(node3);

    when(clientMessage.getPart(7)).thenReturn(part1);
    when(clientMessage.getPart(8)).thenReturn(part2);
    when(clientMessage.getPart(9)).thenReturn(part3);
    Set<String> nodes = executeRegionFunction66.populateRemovedNodes(clientMessage, 3, 6);
    assertTrue(nodes.contains(node1));
    assertTrue(nodes.contains(node2));
    assertTrue(nodes.contains(node3));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void getAuthorizedExecuteFunctionReturnsNullIfAuthorizationIsNull() {
    String regionPath = "regionPath";
    ExecuteFunctionOperationContext context =
        executeRegionFunction66.getAuthorizedExecuteFunctionOperationContext(null, null, true, null,
            functionName, regionPath);
    assertNull(context);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void getAuthorizedExecuteFunctionReturnsExecutionContextIfAuthorizeRequestIsNotNull() {
    String regionPath = "regionPath";
    AuthorizeRequest request = mock(AuthorizeRequest.class);
    when(request.executeFunctionAuthorize(any(), any(), any(), any(), anyBoolean()))
        .thenReturn(mock(ExecuteFunctionOperationContext.class));

    ExecuteFunctionOperationContext context =
        executeRegionFunction66.getAuthorizedExecuteFunctionOperationContext(null, null, true,
            request, functionName, regionPath);
    assertNotNull(context);
  }

}
