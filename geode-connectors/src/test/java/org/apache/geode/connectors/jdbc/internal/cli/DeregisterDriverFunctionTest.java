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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.util.DriverJarUtil;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class DeregisterDriverFunctionTest {
  private DeregisterDriverFunction function;
  private FunctionContext<Object[]> context;
  private DriverJarUtil util;
  private final String DRIVER_CLASS_NAME = "Test.Driver.Name";

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    context = mock(FunctionContextImpl.class);
    function = spy(new DeregisterDriverFunction());
    when(context.getArguments()).thenReturn(new Object[] {DRIVER_CLASS_NAME});
    when(context.getMemberName()).thenReturn("Test Member Name");
    util = mock(DriverJarUtil.class);
    doReturn(util).when(function).getDriverJarUtil();
  }

  @Test
  public void testExecuteFunctionDoesNotReturnError() {
    CliFunctionResult functionResult = function.executeFunction(context);
    assertThat(functionResult.getStatusMessage())
        .contains(DRIVER_CLASS_NAME + " was successfully deregistered.");
    assertThat(functionResult.getStatus()).contains(CliFunctionResult.StatusState.OK.toString());
  }

  @Test
  public void testExecuteFunctionReturnsWithException() throws SQLException {
    String exceptionString = "Test SQL Exception";
    doThrow(new SQLException(exceptionString)).when(util).deregisterDriver(DRIVER_CLASS_NAME);
    CliFunctionResult functionResult = function.executeFunction(context);
    assertThat(functionResult.getStatusMessage()).contains(exceptionString);
    assertThat(functionResult.getStatus()).contains(CliFunctionResult.StatusState.ERROR.toString());
  }
}
