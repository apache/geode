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

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.util.DriverJarUtil;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class ListDriversFunctionTest {

  private ListDriversFunction function;
  private FunctionContext<Object[]> context;
  private DriverJarUtil util;
  private List<String> driverNames;


  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    context = mock(FunctionContextImpl.class);
    function = spy(new ListDriversFunction());
    when(context.getMemberName()).thenReturn("Test Member Name");
    util = mock(DriverJarUtil.class);
    doReturn(util).when(function).getDriverJarUtil();

    driverNames = new ArrayList<>();
    driverNames.add("Driver.Class.One");
    driverNames.add("Driver.Class.Two");
    driverNames.add("Driver.Class.Three");
  }

  @Test
  public void testExecuteFunctionDoesNotReturnError() {
    when(util.getRegisteredDriverNames()).thenReturn(driverNames);
    CliFunctionResult functionResult = function.executeFunction(context);
    assertThat(functionResult.getResultObject().equals(driverNames));
    assertThat(functionResult.getStatusMessage())
        .isEqualTo("{Driver.Class.One, Driver.Class.Two, Driver.Class.Three}");
    assertThat(functionResult.isSuccessful()).isTrue();
  }

  @Test
  public void testExecuteFunctionReturnsWithException() {
    String exceptionString = "Test null pointer exception";
    doThrow(new NullPointerException(exceptionString)).when(util).getRegisteredDriverNames();
    CliFunctionResult functionResult = function.executeFunction(context);
    assertThat(functionResult.getStatusMessage()).contains(exceptionString);
    assertThat(functionResult.getStatus()).contains(CliFunctionResult.StatusState.ERROR.toString());
  }
}
