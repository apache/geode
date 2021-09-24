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
package org.apache.geode.cache.client.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * Test multi-hop on region function execution
 */
@Category({ClientServerTest.class})
@RunWith(GeodeParamsRunner.class)
public class ExecuteRegionFunctionOpTest {

  @Test
  public void addFunctionExceptionWithFunctionTargetInvocationExceptionWrapsInPlainFunctionException() {
    FunctionInvocationTargetException exception = mock(FunctionInvocationTargetException.class);
    ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl op =
        new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl();
    op.addFunctionException(exception);
    assertThat(op.getFunctionException()).isInstanceOf(FunctionException.class);
    assertThat(op.getFunctionException()).isNotInstanceOf(FunctionInvocationTargetException.class);
  }

  @Test
  public void addFunctionExceptionWithInternalFunctionTargetInvocationExceptionWrapsInPlainFunctionException() {
    FunctionInvocationTargetException exception =
        mock(InternalFunctionInvocationTargetException.class);
    ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl op =
        new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl();
    op.addFunctionException(exception);
    assertThat(op.getFunctionException()).isInstanceOf(FunctionException.class);
    assertThat(op.getFunctionException())
        .isNotInstanceOf(InternalFunctionInvocationTargetException.class);
  }

  @Test
  public void addFunctionExceptionWithCauseFunctionTargetInvocationExceptionAddsToListOfException() {
    FunctionInvocationTargetException cause = mock(FunctionInvocationTargetException.class);
    FunctionException exception = new FunctionException(cause);
    ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl op =
        new ExecuteRegionFunctionOp.ExecuteRegionFunctionOpImpl();
    op.addFunctionException(exception);
    assertThat(op.getFunctionException()).isInstanceOf(FunctionException.class);
    assertThat(op.getFunctionException().getExceptions()).contains(cause);
  }

}
