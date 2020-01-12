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
package org.apache.geode.management.internal.configuration.functions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.internal.SystemManagementService;

public class DownloadJarFunctionTest {

  private DownloadJarFunction function;

  private FunctionContext<Object[]> context;

  @Before
  public void setUp() throws Exception {
    function = spy(DownloadJarFunction.class);
    context = mock(FunctionContext.class);
    when(context.getArguments()).thenReturn(new String[] {"hello", "world"});
    InternalLocator locator = mock(InternalLocator.class);
    when(locator.getConfigurationPersistenceService()).thenReturn(mock(
        InternalConfigurationPersistenceService.class));
    doReturn(locator).when(function).getLocator();
    doReturn(mock(SystemManagementService.class)).when(function)
        .getExistingManagementService(context);
  }

  @Test
  public void throwExceptionWhenManagementAgentIsNull() {
    assertThatThrownBy(() -> function.execute(context)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to download jar");
  }
}
