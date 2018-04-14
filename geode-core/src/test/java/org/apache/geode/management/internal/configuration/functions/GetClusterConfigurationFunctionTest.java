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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.internal.configuration.messages.ConfigurationResponse;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
@PowerMockIgnore("*.UnitTest")
@RunWith(PowerMockRunner.class)
@PrepareForTest(InternalLocator.class)
public class GetClusterConfigurationFunctionTest {
  private InternalLocator mockedLocator;
  private FunctionContext mockedFunctionContext;
  private ResultSender<Object> mockedResultSender;
  private InternalClusterConfigurationService mockedConfigurationService;
  private GetClusterConfigurationFunction getClusterConfigurationFunction;

  @Before
  public void setUp() {
    mockedResultSender = mock(ResultSender.class);
    mockedLocator = mock(InternalLocator.class);
    mockedFunctionContext = mock(FunctionContext.class);
    mockedConfigurationService = mock(InternalClusterConfigurationService.class);
    getClusterConfigurationFunction = new GetClusterConfigurationFunction();

    when(mockedLocator.isSharedConfigurationEnabled()).thenReturn(true);
    when(mockedLocator.isSharedConfigurationRunning()).thenReturn(true);
    when(mockedFunctionContext.getResultSender()).thenReturn(mockedResultSender);
    when(mockedFunctionContext.getArguments()).thenReturn(Collections.emptySet());

    PowerMockito.mockStatic(InternalLocator.class);
    when(InternalLocator.getLocator()).thenReturn(mockedLocator);
  }

  @Test
  public void executeShouldReturnIllegalStateExceptionWhenClusterConfigurationServiceIsDisabled() {
    when(mockedLocator.isSharedConfigurationEnabled()).thenReturn(false);
    ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);

    assertThatCode(() -> getClusterConfigurationFunction.execute(mockedFunctionContext))
        .doesNotThrowAnyException();
    verify(mockedResultSender).lastResult(argumentCaptor.capture());
    Exception exceptionThrown = argumentCaptor.getValue();
    assertThat(exceptionThrown).isInstanceOf(IllegalStateException.class)
        .hasMessage("The cluster configuration service is not enabled on this member.");
  }

  @Test
  public void executeShouldReturnExceptionWhenClusterConfigurationServiceIsEnabledButFailuresOccurWhileRetrievingIt() {
    when(mockedConfigurationService.createConfigurationResponse(any()))
        .thenThrow(new RuntimeException("Mocked Exception."));
    when(mockedLocator.getSharedConfiguration()).thenReturn(mockedConfigurationService);
    ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);

    assertThatCode(() -> getClusterConfigurationFunction.execute(mockedFunctionContext))
        .doesNotThrowAnyException();
    verify(mockedResultSender).lastResult(argumentCaptor.capture());
    Exception exceptionThrown = argumentCaptor.getValue();
    assertThat(exceptionThrown).isInstanceOf(RuntimeException.class)
        .hasMessage("Mocked Exception.");
  }

  @Test
  public void executeShouldReturnNullWhenClusterConfigurationServiceIsEnabledButNotRunning() {
    when(mockedLocator.isSharedConfigurationRunning()).thenReturn(false);

    assertThatCode(() -> getClusterConfigurationFunction.execute(mockedFunctionContext))
        .doesNotThrowAnyException();
    verify(mockedResultSender, times(1)).lastResult(null);
  }

  @Test
  public void executeShouldReturnTheRequestConfigurationWhenClusterConfigurationServiceIsEnabled() {
    Set<String> requestedGroups = new HashSet<>(Arrays.asList("group1", "group2"));
    when(mockedFunctionContext.getArguments()).thenReturn(requestedGroups);
    when(mockedLocator.getSharedConfiguration()).thenReturn(mockedConfigurationService);
    ConfigurationResponse mockedResponse = new ConfigurationResponse();
    when(mockedConfigurationService.createConfigurationResponse(any())).thenReturn(mockedResponse);
    ArgumentCaptor<ConfigurationResponse> argumentCaptor =
        ArgumentCaptor.forClass(ConfigurationResponse.class);

    assertThatCode(() -> getClusterConfigurationFunction.execute(mockedFunctionContext))
        .doesNotThrowAnyException();
    verify(mockedResultSender).lastResult(argumentCaptor.capture());
    verify(mockedConfigurationService, times(1)).createConfigurationResponse(requestedGroups);
    verify(mockedResultSender, times(1)).lastResult(mockedResponse);
  }
}
