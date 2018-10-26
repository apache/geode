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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

public class DescribeMappingFunctionTest {

  private static final String EXISTING_MAPPING = "existingMapping";

  private DescribeMappingFunction function;
  private JdbcConnectorService service;
  private FunctionContext<String> context;
  private RegionMapping regionMapping;
  private ResultSender<Object> resultSender;

  @Before
  public void setUp() {
    function = new DescribeMappingFunction();

    InternalCache cache = mock(InternalCache.class);
    context = mock(FunctionContext.class);
    service = mock(JdbcConnectorService.class);
    regionMapping = mock(RegionMapping.class);
    resultSender = mock(ResultSender.class);

    DistributedMember member = mock(DistributedMember.class);
    DistributedSystem system = mock(DistributedSystem.class);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getService(eq(JdbcConnectorService.class))).thenReturn(service);
    when(service.getMappingForRegion(EXISTING_MAPPING)).thenReturn(regionMapping);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(member);
  }

  @Test
  public void isHAReturnsFalse() {
    assertThat(function.isHA()).isFalse();
  }

  @Test
  public void getIdReturnsNameOfClass() {
    assertThat(function.getId()).isEqualTo(function.getClass().getName());
  }

  @Test
  public void serializes() {
    Serializable original = function;
    Object copy = SerializationUtils.clone(original);
    assertThat(copy).isNotSameAs(original).isInstanceOf(DescribeMappingFunction.class);
  }

  @Test
  public void returnsResultWithCorrectConfig() {
    when(context.getArguments()).thenReturn(EXISTING_MAPPING);

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getResultObject()).isSameAs(regionMapping);
  }

  @Test
  public void returnNullWithNonExistingConfig() {
    when(context.getArguments()).thenReturn("non existing");

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getResultObject()).isNull();

  }

  @Test
  public void executeReturnsResultForExceptionWithoutMessage() {
    when(service.getMappingForRegion(any())).thenThrow(new NullPointerException());

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage())
        .contains(NullPointerException.class.getName());
  }

  @Test
  public void executeReturnsResultForExceptionWithMessage() {
    when(service.getMappingForRegion(any()))
        .thenThrow(new IllegalArgumentException("some message"));

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage()).contains("some message");
  }
}
