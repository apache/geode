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

public class DestroyMappingCommandFunctionTest {

  private static final String regionName = "testRegion";

  private DestroyMappingFunction function;
  private FunctionContext<String> context;
  private ResultSender<Object> resultSender;
  private RegionMapping mapping;
  private JdbcConnectorService service;

  @Before
  public void setUp() {
    InternalCache cache = mock(InternalCache.class);
    context = mock(FunctionContext.class);
    DistributedMember member = mock(DistributedMember.class);
    resultSender = mock(ResultSender.class);
    service = mock(JdbcConnectorService.class);
    DistributedSystem system = mock(DistributedSystem.class);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(member);
    when(context.getArguments()).thenReturn(regionName);
    when(cache.getService(eq(JdbcConnectorService.class))).thenReturn(service);

    mapping = new RegionMapping();

    function = new DestroyMappingFunction();
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

    assertThat(copy).isNotSameAs(original).isInstanceOf(DestroyMappingFunction.class);
  }

  @Test
  public void destroyRegionMappingReturnsTrueIfConnectionDestroyed() {
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);
    CliFunctionResult result = function.executeFunction(context);
    assertThat(result.isSuccessful()).isTrue();
  }

  @Test
  public void destroyRegionMappingReturnsFalseIfMappingDoesNotExist() {
    CliFunctionResult result = function.executeFunction(context);
    assertThat(result.isSuccessful()).isFalse();
  }

  @Test
  public void executeDestroysIfMappingFound() {
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    function.execute(context);

    verify(service, times(1)).destroyRegionMapping(eq(regionName));
  }

  @Test
  public void executeReportsErrorIfMappingNotFound() {
    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage())
        .contains("Region mapping for region \"" + regionName + "\" not found");
  }
}
