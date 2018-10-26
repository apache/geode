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
import java.util.HashSet;
import java.util.Set;

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

public class ListMappingFunctionTest {

  private FunctionContext<Void> context;
  private ResultSender<Object> resultSender;
  private JdbcConnectorService service;

  private RegionMapping regionMapping1;
  private RegionMapping regionMapping2;
  private RegionMapping regionMapping3;

  private Set<RegionMapping> expected;

  private ListMappingFunction function;

  @Before
  public void setUp() {
    InternalCache cache = mock(InternalCache.class);
    context = mock(FunctionContext.class);
    DistributedMember member = mock(DistributedMember.class);
    resultSender = mock(ResultSender.class);
    service = mock(JdbcConnectorService.class);
    DistributedSystem system = mock(DistributedSystem.class);

    regionMapping1 = mock(RegionMapping.class);
    regionMapping2 = mock(RegionMapping.class);
    regionMapping3 = mock(RegionMapping.class);

    expected = new HashSet<>();

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(member);
    when(cache.getService(eq(JdbcConnectorService.class))).thenReturn(service);
    when(service.getRegionMappings()).thenReturn(expected);

    function = new ListMappingFunction();
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

    assertThat(copy).isNotSameAs(original).isInstanceOf(ListMappingFunction.class);
  }

  @Test
  public void getRegionMappingsReturnsMultiple() {
    expected.add(regionMapping1);
    expected.add(regionMapping2);
    expected.add(regionMapping3);

    Set<RegionMapping> actual =
        (Set<RegionMapping>) function.executeFunction(context).getResultObject();

    assertThat(actual).containsExactlyInAnyOrder(regionMapping1, regionMapping2, regionMapping3);
  }

  @Test
  public void getRegionMappingsReturnsEmpty() {
    Set<RegionMapping> actual =
        (Set<RegionMapping>) function.executeFunction(context).getResultObject();

    assertThat(actual).isEmpty();
  }

  @Test
  public void executeReturnsResultWithAllRegionMappings() {
    expected.add(regionMapping1);
    expected.add(regionMapping2);
    expected.add(regionMapping3);

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    Set<RegionMapping> mappings =
        (Set<RegionMapping>) argument.getValue().getResultObject();
    assertThat(mappings).containsExactlyInAnyOrder(regionMapping1, regionMapping2, regionMapping3);
  }

  @Test
  public void executeReturnsEmptyResultForNoRegionMappings() {
    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat((Set) argument.getValue().getResultObject()).isEmpty();
  }

  @Test
  public void executeReturnsResultForExceptionWithoutMessage() {
    when(service.getRegionMappings()).thenThrow(new NullPointerException());

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage())
        .contains(NullPointerException.class.getName());
  }

  @Test
  public void executeReturnsResultForExceptionWithMessage() {
    when(service.getRegionMappings()).thenThrow(new IllegalArgumentException("some message"));

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage()).contains("some message");
  }
}
