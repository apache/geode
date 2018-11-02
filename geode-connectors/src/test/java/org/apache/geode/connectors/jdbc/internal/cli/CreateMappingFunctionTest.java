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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

public class CreateMappingFunctionTest {

  private static final String REGION_NAME = "testRegion";

  private RegionMapping regionMapping;
  private FunctionContext<RegionMapping> context;
  private DistributedMember distributedMember;
  private ResultSender<Object> resultSender;
  private JdbcConnectorService service;
  private InternalCache cache;

  private CreateMappingFunction function;

  @Before
  public void setUp() {
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    cache = mock(InternalCache.class);
    Region region = mock(Region.class);
    DistributedSystem system = mock(DistributedSystem.class);
    distributedMember = mock(DistributedMember.class);
    service = mock(JdbcConnectorService.class);

    regionMapping = new RegionMapping(REGION_NAME, null, null, null);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getRegion(REGION_NAME)).thenReturn(region);
    when(system.getDistributedMember()).thenReturn(distributedMember);
    when(context.getArguments()).thenReturn(regionMapping);
    when(cache.getService(eq(JdbcConnectorService.class))).thenReturn(service);

    function = new CreateMappingFunction();
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

    assertThat(copy).isNotSameAs(original).isInstanceOf(CreateMappingFunction.class);
  }

  @Test
  public void createRegionMappingReturnsRegionName() throws Exception {
    function.createRegionMapping(service, regionMapping);

    verify(service, times(1)).createRegionMapping(regionMapping);
  }

  @Test
  public void createRegionMappingThrowsIfMappingExists() throws Exception {
    doAnswer((m) -> {
      throw new RegionMappingExistsException();
    }).when(service)
        .createRegionMapping(eq(regionMapping));

    assertThatThrownBy(() -> function.createRegionMapping(service, regionMapping))
        .isInstanceOf(RegionMappingExistsException.class);

    verify(service, times(1)).createRegionMapping(regionMapping);
  }

  @Test
  public void createRegionMappingThrowsIfRegionDoesNotExist() throws Exception {
    when(cache.getRegion(REGION_NAME)).thenReturn(null);

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(IllegalStateException.class)
        .hasMessage("create jdbc-mapping requires that the region \"" + REGION_NAME + "\" exists.");
  }

  @Test
  public void executeCreatesMapping() throws Exception {
    function.execute(context);

    verify(service, times(1)).createRegionMapping(regionMapping);
  }

  @Test
  public void executeReportsErrorIfRegionMappingExists() throws Exception {
    doThrow(RegionMappingExistsException.class).when(service)
        .createRegionMapping(eq(regionMapping));

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage())
        .contains(RegionMappingExistsException.class.getName());
  }
}
