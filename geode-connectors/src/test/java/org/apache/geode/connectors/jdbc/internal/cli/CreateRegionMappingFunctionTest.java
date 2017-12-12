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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.connectors.jdbc.internal.RegionMappingBuilder;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CreateRegionMappingFunctionTest {

  private static final String REGION_NAME = "testRegion";

  private RegionMapping regionMapping;
  private FunctionContext<RegionMapping> context;
  private DistributedMember distributedMember;
  private ResultSender<Object> resultSender;
  private InternalJdbcConnectorService service;

  private CreateRegionMappingFunction function;

  @Before
  public void setUp() {
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    InternalCache cache = mock(InternalCache.class);
    DistributedSystem system = mock(DistributedSystem.class);
    distributedMember = mock(DistributedMember.class);
    service = mock(InternalJdbcConnectorService.class);

    regionMapping = new RegionMappingBuilder().withRegionName(REGION_NAME).build();

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(distributedMember);
    when(context.getArguments()).thenReturn(regionMapping);
    when(cache.getService(eq(InternalJdbcConnectorService.class))).thenReturn(service);

    function = new CreateRegionMappingFunction();
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

    assertThat(copy).isNotSameAs(original).isInstanceOf(CreateRegionMappingFunction.class);
  }

  @Test
  public void createRegionMappingReturnsRegionName() throws Exception {
    function.createRegionMapping(service, regionMapping);

    verify(service, times(1)).createRegionMapping(regionMapping);
  }

  @Test
  public void createRegionMappingThrowsIfMappingExists() throws Exception {
    doThrow(ConnectionConfigExistsException.class).when(service)
        .createRegionMapping(eq(regionMapping));

    assertThatThrownBy(() -> function.createRegionMapping(service, regionMapping))
        .isInstanceOf(ConnectionConfigExistsException.class);

    verify(service, times(1)).createRegionMapping(regionMapping);
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
    assertThat(argument.getValue().getErrorMessage())
        .contains(RegionMappingExistsException.class.getName());
  }
}
