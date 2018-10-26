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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingNotFoundException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;

public class AlterMappingFunctionTest {

  private static final String REGION_NAME = "testRegion";

  private RegionMapping regionMapping;
  private RegionMapping existingMapping;
  private RegionMapping mappingToAlter;
  private FunctionContext<RegionMapping> context;
  private ResultSender<Object> resultSender;
  private JdbcConnectorService service;

  private AlterMappingFunction function;

  @Before
  public void setUp() {
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    InternalCache cache = mock(InternalCache.class);
    DistributedSystem system = mock(DistributedSystem.class);
    DistributedMember distributedMember = mock(DistributedMember.class);
    service = mock(JdbcConnectorService.class);

    regionMapping = new RegionMapping(REGION_NAME, null, null, null, null);
    existingMapping = new RegionMapping(REGION_NAME, null, null, null, null);
    mappingToAlter =
        new RegionMapping(REGION_NAME, "pdxClass", "myTable", "connection", true);
    mappingToAlter.setFieldMapping(new String[] {"field1:column1", "field2:column2"});

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(distributedMember);
    when(context.getArguments()).thenReturn(regionMapping);
    when(cache.getService(eq(JdbcConnectorService.class))).thenReturn(service);
    function = new AlterMappingFunction();
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

    assertThat(copy).isNotSameAs(original).isInstanceOf(AlterMappingFunction.class);
  }

  @Test
  public void alterMissingRegionMappingThrowsRegionMappingNotFound() {
    AlterMappingFunction alterFunction = mock(AlterMappingFunction.class);
    doAnswer((m) -> {
      throw new RegionMappingNotFoundException();
    }).when(alterFunction).alterRegionMapping(any(),
        any());

    assertThatThrownBy(() -> alterFunction.alterRegionMapping(regionMapping, existingMapping))
        .isInstanceOf(RegionMappingNotFoundException.class);
  }

  @Test
  public void executeInvokesReplaceOnService() throws Exception {
    when(service.getMappingForRegion(REGION_NAME)).thenReturn(existingMapping);

    AlterMappingFunction function = spy(new AlterMappingFunction());
    function.execute(context);

    verify(service, times(1)).replaceRegionMapping(any());
  }

  @Test
  public void executeReportsErrorIfRegionMappingNotFound() throws Exception {
    doAnswer((m) -> {
      throw new RegionMappingNotFoundException();
    }).when(service)
        .replaceRegionMapping(eq(regionMapping));

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage()).contains(REGION_NAME);
  }

  @Test
  public void alterMappingPdxClassName() {
    RegionMapping newConfigValues =
        new RegionMapping(REGION_NAME, "newClassName", null, null, null);

    RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("newClassName");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();

    assertFieldMapping(alteredConfig);
  }

  private void assertFieldMapping(RegionMapping alteredConfig) {
    List<RegionMapping.FieldMapping> fieldMappings =
        alteredConfig.getFieldMapping();
    assertThat(fieldMappings).hasSize(2);
    assertThat(fieldMappings.get(0).getFieldName()).isEqualTo("field1");
    assertThat(fieldMappings.get(0).getColumnName()).isEqualTo("column1");
    assertThat(fieldMappings.get(1).getFieldName()).isEqualTo("field2");
    assertThat(fieldMappings.get(1).getColumnName()).isEqualTo("column2");
  }

  @Test
  public void alterRegionMappingTable() {
    RegionMapping newConfigValues =
        new RegionMapping(REGION_NAME, null, "newTable", null, null);

    RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("newTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    assertFieldMapping(alteredConfig);
  }

  @Test
  public void alterRegionMappingPrimaryKeyInValue() {
    RegionMapping newConfigValues =
        new RegionMapping(REGION_NAME, null, null, null, false);

    RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isFalse();
    assertFieldMapping(alteredConfig);
  }

  @Test
  public void alterRegionMappingConnectionName() {
    RegionMapping newConfigValues =
        new RegionMapping(REGION_NAME, null, null, "newConnection", null);

    RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("newConnection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    assertFieldMapping(alteredConfig);
  }

  @Test
  public void alterRegionMappingFieldMappings() {
    RegionMapping newConfigValues =
        new RegionMapping(REGION_NAME, null, null, null, null);
    newConfigValues.setFieldMapping(new String[] {"field5:column5", "field6:column6"});


    RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    List<RegionMapping.FieldMapping> fieldMappings =
        alteredConfig.getFieldMapping();
    assertThat(fieldMappings).hasSize(2);
    assertThat(fieldMappings)
        .contains(new RegionMapping.FieldMapping("field5", "column5"));
    assertThat(fieldMappings)
        .contains(new RegionMapping.FieldMapping("field6", "column6"));
  }

  @Test
  public void alterRegionMappingRemoveFieldMappings() {
    RegionMapping newConfigValues =
        new RegionMapping(REGION_NAME, null, null, null, null);
    newConfigValues.setFieldMapping(new String[0]);
    RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    List<RegionMapping.FieldMapping> fieldMappings =
        alteredConfig.getFieldMapping();
    assertThat(fieldMappings).hasSize(0);
  }

  @Test
  public void alterRegionMappingWithEmptyString() {
    RegionMapping newConfigValues =
        new RegionMapping(REGION_NAME, null, null, null, null);
    newConfigValues.setFieldMapping(new String[] {""});
    RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    List<RegionMapping.FieldMapping> fieldMappings =
        alteredConfig.getFieldMapping();
    assertThat(fieldMappings).hasSize(0);
  }

  @Test
  public void alterRegionMappingWithNothingToAlter() {
    RegionMapping newConfigValues =
        new RegionMapping(REGION_NAME, null, null, null, null);

    RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    assertFieldMapping(alteredConfig);
  }
}
