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
import static org.mockito.Mockito.doThrow;
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
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigNotFoundException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingNotFoundException;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AlterMappingFunctionTest {

  private static final String REGION_NAME = "testRegion";

  private ConnectorService.RegionMapping regionMapping;
  private ConnectorService.RegionMapping existingMapping;
  private ConnectorService.RegionMapping mappingToAlter;
  private FunctionContext<ConnectorService.RegionMapping> context;
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

    regionMapping = new ConnectorService.RegionMapping(REGION_NAME, null, null, null, null);
    existingMapping = new ConnectorService.RegionMapping(REGION_NAME, null, null, null, null);
    mappingToAlter =
        new ConnectorService.RegionMapping(REGION_NAME, "pdxClass", "myTable", "connection", true);
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
    doThrow(RegionMappingNotFoundException.class).when(alterFunction).alterRegionMapping(any(),
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
    doThrow(ConnectionConfigNotFoundException.class).when(service)
        .replaceRegionMapping(eq(regionMapping));

    IgnoredException ignoredException =
        IgnoredException.addIgnoredException(RegionMappingNotFoundException.class.getName());

    try {
      function.execute(context);
    } finally {
      ignoredException.remove();
    }

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatus()).contains(REGION_NAME);
  }

  @Test
  public void alterMappingPdxClassName() {
    ConnectorService.RegionMapping newConfigValues =
        new ConnectorService.RegionMapping(REGION_NAME, "newClassName", null, null, null);

    ConnectorService.RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("newClassName");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();

    assertFieldMapping(alteredConfig);
  }

  private void assertFieldMapping(ConnectorService.RegionMapping alteredConfig) {
    List<ConnectorService.RegionMapping.FieldMapping> fieldMappings =
        alteredConfig.getFieldMapping();
    assertThat(fieldMappings).hasSize(2);
    assertThat(fieldMappings.get(0).getFieldName()).isEqualTo("field1");
    assertThat(fieldMappings.get(0).getColumnName()).isEqualTo("column1");
    assertThat(fieldMappings.get(1).getFieldName()).isEqualTo("field2");
    assertThat(fieldMappings.get(1).getColumnName()).isEqualTo("column2");
  }

  @Test
  public void alterRegionMappingTable() {
    ConnectorService.RegionMapping newConfigValues =
        new ConnectorService.RegionMapping(REGION_NAME, null, "newTable", null, null);

    ConnectorService.RegionMapping alteredConfig =
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
    ConnectorService.RegionMapping newConfigValues =
        new ConnectorService.RegionMapping(REGION_NAME, null, null, null, false);

    ConnectorService.RegionMapping alteredConfig =
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
    ConnectorService.RegionMapping newConfigValues =
        new ConnectorService.RegionMapping(REGION_NAME, null, null, "newConnection", null);

    ConnectorService.RegionMapping alteredConfig =
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
    ConnectorService.RegionMapping newConfigValues =
        new ConnectorService.RegionMapping(REGION_NAME, null, null, null, null);
    newConfigValues.setFieldMapping(new String[] {"field5:column5", "field6:column6"});


    ConnectorService.RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    List<ConnectorService.RegionMapping.FieldMapping> fieldMappings =
        alteredConfig.getFieldMapping();
    assertThat(fieldMappings).hasSize(2);
    assertThat(fieldMappings)
        .contains(new ConnectorService.RegionMapping.FieldMapping("field5", "column5"));
    assertThat(fieldMappings)
        .contains(new ConnectorService.RegionMapping.FieldMapping("field6", "column6"));
  }

  @Test
  public void alterRegionMappingRemoveFieldMappings() {
    ConnectorService.RegionMapping newConfigValues =
        new ConnectorService.RegionMapping(REGION_NAME, null, null, null, null);
    newConfigValues.setFieldMapping(new String[0]);
    ConnectorService.RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    List<ConnectorService.RegionMapping.FieldMapping> fieldMappings =
        alteredConfig.getFieldMapping();
    assertThat(fieldMappings).hasSize(0);
  }

  @Test
  public void alterRegionMappingWithEmptyString() {
    ConnectorService.RegionMapping newConfigValues =
        new ConnectorService.RegionMapping(REGION_NAME, null, null, null, null);
    newConfigValues.setFieldMapping(new String[] {""});
    ConnectorService.RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    List<ConnectorService.RegionMapping.FieldMapping> fieldMappings =
        alteredConfig.getFieldMapping();
    assertThat(fieldMappings).hasSize(0);
  }

  @Test
  public void alterRegionMappingWithNothingToAlter() {
    ConnectorService.RegionMapping newConfigValues =
        new ConnectorService.RegionMapping(REGION_NAME, null, null, null, null);

    ConnectorService.RegionMapping alteredConfig =
        function.alterRegionMapping(newConfigValues, mappingToAlter);

    assertThat(alteredConfig.getRegionName()).isEqualTo(REGION_NAME);
    assertThat(alteredConfig.getPdxClassName()).isEqualTo("pdxClass");
    assertThat(alteredConfig.getTableName()).isEqualTo("myTable");
    assertThat(alteredConfig.getConnectionConfigName()).isEqualTo("connection");
    assertThat(alteredConfig.isPrimaryKeyInValue()).isTrue();
    assertFieldMapping(alteredConfig);
  }
}
