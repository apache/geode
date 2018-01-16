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
package org.apache.geode.connectors.jdbc.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.connectors.jdbc.internal.RegionMappingBuilder;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionMappingBuilderTest {

  @Test
  public void createsMappingWithDefaultValuesIfNonAreSet() {
    RegionMapping regionMapping = new RegionMappingBuilder().build();

    assertThat(regionMapping.getRegionName()).isNull();
    assertThat(regionMapping.getTableName()).isNull();
    assertThat(regionMapping.getPdxClassName()).isNull();
    assertThat(regionMapping.getConnectionConfigName()).isNull();
    assertThat(regionMapping.isPrimaryKeyInValue()).isNull();
  }

  @Test
  public void createsMappingWithSpecifiedValues() {
    RegionMapping regionMapping = new RegionMappingBuilder().withTableName("tableName")
        .withRegionName("regionName").withPrimaryKeyInValue("true").withPdxClassName("pdxClassName")
        .withConnectionConfigName("configName").withFieldToColumnMapping("fieldName", "columnName")
        .build();

    assertThat(regionMapping.getRegionName()).isEqualTo("regionName");
    assertThat(regionMapping.getTableName()).isEqualTo("tableName");
    assertThat(regionMapping.getPdxClassName()).isEqualTo("pdxClassName");
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo("configName");
    assertThat(regionMapping.isPrimaryKeyInValue()).isTrue();
    assertThat(regionMapping.getColumnNameForField("fieldName")).isEqualTo("columnName");
  }

  @Test
  public void createsMappingWithNullAsPrimaryKeyInValue() {
    RegionMapping regionMapping = new RegionMappingBuilder().withRegionName("regionName")
        .withConnectionConfigName("configName").withPrimaryKeyInValue((String) null).build();

    assertThat(regionMapping.getRegionName()).isEqualTo("regionName");
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo("configName");
    assertThat(regionMapping.isPrimaryKeyInValue()).isNull();
  }

  @Test
  public void createsMappingWithNullFieldToColumnMappings() {
    RegionMapping regionMapping = new RegionMappingBuilder().withRegionName("regionName")
        .withConnectionConfigName("configName").withFieldToColumnMappings(null).build();

    assertThat(regionMapping.getRegionName()).isEqualTo("regionName");
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo("configName");
    assertThat(regionMapping.getFieldToColumnMap()).isNull();
  }

  @Test
  public void createsFieldMappingsFromArray() {
    String[] fieldMappings = new String[] {"field1:column1", "field2:column2"};
    RegionMapping regionMapping =
        new RegionMappingBuilder().withFieldToColumnMappings(fieldMappings).build();

    assertThat(regionMapping.getColumnNameForField("field1")).isEqualTo("column1");
    assertThat(regionMapping.getColumnNameForField("field2")).isEqualTo("column2");
  }

  @Test
  public void createsFieldMappingsFromArrayWithEmptyElement() {
    String[] fieldMappings = new String[] {"field1:column1", "", "field2:column2"};
    RegionMapping regionMapping =
        new RegionMappingBuilder().withFieldToColumnMappings(fieldMappings).build();

    assertThat(regionMapping.getColumnNameForField("field1")).isEqualTo("column1");
    assertThat(regionMapping.getColumnNameForField("field2")).isEqualTo("column2");
  }

  @Test
  public void throwsExceptionForInvalidFieldMappings() {
    RegionMappingBuilder regionMappingbuilder = new RegionMappingBuilder();

    assertThatThrownBy(
        () -> regionMappingbuilder.withFieldToColumnMappings(new String[] {"field1column1"}))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
        () -> regionMappingbuilder.withFieldToColumnMappings(new String[] {":field1column1"}))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
        () -> regionMappingbuilder.withFieldToColumnMappings(new String[] {"field1:column1:extra"}))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
        () -> regionMappingbuilder.withFieldToColumnMappings(new String[] {"field1column1:"}))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> regionMappingbuilder.withFieldToColumnMappings(new String[] {":"}))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
