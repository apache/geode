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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionMappingTest {

  @Test
  public void initiatedWithNullValues() {
    RegionMapping mapping = new RegionMapping(null, null, null, null, false, null);
    assertThat(mapping.getTableName()).isNull();
    assertThat(mapping.getRegionName()).isNull();
    assertThat(mapping.getConnectionConfigName()).isNull();
    assertThat(mapping.getPdxClassName()).isNull();
  }

  @Test
  public void hasCorrectTableName() {
    String name = "name";
    RegionMapping mapping = new RegionMapping(null, null, name, null, false, null);
    assertThat(mapping.getTableName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectRegionName() {
    String name = "name";
    RegionMapping mapping = new RegionMapping(name, null, null, null, false, null);
    assertThat(mapping.getRegionName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectConfigName() {
    String name = "name";
    RegionMapping mapping = new RegionMapping(null, null, null, name, false, null);
    assertThat(mapping.getConnectionConfigName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectPdxClassName() {
    String name = "name";
    RegionMapping mapping = new RegionMapping(null, name, null, null, false, null);
    assertThat(mapping.getPdxClassName()).isEqualTo(name);
  }

  @Test
  public void primaryKeyInValueSetCorrectly() {
    RegionMapping mapping = new RegionMapping(null, null, null, null, true, null);
    assertThat(mapping.isPrimaryKeyInValue()).isTrue();
  }

  @Test
  public void returnsFieldNameIfColumnNotMapped() {
    String fieldName = "myField";
    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put("otherField", "column");
    RegionMapping mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    assertThat(mapping.getColumnNameForField(fieldName)).isEqualTo(fieldName);
  }

  @Test
  public void returnsMappedColumnNameForField() {
    String fieldName = "myField";
    String columnName = "myColumn";
    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put(fieldName, columnName);
    RegionMapping mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    assertThat(mapping.getColumnNameForField(fieldName)).isEqualTo(columnName);
  }

  @Test
  public void returnsAllMappings() {
    String fieldName1 = "myField1";
    String columnName1 = "myColumn1";
    String fieldName2 = "myField2";
    String columnName2 = "myColumn2";
    Map<String, String> fieldMap = new HashMap<>();
    fieldMap.put(fieldName1, columnName1);
    fieldMap.put(fieldName2, columnName2);
    RegionMapping mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getFieldToColumnMap().size()).isEqualTo(2);
    assertThat(mapping.getFieldToColumnMap()).containsOnlyKeys(fieldName1, fieldName2);
    assertThat(mapping.getFieldToColumnMap()).containsEntry(fieldName1, columnName1)
        .containsEntry(fieldName2, columnName2);
  }
}
