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

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionMappingTest {

  private String name;
  private String fieldName1;
  private String columnName1;
  private String fieldName2;
  private String columnName2;

  private Map<String, String> fieldMap;

  private RegionMapping mapping;

  @Before
  public void setUp() {
    name = "name";
    fieldName1 = "myField1";
    columnName1 = "myColumn1";
    fieldName2 = "myField2";
    columnName2 = "myColumn2";

    fieldMap = new HashMap<>();

    mapping = new RegionMapping(null, null, null, null, false, null);
  }

  @Test
  public void initiatedWithNullValues() {
    assertThat(mapping.getTableName()).isNull();
    assertThat(mapping.getRegionName()).isNull();
    assertThat(mapping.getConnectionConfigName()).isNull();
    assertThat(mapping.getPdxClassName()).isNull();
  }

  @Test
  public void hasCorrectTableName() {
    mapping = new RegionMapping(null, null, name, null, false, null);

    assertThat(mapping.getTableName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectRegionName() {
    mapping = new RegionMapping(name, null, null, null, false, null);

    assertThat(mapping.getRegionName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectConfigName() {
    mapping = new RegionMapping(null, null, null, name, false, null);

    assertThat(mapping.getConnectionConfigName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectPdxClassName() {
    mapping = new RegionMapping(null, name, null, null, false, null);

    assertThat(mapping.getPdxClassName()).isEqualTo(name);
  }

  @Test
  public void primaryKeyInValueSetCorrectly() {
    mapping = new RegionMapping(null, null, null, null, true, null);

    assertThat(mapping.isPrimaryKeyInValue()).isTrue();
  }

  @Test
  public void returnsFieldNameIfColumnNotMapped() {
    fieldMap.put("otherField", "column");

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getColumnNameForField(fieldName1)).isEqualTo(fieldName1);
  }

  @Test
  public void returnsMappedColumnNameForField() {
    fieldMap.put(fieldName1, columnName1);

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getColumnNameForField(fieldName1)).isEqualTo(columnName1);
  }

  @Test
  public void returnsAllMappings() {
    fieldMap.put(fieldName1, columnName1);
    fieldMap.put(fieldName2, columnName2);

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getFieldToColumnMap().size()).isEqualTo(2);
    assertThat(mapping.getFieldToColumnMap()).containsOnlyKeys(fieldName1, fieldName2);
    assertThat(mapping.getFieldToColumnMap()).containsEntry(fieldName1, columnName1)
        .containsEntry(fieldName2, columnName2);
  }
}
