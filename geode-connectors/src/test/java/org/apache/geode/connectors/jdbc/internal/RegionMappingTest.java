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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RegionMappingTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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

  }

  @Test
  public void initiatedWithNullValues() {
    mapping = new RegionMapping(null, null, null, null, false, null);
    assertThat(mapping.getTableName()).isNull();
    assertThat(mapping.getRegionName()).isNull();
    assertThat(mapping.getConnectionConfigName()).isNull();
    assertThat(mapping.getPdxClassName()).isNull();
    assertThat(mapping.getFieldToColumnMap()).isEmpty();
    assertThat(mapping.getColumnToFieldMap()).isEmpty();
    assertThat(mapping.getRegionToTableName()).isNull();
    assertThat(mapping.getColumnNameForField("fieldName", null)).isEqualTo("fieldName");
    assertThat(mapping.getFieldNameForColumn("columnName")).isEqualTo("columnname");
  }

  @Test
  public void hasCorrectTableName() {
    mapping = new RegionMapping(null, null, name, null, false, null);

    assertThat(mapping.getTableName()).isEqualTo(name);
    assertThat(mapping.getRegionToTableName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectTableNameWhenRegionNameIsSet() {
    mapping = new RegionMapping("regionName", null, "tableName", null, false, null);

    assertThat(mapping.getRegionName()).isEqualTo("regionName");
    assertThat(mapping.getTableName()).isEqualTo("tableName");
    assertThat(mapping.getRegionToTableName()).isEqualTo("tableName");
  }

  @Test
  public void hasCorrectRegionName() {
    mapping = new RegionMapping(name, null, null, null, false, null);

    assertThat(mapping.getRegionName()).isEqualTo(name);
    assertThat(mapping.getRegionToTableName()).isEqualTo(name);
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
  public void returnsColumnNameIfFieldNotMapped() {
    fieldMap.put("otherField", "column");
    mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    Map<String, String> expectedFieldMap = new HashMap<>(fieldMap);
    expectedFieldMap.put(fieldName1, fieldName1);
    Map<String, String> expectedColumnMap = new HashMap<>();
    expectedColumnMap.put("column", "otherField");
    expectedColumnMap.put(fieldName1.toLowerCase(), fieldName1);

    String columName = mapping.getColumnNameForField(fieldName1, null);

    assertThat(columName).isEqualTo(fieldName1);
    assertThat(mapping.getFieldToColumnMap()).isEqualTo(expectedFieldMap);
    assertThat(mapping.getColumnToFieldMap()).isEqualTo(expectedColumnMap);
  }

  @Test
  public void returnsColumnNameFromTableMetaDataIfFieldNotMappedAndMetaDataMatchesWithCaseDiffering() {
    fieldMap.put("otherField", "column");
    String metaDataColumnName = fieldName1.toUpperCase();
    mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getColumnNames()).thenReturn(Collections.singleton(metaDataColumnName));

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView))
        .isEqualTo(metaDataColumnName);
  }

  @Test
  public void returnsColumnNameFromTableMetaDataIfFieldNotMappedAndMetaDataMatchesExactly() {
    fieldMap.put("otherField", "column");
    String metaDataColumnName = fieldName1;
    mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getColumnNames()).thenReturn(Collections.singleton(metaDataColumnName));

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView))
        .isEqualTo(metaDataColumnName);
  }

  @Test
  public void returnsColumnNameIfFieldNotMappedAndNotInMetaData() {
    fieldMap.put("otherField", "column");
    mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getColumnNames()).thenReturn(Collections.singleton("does not match"));

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView)).isEqualTo(fieldName1);
  }

  @Test
  public void getColumnNameForFieldThrowsIfTwoColumnsMatchField() {
    fieldMap.put("otherField", "column");
    mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);
    HashSet<String> columnNames =
        new HashSet<>(Arrays.asList(fieldName1.toUpperCase(), fieldName1.toLowerCase()));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);

    expectedException.expect(JdbcConnectorException.class);
    expectedException
        .expectMessage("The SQL table has at least two columns that match the PDX field: myField1");
    mapping.getColumnNameForField(fieldName1, tableMetaDataView);

  }

  @Test
  public void returnsFieldNameIfColumnNotMapped() {
    fieldMap.put("otherField", "column");

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getFieldNameForColumn("columnName")).isEqualTo("columnname");
  }

  @Test
  public void returnsMappedColumnNameForField() {
    fieldMap.put(fieldName1, columnName1);

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getColumnNameForField(fieldName1, null)).isEqualTo(columnName1);
  }

  @Test
  public void returnsMappedColumnNameForFieldEvenIfMetaDataMatches() {
    fieldMap.put(fieldName1, columnName1);
    mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getColumnNames()).thenReturn(Collections.singleton(fieldName1));

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView)).isEqualTo(columnName1);
  }

  @Test
  public void returnsMappedFieldNameForColumn() {
    fieldMap.put(fieldName1, columnName1);

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getFieldNameForColumn(columnName1)).isEqualTo(fieldName1);
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
    assertThat(mapping.getColumnToFieldMap().size()).isEqualTo(2);
    assertThat(mapping.getColumnToFieldMap()).containsOnlyKeys(columnName1.toLowerCase(),
        columnName2.toLowerCase());
    assertThat(mapping.getColumnToFieldMap()).containsEntry(columnName1.toLowerCase(), fieldName1)
        .containsEntry(columnName2.toLowerCase(), fieldName2);
  }

  @Test
  public void regionMappingFailsForInvalidFieldToColumnMapping() {
    fieldMap.put(fieldName1, columnName1);
    fieldMap.put(fieldName2, columnName1);
    expectedException.expect(IllegalArgumentException.class);
    new RegionMapping(null, null, null, null, true, fieldMap);
  }

  @Test
  public void verifyTwoNonDefaultInstancesAreEqual() {
    fieldMap.put(fieldName1, columnName1);
    fieldMap.put(fieldName2, columnName2);
    RegionMapping rm1 = new RegionMapping("regionName", "pdxClassName", "tableName",
        "connectionName", true, fieldMap);
    RegionMapping rm2 = new RegionMapping("regionName", "pdxClassName", "tableName",
        "connectionName", true, fieldMap);
    assertThat(rm1).isEqualTo(rm2);
  }

  @Test
  public void verifyTwoDefaultInstancesAreEqual() {
    RegionMapping rm1 = new RegionMapping("regionName", null, null, "connectionName", false, null);
    RegionMapping rm2 = new RegionMapping("regionName", null, null, "connectionName", false, null);
    assertThat(rm1).isEqualTo(rm2);
  }

  @Test
  public void verifyTwoSimiliarInstancesAreNotEqual() {
    fieldMap.put(fieldName1, columnName1);
    fieldMap.put(fieldName2, columnName2);
    RegionMapping rm1 = new RegionMapping("regionName", "pdxClassName", "tableName",
        "connectionName", true, fieldMap);
    RegionMapping rm2 =
        new RegionMapping("regionName", "pdxClassName", "tableName", "connectionName", true, null);
    assertThat(rm1).isNotEqualTo(rm2);
  }


  @Test
  public void verifyTwoInstancesThatAreEqualHaveSameHashCode() {
    fieldMap.put(fieldName1, columnName1);
    fieldMap.put(fieldName2, columnName2);
    RegionMapping rm1 = new RegionMapping("regionName", "pdxClassName", "tableName",
        "connectionName", true, fieldMap);
    RegionMapping rm2 = new RegionMapping("regionName", "pdxClassName", "tableName",
        "connectionName", true, fieldMap);
    assertThat(rm1.hashCode()).isEqualTo(rm2.hashCode());
  }

  @Test
  public void verifyThatMappingIsEqualToItself() {
    mapping = new RegionMapping(null, null, null, null, false, null);
    boolean result = mapping.equals(mapping);
    assertThat(mapping.hashCode()).isEqualTo(mapping.hashCode());
    assertThat(result).isTrue();
  }

  @Test
  public void verifyThatNullIsNotEqual() {
    mapping = new RegionMapping(null, null, null, null, false, null);
    boolean result = mapping.equals(null);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyOtherClassIsNotEqual() {
    mapping = new RegionMapping(null, null, null, null, false, null);
    boolean result = mapping.equals("not equal");
    assertThat(result).isFalse();
  }

}
