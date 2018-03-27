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
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;
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
    assertThat(mapping.getFieldToColumnMap()).isNull();
    assertThat(mapping.getRegionToTableName()).isNull();
    assertThat(mapping.getColumnNameForField("fieldName", mock(TableMetaDataView.class)))
        .isEqualTo("fieldName");
    assertThat(mapping.getFieldNameForColumn("columnName", mock(TypeRegistry.class)))
        .isEqualTo("columnName");
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
    expectedColumnMap.put(fieldName1, fieldName1);

    String columnName = mapping.getColumnNameForField(fieldName1, mock(TableMetaDataView.class));

    assertThat(columnName).isEqualTo(fieldName1);
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
  public void ifMixedCaseColumnNameNotMappedReturnsItAsFieldName() {
    fieldMap.put("otherField", "column");

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getFieldNameForColumn("columnName", null)).isEqualTo("columnName");
  }

  @Test
  public void ifLowerCaseColumnNameNotMappedReturnsItAsFieldName() {
    fieldMap.put("otherField", "column");

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getFieldNameForColumn("columnname", null)).isEqualTo("columnname");
  }

  @Test
  public void ifUpperCaseColumnNameNotMappedReturnsItLowerCasedAsFieldName() {
    fieldMap.put("otherField", "column");

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getFieldNameForColumn("COLUMNNAME", null)).isEqualTo("columnname");
  }


  @Test
  public void throwsIfColumnNotMappedAndPdxClassNameDoesNotExist() {
    mapping = new RegionMapping(null, "pdxClassName", null, null, true, null);
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    when(typeRegistry.getPdxTypesForClassName("pdxClassName")).thenReturn(Collections.emptySet());
    expectedException.expect(JdbcConnectorException.class);
    expectedException.expectMessage("The class pdxClassName has not been pdx serialized.");

    mapping.getFieldNameForColumn("columnName", typeRegistry);
  }

  @Test
  public void throwsIfColumnNotMappedAndPdxClassNameDoesExistButHasNoMatchingFields() {
    String pdxClassName = "pdxClassName";
    String columnName = "columnName";
    mapping = new RegionMapping(null, pdxClassName, null, null, true, null);
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    HashSet<PdxType> pdxTypes = new HashSet<>(Arrays.asList(mock(PdxType.class)));
    when(typeRegistry.getPdxTypesForClassName(pdxClassName)).thenReturn(pdxTypes);
    expectedException.expect(JdbcConnectorException.class);
    expectedException.expectMessage("The class " + pdxClassName
        + " does not have a field that matches the column " + columnName);

    mapping.getFieldNameForColumn(columnName, typeRegistry);
  }

  @Test
  public void throwsIfColumnNotMappedAndPdxClassNameDoesExistButHasMoreThanOneMatchingFields() {
    String pdxClassName = "pdxClassName";
    String columnName = "columnName";
    mapping = new RegionMapping(null, pdxClassName, null, null, true, null);
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    PdxType pdxType = mock(PdxType.class);
    when(pdxType.getFieldNames())
        .thenReturn(Arrays.asList(columnName.toLowerCase(), columnName.toUpperCase()));
    HashSet<PdxType> pdxTypes = new HashSet<>(Arrays.asList(pdxType));
    when(typeRegistry.getPdxTypesForClassName(pdxClassName)).thenReturn(pdxTypes);
    expectedException.expect(JdbcConnectorException.class);
    expectedException.expectMessage(
        "Could not determine what pdx field to use for the column name " + columnName);

    mapping.getFieldNameForColumn(columnName, typeRegistry);
  }

  @Test
  public void returnsIfColumnNotMappedAndPdxClassNameDoesExistAndHasOneFieldThatInexactlyMatches() {
    String pdxClassName = "pdxClassName";
    String columnName = "columnName";
    mapping = new RegionMapping(null, pdxClassName, null, null, true, null);
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    PdxType pdxType = mock(PdxType.class);
    when(pdxType.getFieldNames())
        .thenReturn(Arrays.asList("someOtherField", columnName.toUpperCase()));
    HashSet<PdxType> pdxTypes = new HashSet<>(Arrays.asList(pdxType));
    when(typeRegistry.getPdxTypesForClassName(pdxClassName)).thenReturn(pdxTypes);

    assertThat(mapping.getFieldNameForColumn(columnName, typeRegistry))
        .isEqualTo(columnName.toUpperCase());
  }

  @Test
  public void returnsIfColumnNotMappedAndPdxClassNameDoesExistAndHasOneFieldThatExactlyMatches() {
    String pdxClassName = "pdxClassName";
    String columnName = "columnName";
    mapping = new RegionMapping(null, pdxClassName, null, null, true, null);
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    PdxType pdxType = mock(PdxType.class);
    when(pdxType.getPdxField(columnName)).thenReturn(mock(PdxField.class));
    HashSet<PdxType> pdxTypes = new HashSet<>(Arrays.asList(pdxType));
    when(typeRegistry.getPdxTypesForClassName(pdxClassName)).thenReturn(pdxTypes);

    assertThat(mapping.getFieldNameForColumn(columnName, typeRegistry)).isEqualTo(columnName);
  }

  @Test
  public void returnsMappedColumnNameForField() {
    fieldMap.put(fieldName1, columnName1);

    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    assertThat(mapping.getColumnNameForField(fieldName1, mock(TableMetaDataView.class)))
        .isEqualTo(columnName1);
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

    assertThat(mapping.getFieldNameForColumn(columnName1, null)).isEqualTo(fieldName1);
  }

  @Test
  public void returnsCachedFieldNameForColumn() {
    fieldMap.put(fieldName1, columnName1);
    mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);

    mapping.getColumnNameForField(fieldName1, tableMetaDataView);

    assertThat(mapping.getFieldNameForColumn(columnName1, null)).isEqualTo(fieldName1);
  }

  @Test
  public void returnsCachedColumnNameForField() {
    fieldMap.put(fieldName1, columnName1);
    mapping = new RegionMapping(null, null, null, null, true, fieldMap);
    mapping.getFieldNameForColumn(columnName1, null);

    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView)).isEqualTo(columnName1);
  }

  @Test
  public void returnsAllMappings() {
    fieldMap.put(fieldName1, columnName1);
    fieldMap.put(fieldName2, columnName2);
    mapping = new RegionMapping(null, null, null, null, true, fieldMap);

    Map<String, String> actualFieldMap = mapping.getFieldToColumnMap();

    assertThat(actualFieldMap).isEqualTo(fieldMap);
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

  @Test
  public void verifyMappingWithDifferentRegionNamesAreNotEqual() {
    RegionMapping rm1 = new RegionMapping(null, null, null, null, false, null);
    RegionMapping rm2 = new RegionMapping("name", null, null, null, false, null);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentPdxClassNameAreNotEqual() {
    RegionMapping rm1 = new RegionMapping(null, null, null, null, false, null);
    RegionMapping rm2 = new RegionMapping(null, "pdxClass", null, null, false, null);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentFieldMappingAreNotEqual() {
    Map<String, String> fieldMap1 = new HashMap<>();
    Map<String, String> fieldMap2 = new HashMap<>();
    fieldMap1.put(fieldName1, columnName1);
    fieldMap2.put(fieldName2, columnName2);

    RegionMapping rm1 = new RegionMapping("name", "pdxClass", "tname", "cc", true, fieldMap1);
    RegionMapping rm2 = new RegionMapping("name", "pdxClass", "tname", "cc", true, fieldMap2);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithOneFieldMappingNullAreNotEqual() {
    Map<String, String> fieldMap1 = new HashMap<>();

    RegionMapping rm1 = new RegionMapping("name", "pdxClass", "tname", "cc", true, fieldMap1);
    RegionMapping rm2 = new RegionMapping("name", "pdxClass", "tname", "cc", true, null);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();

    rm1 = new RegionMapping("name", "pdxClass", "tname", "cc", true, null);
    rm2 = new RegionMapping("name", "pdxClass", "tname", "cc", true, fieldMap1);
    result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyToStringForExpectedMessage() {
    mapping = new RegionMapping("name", "pdxClass", "tname", "cc", true, null);

    String result = mapping.toString();
    System.out.println("DEBUG:" + result);

    assertThat(result).contains("RegionMapping{regionName='");
  }

}
