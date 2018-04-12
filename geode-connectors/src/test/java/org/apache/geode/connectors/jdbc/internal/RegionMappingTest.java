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
import static org.mockito.Mockito.mock;
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
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
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

  private ConnectorService.RegionMapping mapping;

  @Before
  public void setUp() {
    name = "name";
    fieldName1 = "myField1";
    columnName1 = "myColumn1";
    fieldName2 = "myField2";
    columnName2 = "myColumn2";
  }

  @Test
  public void emptyArrayFiledMapping() {
    mapping = new ConnectorService.RegionMapping("region", "class", "table", "connection", false);
    mapping.setFieldMapping(new String[0]);
    assertThat(mapping.isFieldMappingModified()).isTrue();
    assertThat(mapping.getFieldMapping()).isEmpty();
  }

  @Test
  public void initiatedWithNullValues() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, false);

    assertThat(mapping.getTableName()).isNull();
    assertThat(mapping.getRegionName()).isNull();
    assertThat(mapping.getConnectionConfigName()).isNull();
    assertThat(mapping.getPdxClassName()).isNull();
    assertThat(mapping.getRegionToTableName()).isNull();
    assertThat(mapping.getColumnNameForField("fieldName", mock(TableMetaDataView.class)))
        .isEqualTo("fieldName");
    assertThat(mapping.getFieldNameForColumn("columnName", mock(TypeRegistry.class)))
        .isEqualTo("columnName");
  }

  @Test
  public void hasCorrectTableName() {
    mapping = new ConnectorService.RegionMapping(null, null, name, null, false);

    assertThat(mapping.getTableName()).isEqualTo(name);
    assertThat(mapping.getRegionToTableName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectTableNameWhenRegionNameIsSet() {
    mapping = new ConnectorService.RegionMapping("regionName", null, "tableName", null, false);

    assertThat(mapping.getRegionName()).isEqualTo("regionName");
    assertThat(mapping.getTableName()).isEqualTo("tableName");
    assertThat(mapping.getRegionToTableName()).isEqualTo("tableName");
  }

  @Test
  public void hasCorrectRegionName() {
    mapping = new ConnectorService.RegionMapping(name, null, null, null, false);

    assertThat(mapping.getRegionName()).isEqualTo(name);
    assertThat(mapping.getRegionToTableName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectConfigName() {
    mapping = new ConnectorService.RegionMapping(null, null, null, name, false);

    assertThat(mapping.getConnectionConfigName()).isEqualTo(name);
  }

  @Test
  public void hasCorrectPdxClassName() {
    mapping = new ConnectorService.RegionMapping(null, name, null, null, false);

    assertThat(mapping.getPdxClassName()).isEqualTo(name);
  }

  @Test
  public void primaryKeyInValueSetCorrectly() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);

    assertThat(mapping.isPrimaryKeyInValue()).isTrue();
  }

  @Test
  public void returnsColumnNameIfFieldNotMapped() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {"otherField:column"});

    String columnName = mapping.getColumnNameForField(fieldName1, mock(TableMetaDataView.class));

    assertThat(columnName).isEqualTo(fieldName1);
  }

  @Test
  public void returnsColumnNameFromTableMetaDataIfFieldNotMappedAndMetaDataMatchesWithCaseDiffering() {
    String metaDataColumnName = fieldName1.toUpperCase();
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {"otherField:column"});
    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getColumnNames()).thenReturn(Collections.singleton(metaDataColumnName));

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView))
        .isEqualTo(metaDataColumnName);
  }

  @Test
  public void returnsColumnNameFromTableMetaDataIfFieldNotMappedAndMetaDataMatchesExactly() {
    String metaDataColumnName = fieldName1;
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {"otherField:column"});
    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getColumnNames()).thenReturn(Collections.singleton(metaDataColumnName));

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView))
        .isEqualTo(metaDataColumnName);
  }

  @Test
  public void returnsColumnNameIfFieldNotMappedAndNotInMetaData() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {"otherField:column"});
    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getColumnNames()).thenReturn(Collections.singleton("does not match"));

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView)).isEqualTo(fieldName1);
  }

  @Test
  public void getColumnNameForFieldThrowsIfTwoColumnsMatchField() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {"otherField:column"});

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
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {"otherField:column"});

    assertThat(mapping.getFieldNameForColumn("columnName", null)).isEqualTo("columnName");
  }

  @Test
  public void ifLowerCaseColumnNameNotMappedReturnsItAsFieldName() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {"otherField:column"});

    assertThat(mapping.getFieldNameForColumn("columnname", null)).isEqualTo("columnname");
  }

  @Test
  public void ifUpperCaseColumnNameNotMappedReturnsItLowerCasedAsFieldName() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {"otherField:column"});

    assertThat(mapping.getFieldNameForColumn("COLUMNNAME", null)).isEqualTo("columnname");
  }


  @Test
  public void throwsIfColumnNotMappedAndPdxClassNameDoesNotExist() {
    mapping = new ConnectorService.RegionMapping(null, "pdxClassName", null, null, true);
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
    mapping = new ConnectorService.RegionMapping(null, pdxClassName, null, null, true);
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
    mapping = new ConnectorService.RegionMapping(null, pdxClassName, null, null, true);
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
    mapping = new ConnectorService.RegionMapping(null, pdxClassName, null, null, true);
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
    mapping = new ConnectorService.RegionMapping(null, pdxClassName, null, null, true);
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    PdxType pdxType = mock(PdxType.class);
    when(pdxType.getPdxField(columnName)).thenReturn(mock(PdxField.class));
    HashSet<PdxType> pdxTypes = new HashSet<>(Arrays.asList(pdxType));
    when(typeRegistry.getPdxTypesForClassName(pdxClassName)).thenReturn(pdxTypes);

    assertThat(mapping.getFieldNameForColumn(columnName, typeRegistry)).isEqualTo(columnName);
  }

  @Test
  public void returnsMappedColumnNameForField() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {fieldName1 + ":" + columnName1});

    assertThat(mapping.getColumnNameForField(fieldName1, mock(TableMetaDataView.class)))
        .isEqualTo(columnName1);
  }

  @Test
  public void returnsMappedColumnNameForFieldEvenIfMetaDataMatches() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {fieldName1 + ":" + columnName1});

    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataView.getColumnNames()).thenReturn(Collections.singleton(fieldName1));

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView)).isEqualTo(columnName1);
  }

  @Test
  public void returnsMappedFieldNameForColumn() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {fieldName1 + ":" + columnName1});

    assertThat(mapping.getFieldNameForColumn(columnName1, null)).isEqualTo(fieldName1);
  }

  @Test
  public void returnsCachedFieldNameForColumn() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {fieldName1 + ":" + columnName1});

    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);

    mapping.getColumnNameForField(fieldName1, tableMetaDataView);

    assertThat(mapping.getFieldNameForColumn(columnName1, null)).isEqualTo(fieldName1);
  }

  @Test
  public void returnsCachedColumnNameForField() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(new String[] {fieldName1 + ":" + columnName1});

    mapping.getFieldNameForColumn(columnName1, null);

    TableMetaDataView tableMetaDataView = mock(TableMetaDataView.class);

    assertThat(mapping.getColumnNameForField(fieldName1, tableMetaDataView)).isEqualTo(columnName1);
  }

  @Test
  public void returnsAllMappings() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, true);
    mapping.setFieldMapping(
        new String[] {fieldName1 + ":" + columnName1, fieldName2 + ":" + columnName2});

    assertThat(mapping.getFieldMapping()).hasSize(2);
    assertThat(mapping.getFieldMapping().get(0).getFieldName()).isEqualTo("myField1");
    assertThat(mapping.getFieldMapping().get(0).getColumnName()).isEqualTo("myColumn1");
    assertThat(mapping.getFieldMapping().get(1).getFieldName()).isEqualTo("myField2");
    assertThat(mapping.getFieldMapping().get(1).getColumnName()).isEqualTo("myColumn2");
  }

  @Test
  public void verifyTwoNonDefaultInstancesAreEqual() {
    ConnectorService.RegionMapping rm1 = new ConnectorService.RegionMapping("regionName",
        "pdxClassName", "tableName", "connectionName", true);
    rm1.setFieldMapping(
        new String[] {fieldName1 + ":" + columnName1, fieldName2 + ":" + columnName2});

    ConnectorService.RegionMapping rm2 = new ConnectorService.RegionMapping("regionName",
        "pdxClassName", "tableName", "connectionName", true);
    rm2.setFieldMapping(
        new String[] {fieldName1 + ":" + columnName1, fieldName2 + ":" + columnName2});

    assertThat(rm1).isEqualTo(rm2);
  }

  @Test
  public void verifyTwoDefaultInstancesAreEqual() {
    ConnectorService.RegionMapping rm1 =
        new ConnectorService.RegionMapping("regionName", null, null, "connectionName", false);
    ConnectorService.RegionMapping rm2 =
        new ConnectorService.RegionMapping("regionName", null, null, "connectionName", false);
    assertThat(rm1).isEqualTo(rm2);
  }


  @Test
  public void verifyTwoInstancesThatAreEqualHaveSameHashCode() {
    ConnectorService.RegionMapping rm1 = new ConnectorService.RegionMapping("regionName",
        "pdxClassName", "tableName", "connectionName", true);
    rm1.setFieldMapping(
        new String[] {fieldName1 + ":" + columnName1, fieldName2 + ":" + columnName2});

    ConnectorService.RegionMapping rm2 = new ConnectorService.RegionMapping("regionName",
        "pdxClassName", "tableName", "connectionName", true);
    rm1.setFieldMapping(
        new String[] {fieldName1 + ":" + columnName1, fieldName2 + ":" + columnName2});

    assertThat(rm1.hashCode()).isEqualTo(rm2.hashCode());
  }

  @Test
  public void verifyThatMappingIsEqualToItself() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, false);
    boolean result = mapping.equals(mapping);
    assertThat(mapping.hashCode()).isEqualTo(mapping.hashCode());
    assertThat(result).isTrue();
  }

  @Test
  public void verifyThatNullIsNotEqual() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, false);
    boolean result = mapping.equals(null);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyOtherClassIsNotEqual() {
    mapping = new ConnectorService.RegionMapping(null, null, null, null, false);
    boolean result = mapping.equals("not equal");
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentRegionNamesAreNotEqual() {
    ConnectorService.RegionMapping rm1 =
        new ConnectorService.RegionMapping(null, null, null, null, false);
    ConnectorService.RegionMapping rm2 =
        new ConnectorService.RegionMapping("name", null, null, null, false);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

  @Test
  public void verifyMappingWithDifferentPdxClassNameAreNotEqual() {
    ConnectorService.RegionMapping rm1 =
        new ConnectorService.RegionMapping(null, null, null, null, false);
    ConnectorService.RegionMapping rm2 =
        new ConnectorService.RegionMapping(null, "pdxClass", null, null, false);
    boolean result = rm1.equals(rm2);
    assertThat(result).isFalse();
  }

}
