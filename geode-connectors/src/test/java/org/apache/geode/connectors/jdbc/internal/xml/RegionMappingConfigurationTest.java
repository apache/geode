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
package org.apache.geode.connectors.jdbc.internal.xml;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.SerializationException;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorServiceImpl;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.ExtensionPoint;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;

public class RegionMappingConfigurationTest {

  private static final String TEST_REGION_NAME = "testRegion";
  private static final String DATA_SOURCE_NAME = "dataSource";

  private static final String KEY_COLUMN_NAME = "id";
  private static final String VALUE_COLUMN_NAME = "name";

  private final List<String> keyColumns = new ArrayList<>();
  private final Set<String> allColumns = new HashSet<>();
  private final List<FieldMapping> fieldMappings = new ArrayList<>();

  private RegionMapping mapping;

  private RegionMappingConfiguration config;

  private JdbcConnectorService service;
  private final TableMetaDataView view = mock(TableMetaDataView.class);
  private final TableMetaDataManager manager = mock(TableMetaDataManager.class);
  private final InternalCache cache = mock(InternalCache.class);
  private final DataSource dataSource = mock(DataSource.class);
  private final Connection connection = mock(Connection.class);
  private final PdxType pdxType = mock(PdxType.class);
  private final TypeRegistry typeRegistry = mock(TypeRegistry.class);

  public static class PdxClassDummy {
  }

  public static class PdxClassDummyNoZeroArg {
    public PdxClassDummyNoZeroArg(@SuppressWarnings("unused") int arg) {}
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    mapping = mock(RegionMapping.class);

    service = spy(JdbcConnectorServiceImpl.class);
    when(cache.getService(JdbcConnectorService.class)).thenReturn(service);

    when(cache.getExtensionPoint()).thenReturn(mock(ExtensionPoint.class));
    when(mapping.getRegionName()).thenReturn(TEST_REGION_NAME);
    when(mapping.getDataSourceName()).thenReturn(DATA_SOURCE_NAME);
    when(mapping.getFieldMappings()).thenReturn(fieldMappings);
    when(mapping.getIds()).thenReturn(KEY_COLUMN_NAME);
    when(mapping.getSpecifiedIds()).thenReturn(true);

    when(dataSource.getConnection()).thenReturn(connection);
    when(manager.getTableMetaDataView(connection, mapping)).thenReturn(view);
    when(view.getKeyColumnNames()).thenReturn(keyColumns);
    when(view.getColumnNames()).thenReturn(allColumns);
    when(view.getColumnDataType(KEY_COLUMN_NAME)).thenReturn(JDBCType.INTEGER);
    when(view.getColumnDataType(VALUE_COLUMN_NAME)).thenReturn(JDBCType.VARCHAR);
    when(view.isColumnNullable(KEY_COLUMN_NAME)).thenReturn(false);
    when(view.isColumnNullable(VALUE_COLUMN_NAME)).thenReturn(true);

    config = spy(new RegionMappingConfiguration(mapping));

    keyColumns.add(KEY_COLUMN_NAME);
    allColumns.add(KEY_COLUMN_NAME);
    allColumns.add(VALUE_COLUMN_NAME);

    fieldMappings
        .add(new FieldMapping("id", "integer", KEY_COLUMN_NAME, JDBCType.INTEGER.getName(), false));
    fieldMappings.add(
        new FieldMapping("name", "string", VALUE_COLUMN_NAME, JDBCType.VARCHAR.getName(), true));

    when(pdxType.getFieldCount()).thenReturn(2);
    PdxField field1 = mock(PdxField.class);
    when(field1.getFieldName()).thenReturn("id");
    when(field1.getFieldType()).thenReturn(FieldType.INT);
    PdxField field2 = mock(PdxField.class);
    when(field2.getFieldName()).thenReturn("name");
    when(field2.getFieldType()).thenReturn(FieldType.STRING);
    List<PdxField> pdxFields = Arrays.asList(field1, field2);
    when(pdxType.getFields()).thenReturn(pdxFields);

    when(cache.getPdxRegistry()).thenReturn(typeRegistry);

    doReturn(dataSource).when(config).getDataSource(DATA_SOURCE_NAME);
    doReturn(manager).when(config).getTableMetaDataManager();
  }

  @Test
  public void createDefaultFieldMappingSucceedsWithExactMatchPdxFields() {
    List<FieldMapping> fieldsMappings = config.createDefaultFieldMapping(service, pdxType);

    assertThat(fieldsMappings).hasSize(2);
    assertThat(fieldsMappings).contains(
        new FieldMapping("id", FieldType.INT.name(), "id", JDBCType.INTEGER.name(), false));
    assertThat(fieldsMappings).contains(
        new FieldMapping("name", FieldType.STRING.name(), "name", JDBCType.VARCHAR.name(), true));
  }

  @Test
  public void createDefaultFieldMappingSucceedsWithIgnoreCaseMatchPdxFields() {
    when(pdxType.getFieldCount()).thenReturn(2);
    PdxField field1 = mock(PdxField.class);
    when(field1.getFieldName()).thenReturn("ID");
    when(field1.getFieldType()).thenReturn(FieldType.INT);
    PdxField field2 = mock(PdxField.class);
    when(field2.getFieldName()).thenReturn("NAME");
    when(field2.getFieldType()).thenReturn(FieldType.STRING);
    List<PdxField> pdxFields = Arrays.asList(field1, field2);
    when(pdxType.getFields()).thenReturn(pdxFields);

    List<FieldMapping> fieldsMappings = config.createDefaultFieldMapping(service, pdxType);

    assertThat(fieldsMappings).hasSize(2);
    assertThat(fieldsMappings).contains(
        new FieldMapping("ID", FieldType.INT.name(), "id", JDBCType.INTEGER.name(), false));
    assertThat(fieldsMappings).contains(
        new FieldMapping("NAME", FieldType.STRING.name(), "name", JDBCType.VARCHAR.name(), true));
  }

  @Test
  public void createDefaultFieldMappingThrowsExceptionWhenGivenUnMatchPdxFieldName() {
    when(pdxType.getFieldCount()).thenReturn(2);
    PdxField field1 = mock(PdxField.class);
    when(field1.getFieldName()).thenReturn("id");
    when(field1.getFieldType()).thenReturn(FieldType.INT);
    PdxField field2 = mock(PdxField.class);
    when(field2.getFieldName()).thenReturn("nameString");
    when(field2.getFieldType()).thenReturn(FieldType.STRING);
    List<PdxField> pdxFields = Arrays.asList(field1, field2);
    when(pdxType.getFields()).thenReturn(pdxFields);

    Throwable throwable = catchThrowable(() -> config.createDefaultFieldMapping(service, pdxType));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
        String.format("No PDX field name matched the column name \"%s\"",
            VALUE_COLUMN_NAME));
  }

  @Test
  public void createDefaultFieldMappingThrowsExceptionWhenGivenDuplicatePdxFieldName() {
    when(pdxType.getFieldCount()).thenReturn(2);
    PdxField field1 = mock(PdxField.class);
    when(field1.getFieldName()).thenReturn("id");
    when(field1.getFieldType()).thenReturn(FieldType.INT);
    PdxField field2 = mock(PdxField.class);
    when(field2.getFieldName()).thenReturn("NAME");
    when(field2.getFieldType()).thenReturn(FieldType.STRING);
    PdxField field3 = mock(PdxField.class);
    when(field3.getFieldName()).thenReturn("Name");
    when(field3.getFieldType()).thenReturn(FieldType.STRING);
    List<PdxField> pdxFields = Arrays.asList(field2, field3, field1);
    when(pdxType.getFields()).thenReturn(pdxFields);

    Throwable throwable = catchThrowable(() -> config.createDefaultFieldMapping(service, pdxType));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
        String.format("More than one PDX field name matched the column name \"%s\"",
            VALUE_COLUMN_NAME));
  }

  @Test
  public void createDefaultFieldMappingThrowsExceptionWhenDataSourceDoesNotExist() {
    doReturn(null).when(config).getDataSource(DATA_SOURCE_NAME);
    Throwable throwable = catchThrowable(() -> config.createDefaultFieldMapping(service, pdxType));
    assertThat(throwable).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
        String.format("No datasource \"%s\" found when creating default field mapping",
            mapping.getDataSourceName()));
  }

  @Test
  public void createDefaultFieldMappingThrowsExceptionWhenGetConnectionHasSqlException()
      throws SQLException {
    when(dataSource.getConnection()).thenThrow(SQLException.class);
    Throwable throwable = catchThrowable(() -> config.createDefaultFieldMapping(service, pdxType));
    assertThat(throwable).isInstanceOf(JdbcConnectorException.class);
    verify(connection, never()).close();
  }

  @Test
  public void createDefaultFieldMappingThrowsExceptionWhenGivenExistingPdxTypeWithWrongNumberOfFields() {
    doReturn(3).when(pdxType).getFieldCount();
    Throwable throwable = catchThrowable(() -> config.createDefaultFieldMapping(service, pdxType));
    assertThat(throwable).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
        String.format(
            "The table and pdx class must have the same number of columns/fields. But the table has %d columns and the pdx class has %d fields.",
            view.getColumnNames().size(), pdxType.getFieldCount()));
  }

  @Test
  public void getPdxTypeForClassSucceedsWithExistingPdxType() {
    when(typeRegistry.getExistingTypeForClass(PdxClassDummy.class)).thenReturn(pdxType);

    PdxType result = config.getPdxTypeForClass(cache, PdxClassDummy.class);
    verify(config, never()).generatePdxTypeForClass(cache, typeRegistry, PdxClassDummy.class);
    assertThat(result).isEqualTo(pdxType);
  }

  @Test
  public void getPdxTypeForClassSucceedsWithGeneratingPdxType() {
    when(typeRegistry.getExistingTypeForClass(PdxClassDummy.class)).thenReturn(null)
        .thenReturn(pdxType);

    PdxType result = config.getPdxTypeForClass(cache, PdxClassDummy.class);
    verify(config, times(1)).generatePdxTypeForClass(cache, typeRegistry, PdxClassDummy.class);
    verify(cache, times(1)).registerPdxMetaData(any());
    assertThat(result).isEqualTo(pdxType);
  }

  @Test
  public void getPdxTypeForClassSucceedsWithGivenNonPdxUsesReflectionBasedAutoSerializer() {
    when(typeRegistry.getExistingTypeForClass(PdxClassDummy.class)).thenReturn(null)
        .thenReturn(pdxType);

    SerializationException ex = new SerializationException("test");
    doThrow(ex).when(cache).registerPdxMetaData(any());

    ReflectionBasedAutoSerializer serializer = mock(ReflectionBasedAutoSerializer.class);
    PdxWriter pdxWriter = mock(PdxWriter.class);
    String domainClassNameInAutoSerializer = "\\Q" + PdxClassDummy.class.getName() + "\\E";
    doReturn(serializer).when(config)
        .getReflectionBasedAutoSerializer(domainClassNameInAutoSerializer);
    doReturn(pdxWriter).when(config).createPdxWriter(same(typeRegistry), any());
    when(serializer.toData(any(), same(pdxWriter))).thenReturn(true);

    PdxType result = config.getPdxTypeForClass(cache, PdxClassDummy.class);
    verify(config, times(1)).generatePdxTypeForClass(cache, typeRegistry, PdxClassDummy.class);
    verify(cache, times(1)).registerPdxMetaData(any());
    verify(config, times(1)).getReflectionBasedAutoSerializer(domainClassNameInAutoSerializer);
    assertThat(result).isEqualTo(pdxType);
  }

  @Test
  public void getPdxTypeForClassThrowsExceptionWhenGivenPdxRegistrationFailsAndReflectionBasedAutoSerializer() {
    when(typeRegistry.getExistingTypeForClass(PdxClassDummy.class)).thenReturn(null);

    SerializationException ex = new SerializationException("test");
    doThrow(ex).when(cache).registerPdxMetaData(any());

    ReflectionBasedAutoSerializer serializer = mock(ReflectionBasedAutoSerializer.class);
    PdxWriter pdxWriter = mock(PdxWriter.class);
    String domainClassNameInAutoSerializer = "\\Q" + PdxClassDummy.class.getName() + "\\E";
    doReturn(serializer).when(config)
        .getReflectionBasedAutoSerializer(domainClassNameInAutoSerializer);
    doReturn(pdxWriter).when(config).createPdxWriter(same(typeRegistry), any());
    when(serializer.toData(any(), same(pdxWriter))).thenReturn(false);

    Throwable throwable =
        catchThrowable(() -> config.getPdxTypeForClass(cache, PdxClassDummy.class));
    verify(config, times(1)).generatePdxTypeForClass(cache, typeRegistry, PdxClassDummy.class);
    verify(cache, times(1)).registerPdxMetaData(any());
    verify(config, times(1)).getReflectionBasedAutoSerializer(domainClassNameInAutoSerializer);
    assertThat(throwable).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
        String.format(
            "Could not generate a PdxType using the ReflectionBasedAutoSerializer for the class  %s after failing to register pdx metadata due to %s. Check the server log for details.",
            PdxClassDummy.class.getName(), ex.getMessage()));
  }

  @Test
  public void getPdxTypeForClassThrowsExceptionWhenGivenPdxSerializableWithNoZeroArgConstructor() {
    Throwable throwable =
        catchThrowable(() -> config.getPdxTypeForClass(cache, PdxClassDummyNoZeroArg.class));
    verify(config, times(1)).generatePdxTypeForClass(cache, typeRegistry,
        PdxClassDummyNoZeroArg.class);
    assertThat(throwable).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(
        String.format(
            "Could not generate a PdxType for the class %s because it did not have a public zero arg constructor.",
            PdxClassDummyNoZeroArg.class.getName()));
  }
}
