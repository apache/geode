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

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import com.healthmarketscience.rmiio.RemoteInputStream;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.SerializationException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataManager;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;

public class CreateMappingPreconditionCheckFunctionTest {

  private static final String REGION_NAME = "testRegion";
  private static final String PDX_CLASS_NAME = "testPdxClassName";
  private static final String DATA_SOURCE_NAME = "testDataSourceName";
  private static final String MEMBER_NAME = "testMemberName";

  private RegionMapping regionMapping;
  private FunctionContext<Object[]> context;
  private InternalCache cache;
  private TypeRegistry typeRegistry;
  private TableMetaDataManager tableMetaDataManager;
  private TableMetaDataView tableMetaDataView;
  private DataSource dataSource;
  private PdxType pdxType = mock(PdxType.class);
  private String remoteInputStreamName;
  private RemoteInputStream remoteInputStream;
  private final Object[] inputArgs = new Object[3];

  private CreateMappingPreconditionCheckFunction function;

  public static class PdxClassDummy {
  }

  public static class PdxClassDummyNoZeroArg {
    public PdxClassDummyNoZeroArg(@SuppressWarnings("unused") int arg) {}
  }

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws SQLException, ClassNotFoundException {
    context = mock(FunctionContext.class);
    ResultSender<Object> resultSender = mock(ResultSender.class);
    cache = mock(InternalCache.class);
    typeRegistry = mock(TypeRegistry.class);
    when(cache.getPdxRegistry()).thenReturn(typeRegistry);
    regionMapping = mock(RegionMapping.class);
    remoteInputStreamName = null;
    remoteInputStream = null;
    setupInputArgs();

    when(regionMapping.getRegionName()).thenReturn(REGION_NAME);
    when(regionMapping.getPdxName()).thenReturn(PDX_CLASS_NAME);
    when(regionMapping.getDataSourceName()).thenReturn(DATA_SOURCE_NAME);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(context.getArguments()).thenReturn(inputArgs);
    when(context.getMemberName()).thenReturn(MEMBER_NAME);

    dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(typeRegistry.getExistingTypeForClass(PdxClassDummy.class)).thenReturn(pdxType);
    tableMetaDataManager = mock(TableMetaDataManager.class);
    tableMetaDataView = mock(TableMetaDataView.class);
    when(tableMetaDataManager.getTableMetaDataView(connection, regionMapping))
        .thenReturn(tableMetaDataView);
    setupFunction();
  }

  private void setupInputArgs() {
    inputArgs[0] = regionMapping;
    inputArgs[1] = remoteInputStreamName;
    inputArgs[2] = remoteInputStream;
  }

  private void setupFunction() throws ClassNotFoundException {
    function = spy(CreateMappingPreconditionCheckFunction.class);
    doReturn(dataSource).when(function).getDataSource(DATA_SOURCE_NAME);
    doReturn(PdxClassDummy.class).when(function).loadClass(PDX_CLASS_NAME);
    doReturn(null).when(function).getReflectionBasedAutoSerializer(any());
    doReturn(null).when(function).createPdxWriter(same(typeRegistry), any());
    doReturn(tableMetaDataManager).when(function).getTableMetaDataManager();
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
    Serializable original = new CreateMappingPreconditionCheckFunction();

    Object copy = SerializationUtils.clone(original);

    assertThat(copy).isNotSameAs(original)
        .isInstanceOf(CreateMappingPreconditionCheckFunction.class);
  }

  @Test
  public void executeFunctionThrowsIfDataSourceDoesNotExist() {
    doReturn(null).when(function).getDataSource(DATA_SOURCE_NAME);

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage("JDBC data-source named \"" + DATA_SOURCE_NAME
            + "\" not found. Create it with gfsh 'create data-source --pooled --name="
            + DATA_SOURCE_NAME + "'.");
  }

  @Test
  public void executeFunctionThrowsIfDataSourceGetConnectionThrows() throws SQLException {
    String reason = "connection failed";
    when(dataSource.getConnection()).thenThrow(new SQLException(reason));

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class).hasMessageContaining(reason);
  }

  @Test
  public void executeFunctionThrowsIfClassNotFound() throws ClassNotFoundException {
    ClassNotFoundException ex = new ClassNotFoundException("class not found");
    doThrow(ex).when(function).loadClass(PDX_CLASS_NAME);

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage("The pdx class \"" + PDX_CLASS_NAME
            + "\" could not be loaded because: java.lang.ClassNotFoundException: class not found");
  }

  @Test
  public void executeFunctionReturnsNoFieldMappingsIfNoColumns() {
    Set<String> columnNames = Collections.emptySet();
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    ArrayList<FieldMapping> fieldsMappings = getFieldMappings(result);
    assertThat(fieldsMappings).isEmpty();
  }

  @Test
  public void executeFunctionReturnsFieldMappingsThatMatchTableMetaData() {
    Set<String> columnNames = new LinkedHashSet<>(Arrays.asList("col1", "col2"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    when(tableMetaDataView.isColumnNullable("col2")).thenReturn(true);
    when(tableMetaDataView.getColumnDataType("col2")).thenReturn(JDBCType.DATE);
    when(pdxType.getFieldCount()).thenReturn(2);
    PdxField field1 = mock(PdxField.class);
    when(field1.getFieldName()).thenReturn("col1");
    when(field1.getFieldType()).thenReturn(FieldType.DATE);
    PdxField field2 = mock(PdxField.class);
    when(field2.getFieldName()).thenReturn("col2");
    when(field2.getFieldType()).thenReturn(FieldType.DATE);
    List<PdxField> pdxFields = Arrays.asList(field1, field2);
    when(pdxType.getFields()).thenReturn(pdxFields);

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    ArrayList<FieldMapping> fieldsMappings = getFieldMappings(result);
    assertThat(fieldsMappings).hasSize(2);
    assertThat(fieldsMappings.get(0))
        .isEqualTo(
            new FieldMapping("col1", FieldType.DATE.name(), "col1", JDBCType.DATE.name(), false));
    assertThat(fieldsMappings.get(1))
        .isEqualTo(
            new FieldMapping("col2", FieldType.DATE.name(), "col2", JDBCType.DATE.name(), true));
  }

  @Test
  public void executeFunctionReturnsFieldMappingsThatMatchTableMetaDataAndExistingPdxField() {
    Set<String> columnNames = new LinkedHashSet<>(singletonList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    PdxField pdxField1 = mock(PdxField.class);
    when(pdxField1.getFieldName()).thenReturn("COL1");
    when(pdxField1.getFieldType()).thenReturn(FieldType.LONG);
    when(pdxType.getFieldCount()).thenReturn(1);
    when(pdxType.getFields()).thenReturn(singletonList(pdxField1));

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    ArrayList<FieldMapping> fieldsMappings = getFieldMappings(result);
    assertThat(fieldsMappings).hasSize(1);
    assertThat(fieldsMappings.get(0))
        .isEqualTo(
            new FieldMapping("COL1", FieldType.LONG.name(), "col1", JDBCType.DATE.name(), false));
  }

  @Test
  public void executeFunctionGivenPdxSerializableCallsRegisterPdxMetaData() {
    Set<String> columnNames = new LinkedHashSet<>(singletonList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    PdxField pdxField1 = mock(PdxField.class);
    when(pdxField1.getFieldName()).thenReturn("COL1");
    when(pdxField1.getFieldType()).thenReturn(FieldType.LONG);
    when(pdxType.getFieldCount()).thenReturn(1);
    when(pdxType.getFields()).thenReturn(singletonList(pdxField1));
    when(typeRegistry.getExistingTypeForClass(PdxClassDummy.class)).thenReturn(null)
        .thenReturn(pdxType);

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    verify(cache).registerPdxMetaData(any());
    ArrayList<FieldMapping> fieldsMappings = getFieldMappings(result);
    assertThat(fieldsMappings).hasSize(1);
    assertThat(fieldsMappings.get(0))
        .isEqualTo(
            new FieldMapping("COL1", FieldType.LONG.name(), "col1", JDBCType.DATE.name(), false));
  }

  @Test
  public void executeFunctionThrowsGivenPdxSerializableWithNoZeroArgConstructor()
      throws Exception {
    Set<String> columnNames = new LinkedHashSet<>(singletonList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    PdxField pdxField1 = mock(PdxField.class);
    when(pdxField1.getFieldName()).thenReturn("COL1");
    when(pdxField1.getFieldType()).thenReturn(FieldType.LONG);
    when(pdxType.getFieldCount()).thenReturn(1);
    when(pdxType.getFields()).thenReturn(singletonList(pdxField1));
    doReturn(PdxClassDummyNoZeroArg.class).when(function).loadClass(PDX_CLASS_NAME);
    when(typeRegistry.getExistingTypeForClass(PdxClassDummyNoZeroArg.class)).thenReturn(null);

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage(
            "Could not generate a PdxType for the class org.apache.geode.connectors.jdbc.internal.cli.CreateMappingPreconditionCheckFunctionTest$PdxClassDummyNoZeroArg because it did not have a public zero arg constructor. Details: java.lang.NoSuchMethodException: org.apache.geode.connectors.jdbc.internal.cli.CreateMappingPreconditionCheckFunctionTest$PdxClassDummyNoZeroArg.<init>()");
  }

  @Test
  public void executeFunctionGivenNonPdxUsesReflectionBasedAutoSerializer() {
    Set<String> columnNames = new LinkedHashSet<>(singletonList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    PdxField pdxField1 = mock(PdxField.class);
    when(pdxField1.getFieldName()).thenReturn("COL1");
    when(pdxField1.getFieldType()).thenReturn(FieldType.LONG);
    when(pdxType.getFieldCount()).thenReturn(1);
    when(pdxType.getFields()).thenReturn(singletonList(pdxField1));
    when(typeRegistry.getExistingTypeForClass(PdxClassDummy.class)).thenReturn(null)
        .thenReturn(pdxType);
    String domainClassNameInAutoSerializer = "\\Q" + PdxClassDummy.class.getName() + "\\E";
    ReflectionBasedAutoSerializer reflectionedBasedAutoSerializer =
        mock(ReflectionBasedAutoSerializer.class);
    PdxWriter pdxWriter = mock(PdxWriter.class);
    when(reflectionedBasedAutoSerializer.toData(any(), same(pdxWriter))).thenReturn(true);
    doReturn(reflectionedBasedAutoSerializer).when(function)
        .getReflectionBasedAutoSerializer(domainClassNameInAutoSerializer);
    doReturn(pdxWriter).when(function).createPdxWriter(same(typeRegistry), any());
    SerializationException ex = new SerializationException("test");
    doThrow(ex).when(cache).registerPdxMetaData(any());

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    verify(function).getReflectionBasedAutoSerializer(domainClassNameInAutoSerializer);
    ArrayList<FieldMapping> fieldsMappings = getFieldMappings(result);
    assertThat(fieldsMappings).hasSize(1);
    assertThat(fieldsMappings.get(0))
        .isEqualTo(
            new FieldMapping("COL1", FieldType.LONG.name(), "col1", JDBCType.DATE.name(), false));
  }

  @SuppressWarnings("unchecked")
  private ArrayList<FieldMapping> getFieldMappings(CliFunctionResult result) {
    Object[] outputs = (Object[]) result.getResultObject();
    return (ArrayList<FieldMapping>) outputs[1];
  }

  @Test
  public void executeFunctionThrowsGivenPdxRegistrationFailsAndReflectionBasedAutoSerializerThatReturnsFalse() {
    Set<String> columnNames = new LinkedHashSet<>(singletonList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    PdxField pdxField1 = mock(PdxField.class);
    when(pdxField1.getFieldName()).thenReturn("COL1");
    when(pdxField1.getFieldType()).thenReturn(FieldType.LONG);
    when(pdxType.getFieldCount()).thenReturn(1);
    when(pdxType.getFields()).thenReturn(singletonList(pdxField1));
    when(typeRegistry.getExistingTypeForClass(PdxClassDummy.class)).thenReturn(null)
        .thenReturn(pdxType);
    String domainClassNameInAutoSerializer = "\\Q" + PdxClassDummy.class.getName() + "\\E";
    ReflectionBasedAutoSerializer reflectionedBasedAutoSerializer =
        mock(ReflectionBasedAutoSerializer.class);
    PdxWriter pdxWriter = mock(PdxWriter.class);
    when(reflectionedBasedAutoSerializer.toData(any(), same(pdxWriter))).thenReturn(false);
    doReturn(reflectionedBasedAutoSerializer).when(function)
        .getReflectionBasedAutoSerializer(domainClassNameInAutoSerializer);
    SerializationException ex = new SerializationException("test");
    doThrow(ex).when(cache).registerPdxMetaData(any());
    doReturn(reflectionedBasedAutoSerializer).when(function)
        .getReflectionBasedAutoSerializer(PdxClassDummy.class.getName());
    doReturn(pdxWriter).when(function).createPdxWriter(same(typeRegistry), any());

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage(
            "Could not generate a PdxType using the ReflectionBasedAutoSerializer for the class  org.apache.geode.connectors.jdbc.internal.cli.CreateMappingPreconditionCheckFunctionTest$PdxClassDummy after failing to register pdx metadata due to test. Check the server log for details.");
  }

  @Test
  public void executeFunctionThrowsGivenExistingPdxTypeWithMultipleInexactMatches() {
    Set<String> columnNames = new LinkedHashSet<>(singletonList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    when(pdxType.getFieldCount()).thenReturn(1);
    PdxField pdxField1 = mock(PdxField.class);
    when(pdxField1.getFieldName()).thenReturn("COL1");
    when(pdxField1.getFieldType()).thenReturn(FieldType.DATE);
    PdxField pdxField2 = mock(PdxField.class);
    when(pdxField2.getFieldName()).thenReturn("Col1");
    when(pdxField2.getFieldType()).thenReturn(FieldType.DATE);
    when(pdxType.getFields()).thenReturn(Arrays.asList(pdxField1, pdxField2));

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage("More than one PDX field name matched the column name \"col1\"");
  }

  @Test
  public void executeFunctionThrowsGivenExistingPdxTypeWithNoMatches() {
    Set<String> columnNames = new LinkedHashSet<>(singletonList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    when(pdxType.getFieldCount()).thenReturn(1);
    PdxField pdxField1 = mock(PdxField.class);
    when(pdxField1.getFieldName()).thenReturn("pdxCOL1");
    when(pdxField1.getFieldType()).thenReturn(FieldType.DATE);
    PdxField pdxField2 = mock(PdxField.class);
    when(pdxField2.getFieldName()).thenReturn("pdxCol1");
    when(pdxField2.getFieldType()).thenReturn(FieldType.DATE);
    when(pdxType.getFields()).thenReturn(Arrays.asList(pdxField1, pdxField2));

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage("No PDX field name matched the column name \"col1\"");
  }

  @Test
  public void executeFunctionThrowsGivenExistingPdxTypeWithWrongNumberOfFields() {
    Set<String> columnNames = new LinkedHashSet<>(singletonList("col1"));
    when(tableMetaDataView.getColumnNames()).thenReturn(columnNames);
    when(tableMetaDataView.isColumnNullable("col1")).thenReturn(false);
    when(tableMetaDataView.getColumnDataType("col1")).thenReturn(JDBCType.DATE);
    when(pdxType.getFieldCount()).thenReturn(2);

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessage(
            "The table and pdx class must have the same number of columns/fields. But the table has 1 columns and the pdx class has 2 fields.");
  }

  @Test
  public void executeFunctionReturnsResultWithCorrectMemberName() {
    when(regionMapping.getIds()).thenReturn("myId");

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getMemberIdOrName()).isEqualTo(MEMBER_NAME);
  }

  @Test
  public void executeFunctionReturnsNullInSlotZeroIfRegionMappingHasIds() {
    when(regionMapping.getIds()).thenReturn("myId");

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    Object[] outputs = (Object[]) result.getResultObject();
    assertThat(outputs[0]).isNull();
  }

  @Test
  public void executeFunctionReturnsViewsKeyColumnsInSlotZeroIfRegionMappingHasNullIds() {
    when(regionMapping.getIds()).thenReturn(null);
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(Arrays.asList("keyCol1", "keyCol2"));

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    Object[] outputs = (Object[]) result.getResultObject();
    assertThat(outputs[0]).isEqualTo("keyCol1,keyCol2");
  }

  @Test
  public void executeFunctionReturnsViewsKeyColumnsInSlotZeroIfRegionMappingHasEmptyIds() {
    when(regionMapping.getIds()).thenReturn("");
    when(tableMetaDataView.getKeyColumnNames()).thenReturn(singletonList("keyCol1"));

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    Object[] outputs = (Object[]) result.getResultObject();
    assertThat(outputs[0]).isEqualTo("keyCol1");
  }

  @Test
  public void executeFunctionThrowsGivenRemoteInputStreamAndLoadClassThatThrowsClassNotFound()
      throws ClassNotFoundException {
    remoteInputStreamName = "remoteInputStreamName";
    remoteInputStream = mock(RemoteInputStream.class);
    setupInputArgs();
    doThrow(ClassNotFoundException.class).when(function).loadClass(eq(PDX_CLASS_NAME), any());

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessageContaining(
            "The pdx class \"" + PDX_CLASS_NAME + "\" could not be loaded because: ")
        .hasMessageContaining("ClassNotFoundException");
    verify(function).createTemporaryDirectory(any());
    verify(function).deleteDirectory(any());
  }

  @Test
  public void executeFunctionThrowsGivenRemoteInputStreamAndcreateTempDirectoryException()
      throws IOException {
    remoteInputStreamName = "remoteInputStreamName";
    remoteInputStream = mock(RemoteInputStream.class);
    setupInputArgs();
    doThrow(IOException.class).when(function).createTempDirectory(any());

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessageContaining(
            "Could not create a temporary directory with the prefix \"pdx-class-dir-\" because: ")
        .hasMessageContaining("IOException");
    verify(function, never()).deleteDirectory(any());
    verify(remoteInputStream, never()).close(true);
  }

  @Test
  public void executeFunctionThrowsGivenRemoteInputStreamAndCopyFileIOException()
      throws IOException {
    remoteInputStreamName = "remoteInputStreamName";
    remoteInputStream = mock(RemoteInputStream.class);
    setupInputArgs();
    doThrow(IOException.class).when(function).copyFile(any(), any());

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(JdbcConnectorException.class)
        .hasMessageContaining(
            "The pdx class file \"" + remoteInputStreamName
                + "\" could not be copied to a temporary file, because: ")
        .hasMessageContaining("IOException");
    verify(function).createTemporaryDirectory(any());
    verify(function).deleteDirectory(any());
    verify(remoteInputStream).close(true);
  }

  @Test
  public void executeFunctionReturnsSuccessGivenRemoteInputStreamClassAndPackageName()
      throws ClassNotFoundException {
    remoteInputStreamName = "remoteInputStreamName.class";
    remoteInputStream = mock(RemoteInputStream.class);
    setupInputArgs();
    String PDX_CLASS_NAME_WITH_PACKAGE = "foo.bar.MyPdxClassName";
    when(regionMapping.getPdxName()).thenReturn(PDX_CLASS_NAME_WITH_PACKAGE);
    doReturn(PdxClassDummy.class).when(function).loadClass(eq(PDX_CLASS_NAME_WITH_PACKAGE), any());

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    verify(function).createTemporaryDirectory(any());
    verify(function).deleteDirectory(any());
  }

  @Test
  public void executeFunctionReturnsSuccessGivenRemoteInputStreamJar()
      throws ClassNotFoundException {
    remoteInputStreamName = "remoteInputStreamName.jar";
    remoteInputStream = mock(RemoteInputStream.class);
    setupInputArgs();
    doReturn(PdxClassDummy.class).when(function).loadClass(eq(PDX_CLASS_NAME), any());

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    verify(function).createTemporaryDirectory(any());
    verify(function).deleteDirectory(any());
  }

}
