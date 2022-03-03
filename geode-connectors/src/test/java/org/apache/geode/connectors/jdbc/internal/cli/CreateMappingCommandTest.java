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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.exporter.RemoteStreamExporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheConfig.AsyncEventQueue;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.JdbcAsyncWriter;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.ManagementAgent;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class CreateMappingCommandTest {

  private InternalCache cache;
  private CreateMappingCommand createRegionMappingCommand;

  private String regionName;
  private String dataSourceName;
  private String tableName;
  private String pdxClass;
  private String pdxClassFile;
  private String group1Name;
  private String group2Name;
  private Set<InternalDistributedMember> members;
  private CliFunctionResult preconditionCheckResults;
  private final ArrayList<FieldMapping> fieldMappings = new ArrayList<>();

  private final Object[] preconditionOutput = new Object[] {null, fieldMappings, null};
  private List<CliFunctionResult> results;
  private CliFunctionResult successFunctionResult;
  private RegionMapping mapping;
  private final Object[] arguments = new Object[2];
  private CacheConfig cacheConfig;
  RegionConfig matchingRegion;
  RegionAttributesType matchingRegionAttributes;

  @Before
  public void setup() {
    regionName = "regionName";
    dataSourceName = "connection";
    tableName = "testTable";
    pdxClass = "myPdxClass";
    pdxClassFile = null;
    group1Name = "group1";
    group2Name = "group2";
    cache = mock(InternalCache.class);
    DistributionManager dm = mock(DistributionManager.class);
    when(cache.getDistributionManager()).thenReturn(dm);
    members = new HashSet<>();
    members.add(mock(InternalDistributedMember.class));
    createRegionMappingCommand = spy(CreateMappingCommand.class);
    createRegionMappingCommand.setCache(cache);
    results = new ArrayList<>();
    successFunctionResult = mock(CliFunctionResult.class);
    when(successFunctionResult.isSuccessful()).thenReturn(true);
    preconditionCheckResults = mock(CliFunctionResult.class);
    when(preconditionCheckResults.isSuccessful()).thenReturn(true);
    when(preconditionCheckResults.getResultObject()).thenReturn(preconditionOutput);
    doReturn(preconditionCheckResults).when(createRegionMappingCommand)
        .executeFunctionAndGetFunctionResult(any(), any(), any());
    doReturn(results).when(createRegionMappingCommand).executeAndGetFunctionResult(any(), any(),
        any());
    doReturn(members).when(createRegionMappingCommand).findMembers(any(), any());

    mapping = mock(RegionMapping.class);
    when(mapping.getRegionName()).thenReturn(regionName);

    cacheConfig = mock(CacheConfig.class);

    matchingRegion = mock(RegionConfig.class);
    when(matchingRegion.getType()).thenReturn("PARTITION");
    when(matchingRegion.getName()).thenReturn(regionName);
    matchingRegionAttributes = mock(RegionAttributesType.class);
    when(matchingRegionAttributes.getDataPolicy()).thenReturn(RegionAttributesDataPolicy.REPLICATE);
    when(matchingRegion.getRegionAttributes()).thenReturn(matchingRegionAttributes);

    arguments[0] = mapping;
    arguments[1] = false;
  }

  private void setupRequiredPreconditions() {
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
  }

  private void setupRequiredPreconditionsForGroup() {
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService.getCacheConfig(group1Name)).thenReturn(cacheConfig);
    when(configurationPersistenceService.getCacheConfig(group2Name)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
  }

  @Test
  public void createsMappingReturnsStatusOKWhenFunctionResultSuccess() throws IOException {
    setupRequiredPreconditions();
    results.add(successFunctionResult);
    String ids = "ids";
    String catalog = "catalog";
    String schema = "schema";

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, ids, catalog, schema, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    Object[] results = (Object[]) result.getConfigObject();
    RegionMapping regionMapping = (RegionMapping) results[0];
    boolean synchronous = (boolean) results[1];
    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo(regionName);
    assertThat(regionMapping.getDataSourceName()).isEqualTo(dataSourceName);
    assertThat(regionMapping.getTableName()).isEqualTo(tableName);
    assertThat(regionMapping.getPdxName()).isEqualTo(pdxClass);
    assertThat(regionMapping.getIds()).isEqualTo(ids);
    assertThat(regionMapping.getCatalog()).isEqualTo(catalog);
    assertThat(regionMapping.getSchema()).isEqualTo(schema);
    assertThat(synchronous).isFalse();
  }

  @Test
  public void createsMappingReturnsStatusOKWhenFunctionResultSuccessWithGroups()
      throws IOException {
    setupRequiredPreconditionsForGroup();
    results.add(successFunctionResult);
    String ids = "ids";
    String catalog = "catalog";
    String schema = "schema";
    String[] groups = {group1Name, group2Name};

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, ids, catalog, schema, false, groups);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    Object[] results = (Object[]) result.getConfigObject();
    RegionMapping regionMapping = (RegionMapping) results[0];
    boolean synchronous = (boolean) results[1];
    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo(regionName);
    assertThat(regionMapping.getDataSourceName()).isEqualTo(dataSourceName);
    assertThat(regionMapping.getTableName()).isEqualTo(tableName);
    assertThat(regionMapping.getPdxName()).isEqualTo(pdxClass);
    assertThat(regionMapping.getIds()).isEqualTo(ids);
    assertThat(regionMapping.getCatalog()).isEqualTo(catalog);
    assertThat(regionMapping.getSchema()).isEqualTo(schema);
    assertThat(regionMapping.getFieldMappings()).isEmpty();
    assertThat(synchronous).isFalse();
  }

  @Test
  public void createsMappingReturnsCorrectFieldMappings() throws IOException {
    setupRequiredPreconditions();
    results.add(successFunctionResult);
    String ids = "ids";
    String catalog = "catalog";
    String schema = "schema";
    fieldMappings.add(new FieldMapping("pdx1", "pdx1type", "jdbc1", "jdbc1type", false));
    fieldMappings.add(new FieldMapping("pdx2", "pdx2type", "jdbc2", "jdbc2type", false));

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, ids, catalog, schema, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    Object[] results = (Object[]) result.getConfigObject();
    RegionMapping regionMapping = (RegionMapping) results[0];
    assertThat(regionMapping.getFieldMappings()).isEqualTo(fieldMappings);
  }

  @Test
  public void createsMappingWithPdxClassFileReturnsCorrectFieldMappings() throws IOException {
    RemoteInputStream remoteInputStream = setupPdxClassFile();
    setupRequiredPreconditions();
    results.add(successFunctionResult);
    String ids = "ids";
    String catalog = "catalog";
    String schema = "schema";
    fieldMappings.add(new FieldMapping("pdx1", "pdx1type", "jdbc1", "jdbc1type", false));
    fieldMappings.add(new FieldMapping("pdx2", "pdx2type", "jdbc2", "jdbc2type", false));

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, ids, catalog, schema, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    Object[] results = (Object[]) result.getConfigObject();
    RegionMapping regionMapping = (RegionMapping) results[0];
    assertThat(regionMapping.getFieldMappings()).isEqualTo(fieldMappings);
    ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);
    verify(createRegionMappingCommand).executeFunctionAndGetFunctionResult(any(),
        argumentCaptor.capture(), any());
    Object[] args = argumentCaptor.getValue();
    assertThat(args).hasSize(3);
    assertThat(args[0]).isEqualTo(regionMapping);
    assertThat(args[1]).isEqualTo("myPdxClassFilePath");
    assertThat(args[2]).isSameAs(remoteInputStream);
  }

  @Test
  public void createsMappingWithPdxClassFileAndFilePathFromShellIsEmptyListThrowsIllegalStateException()
      throws IOException {
    setupPdxClassFile();
    doReturn(Collections.emptyList()).when(createRegionMappingCommand).getFilePathFromShell();
    setupRequiredPreconditions();
    String ids = "ids";
    String catalog = "catalog";
    String schema = "schema";

    assertThatThrownBy(() -> createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, ids, catalog, schema, false, null))
            .isInstanceOf(IllegalStateException.class);
  }

  private RemoteInputStream setupPdxClassFile() throws FileNotFoundException, RemoteException {
    pdxClassFile = "myPdxClassFile";
    String tempPdxClassFilePath = "myPdxClassFilePath";
    List<String> list = Collections.singletonList(tempPdxClassFilePath);
    doReturn(list).when(createRegionMappingCommand).getFilePathFromShell();
    SystemManagementService systemManagementService = mock(SystemManagementService.class);
    doReturn(systemManagementService).when(createRegionMappingCommand).getManagementService();
    ManagementAgent managementAgent = mock(ManagementAgent.class);
    when(systemManagementService.getManagementAgent()).thenReturn(managementAgent);
    RemoteStreamExporter remoteStreamExporter = mock(RemoteStreamExporter.class);
    when(managementAgent.getRemoteStreamExporter()).thenReturn(remoteStreamExporter);
    SimpleRemoteInputStream simpleRemoteInputStream = mock(SimpleRemoteInputStream.class);
    doReturn(simpleRemoteInputStream).when(createRegionMappingCommand)
        .createSimpleRemoteInputStream(tempPdxClassFilePath);
    RemoteInputStream remoteInputStream = mock(RemoteInputStream.class);
    when(remoteStreamExporter.export(simpleRemoteInputStream)).thenReturn(remoteInputStream);
    return remoteInputStream;
  }

  @Test
  public void createsMappingReturnsRegionMappingWithComputedIds() throws IOException {
    setupRequiredPreconditions();
    results.add(successFunctionResult);
    String ids = "does not matter";
    String catalog = "catalog";
    String schema = "schema";
    String computedIds = "id1,id2";
    preconditionOutput[0] = computedIds;

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, ids, catalog, schema, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    Object[] results = (Object[]) result.getConfigObject();
    RegionMapping regionMapping = (RegionMapping) results[0];
    assertThat(regionMapping.getIds()).isEqualTo(computedIds);
  }

  @Test
  public void createsMappingReturnsErrorIfPreconditionCheckErrors() throws IOException {
    setupRequiredPreconditions();
    results.add(successFunctionResult);
    String ids = "ids";
    String catalog = "catalog";
    String schema = "schema";
    when(preconditionCheckResults.isSuccessful()).thenReturn(false);
    when(preconditionCheckResults.getStatusMessage()).thenReturn("precondition check failed");

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, ids, catalog, schema, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("precondition check failed");
  }

  @Test
  public void createsMappingWithRegionPathCreatesMappingWithSlashRemoved() throws IOException {
    setupRequiredPreconditions();
    results.add(successFunctionResult);

    ResultModel result =
        createRegionMappingCommand.createMapping(SEPARATOR + regionName, dataSourceName,
            tableName, pdxClass, pdxClassFile, false, null, null, null, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    Object[] results = (Object[]) result.getConfigObject();
    RegionMapping regionMapping = (RegionMapping) results[0];
    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo(regionName);
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenFunctionResultIsEmpty() throws IOException {
    setupRequiredPreconditions();
    results.clear();

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, null, null, null, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenClusterConfigIsDisabled() throws IOException {
    results.add(successFunctionResult);
    doReturn(null).when(createRegionMappingCommand).getConfigurationPersistenceService();

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, null, null, null, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("Cluster Configuration must be enabled.");
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenClusterConfigDoesNotContainRegion()
      throws IOException {
    results.add(successFunctionResult);
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(cacheConfig);
    when(cacheConfig.getRegions()).thenReturn(Collections.emptyList());

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, null, null, null, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString())
        .contains("A region named " + regionName + " must already exist.");
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenRegionMappingExists() throws IOException {
    results.add(successFunctionResult);
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    RegionAttributesType loaderAttribute = mock(RegionAttributesType.class);
    DeclarableType loaderDeclarable = mock(DeclarableType.class);
    when(loaderDeclarable.getClassName()).thenReturn(null);
    when(loaderAttribute.getCacheLoader()).thenReturn(loaderDeclarable);
    when(matchingRegion.getRegionAttributes()).thenReturn(loaderAttribute);
    List<CacheElement> customList = new ArrayList<>();
    RegionMapping existingMapping = mock(RegionMapping.class);
    customList.add(existingMapping);
    when(matchingRegion.getCustomRegionElements()).thenReturn(customList);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, null, null, null, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("A JDBC mapping for " + regionName + " already exists.");
  }

  @Test
  public void createsMappingIsSkippedWhenRegionMappingExistsWithIfNotExistsTrue()
      throws IOException {
    results.add(successFunctionResult);
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    RegionAttributesType loaderAttribute = mock(RegionAttributesType.class);
    DeclarableType loaderDeclarable = mock(DeclarableType.class);
    when(loaderDeclarable.getClassName()).thenReturn(null);
    when(loaderAttribute.getCacheLoader()).thenReturn(loaderDeclarable);
    when(matchingRegion.getRegionAttributes()).thenReturn(loaderAttribute);
    List<CacheElement> customList = new ArrayList<>();
    RegionMapping existingMapping = mock(RegionMapping.class);
    customList.add(existingMapping);
    when(matchingRegion.getCustomRegionElements()).thenReturn(customList);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, null, null, null, true, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
    assertThat(result.toString())
        .contains("Skipping: A JDBC mapping for " + regionName + " already exists.");
  }

  @Test
  public void createsMappingReturnsStatusERRORWhenClusterConfigRegionHasLoader()
      throws IOException {
    results.add(successFunctionResult);
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    RegionAttributesType loaderAttribute = mock(RegionAttributesType.class);
    DeclarableType loaderDeclarable = mock(DeclarableType.class);
    when(loaderDeclarable.getClassName()).thenReturn("MyCacheLoaderClass");
    when(loaderAttribute.getCacheLoader()).thenReturn(loaderDeclarable);
    when(matchingRegion.getRegionAttributes()).thenReturn(loaderAttribute);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, null, null, null, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("The existing region " + regionName
        + " must not already have a cache-loader, but it has MyCacheLoaderClass");
  }

  @Test
  public void createMappingWithSynchronousReturnsStatusERRORWhenClusterConfigRegionHasWriter()
      throws IOException {
    results.add(successFunctionResult);
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    RegionAttributesType writerAttribute = mock(RegionAttributesType.class);
    DeclarableType writerDeclarable = mock(DeclarableType.class);
    when(writerDeclarable.getClassName()).thenReturn("MyCacheWriterClass");
    when(writerAttribute.getCacheWriter()).thenReturn(writerDeclarable);
    when(matchingRegion.getRegionAttributes()).thenReturn(writerAttribute);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, true, null, null, null, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString()).contains("The existing region " + regionName
        + " must not already have a cache-writer, but it has MyCacheWriterClass");
  }

  @Test
  public void createMappingWithSynchronousReturnsStatusOKWhenAsycnEventQueueAlreadyExists()
      throws IOException {
    results.add(successFunctionResult);
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    RegionAttributesType loaderAttribute = mock(RegionAttributesType.class);
    when(loaderAttribute.getCacheLoader()).thenReturn(null);
    when(matchingRegion.getRegionAttributes()).thenReturn(loaderAttribute);
    List<AsyncEventQueue> asyncEventQueues = new ArrayList<>();
    AsyncEventQueue matchingQueue = mock(AsyncEventQueue.class);
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    when(matchingQueue.getId()).thenReturn(queueName);
    asyncEventQueues.add(matchingQueue);
    when(cacheConfig.getAsyncEventQueues()).thenReturn(asyncEventQueues);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, true, null, null, null, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.OK);
  }


  @Test
  public void createsMappingReturnsStatusERRORWhenAsycnEventQueueAlreadyExists()
      throws IOException {
    results.add(successFunctionResult);
    ConfigurationPersistenceService configurationPersistenceService =
        mock(ConfigurationPersistenceService.class);
    doReturn(configurationPersistenceService).when(createRegionMappingCommand)
        .getConfigurationPersistenceService();
    when(configurationPersistenceService
        .getCacheConfig(ConfigurationPersistenceService.CLUSTER_CONFIG)).thenReturn(cacheConfig);
    List<RegionConfig> list = new ArrayList<>();
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    RegionAttributesType loaderAttribute = mock(RegionAttributesType.class);
    when(loaderAttribute.getCacheLoader()).thenReturn(null);
    when(matchingRegion.getRegionAttributes()).thenReturn(loaderAttribute);
    List<AsyncEventQueue> asyncEventQueues = new ArrayList<>();
    AsyncEventQueue matchingQueue = mock(AsyncEventQueue.class);
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    when(matchingQueue.getId()).thenReturn(queueName);
    asyncEventQueues.add(matchingQueue);
    when(cacheConfig.getAsyncEventQueues()).thenReturn(asyncEventQueues);

    ResultModel result = createRegionMappingCommand.createMapping(regionName, dataSourceName,
        tableName, pdxClass, pdxClassFile, false, null, null, null, false, null);

    assertThat(result.getStatus()).isSameAs(Result.Status.ERROR);
    assertThat(result.toString())
        .contains("An async-event-queue named " + queueName + " must not already exist.");
  }

  @Test
  public void updateClusterConfigWithNoRegionsDoesNotThrowException() {
    when(cacheConfig.getRegions()).thenReturn(Collections.emptyList());

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAddsMappingToRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(listCacheElements.size()).isEqualTo(1);
    assertThat(listCacheElements).contains(mapping);
  }

  @Test
  public void updateClusterConfigWithOneNonMatchingRegionDoesNotAddMapping() {
    List<RegionConfig> list = new ArrayList<>();
    RegionConfig nonMatchingRegion = mock(RegionConfig.class);
    when(nonMatchingRegion.getName()).thenReturn("nonMatchingRegion");
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(nonMatchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(nonMatchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(listCacheElements).isEmpty();
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionCreatesAsyncEventQueue() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(queueList.size()).isEqualTo(1);
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    AsyncEventQueue createdQueue = queueList.get(0);
    assertThat(createdQueue.getId()).isEqualTo(queueName);
    assertThat(createdQueue.isParallel()).isFalse();
    assertThat(createdQueue.getAsyncEventListener().getClassName())
        .isEqualTo(JdbcAsyncWriter.class.getName());
  }

  @Test
  public void updateClusterConfigWithOneMatchingPartitionedRegionCreatesParallelAsyncEventQueue() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.PARTITION);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(queueList.get(0).isParallel()).isTrue();
  }

  @Test
  public void updateClusterConfigWithOneMatchingPartitionedRegionRefidCreatesParallelAsyncEventQueue() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getDataPolicy()).thenReturn(null);
    when(matchingRegionAttributes.getRefid()).thenReturn(RegionShortcut.PARTITION.name());

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(queueList.get(0).isParallel()).isTrue();
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionCallsSetCacheLoader() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    verify(matchingRegionAttributes).setCacheLoader(any());
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndNullQueuesAddsAsyncEventQueueIdToRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn(null);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(matchingRegionAttributes).setAsyncEventQueueIds(argument.capture());
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    assertThat(argument.getValue()).isEqualTo(queueName);
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndEmptyQueuesAddsAsyncEventQueueIdToRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn("");

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(matchingRegionAttributes).setAsyncEventQueueIds(argument.capture());
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    assertThat(argument.getValue()).isEqualTo(queueName);
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndExistingQueuesAddsAsyncEventQueueIdToRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn("q1,q2");

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(matchingRegionAttributes).setAsyncEventQueueIds(argument.capture());
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    assertThat(argument.getValue()).isEqualTo("q1,q2," + queueName);
  }

  @Test
  public void updateClusterConfigWithOneMatchingRegionAndQueuesContainingDuplicateDoesNotModifyAsyncEventQueueIdOnRegion() {
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    String existingQueues = "q1," + queueName + ",q2";
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn(existingQueues);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    verify(matchingRegionAttributes, never()).setAsyncEventQueueIds(any());
  }

  @Test
  public void updateClusterConfigWithSynchronousSetsTheCacheWriterOnTheMatchingRegion() {
    arguments[1] = true;
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    verify(matchingRegionAttributes).setCacheWriter(any());
  }

  @Test
  public void updateClusterConfigWithSynchronousAndOneMatchingRegionAndExistingQueuesDoesNotAddsAsyncEventQueueIdToRegion() {
    arguments[1] = true;
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);
    when(matchingRegionAttributes.getAsyncEventQueueIds()).thenReturn("q1,q2");

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    verify(matchingRegionAttributes, never()).setAsyncEventQueueIds(any());
  }

  @Test
  public void updateClusterConfigWithAsycProxyRegionOnlyUpdateAEQId() {
    arguments[1] = false;
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    RegionAttributesType attributesType = mock(RegionAttributesType.class);
    when(matchingRegion.getRegionAttributes().getDataPolicy())
        .thenReturn(RegionAttributesDataPolicy.EMPTY);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);
    verify(matchingRegionAttributes, times(1)).setAsyncEventQueueIds(queueName);
    verify(matchingRegionAttributes, never()).setCacheWriter(any());
    verify(matchingRegionAttributes, never()).setCacheLoader(any());
  }

  @Test
  public void updateClusterConfigWithSynchronousAndOneMatchingRegionDoesNotCreateAsyncEventQueue() {
    arguments[1] = true;
    List<RegionConfig> list = new ArrayList<>();
    List<CacheElement> listCacheElements = new ArrayList<>();
    when(matchingRegion.getCustomRegionElements()).thenReturn(listCacheElements);
    list.add(matchingRegion);
    when(cacheConfig.getRegions()).thenReturn(list);
    List<CacheConfig.AsyncEventQueue> queueList = new ArrayList<>();
    when(cacheConfig.getAsyncEventQueues()).thenReturn(queueList);

    createRegionMappingCommand.updateConfigForGroup(null, cacheConfig, arguments);

    assertThat(queueList).isEmpty();
  }

  @Test
  public void createAsyncEventQueueNameWithRegionPathReturnsQueueNameThatIsTheSameAsRegionWithNoSlash() {
    String queueName1 = MappingCommandUtils.createAsyncEventQueueName("regionName");
    String queueName2 = MappingCommandUtils.createAsyncEventQueueName(SEPARATOR + "regionName");
    assertThat(queueName1).isEqualTo(queueName2);
  }

  @Test
  public void createAsyncEventQueueNameWithEmptyStringReturnsQueueName() {
    String queueName = MappingCommandUtils.createAsyncEventQueueName("");
    assertThat(queueName).isEqualTo("JDBC#");
  }

  @Test
  public void createAsyncEventQueueNameWithSubregionNameReturnsQueueNameWithNoSlashes() {
    String queueName = MappingCommandUtils.createAsyncEventQueueName(
        SEPARATOR + "parent" + SEPARATOR + "child" + SEPARATOR + "grandchild");
    assertThat(queueName).isEqualTo("JDBC#parent_child_grandchild");
  }

}
