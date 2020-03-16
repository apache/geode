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


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.exporter.RemoteStreamExporter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheConfig.AsyncEventQueue;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.JdbcAsyncWriter;
import org.apache.geode.connectors.jdbc.JdbcLoader;
import org.apache.geode.connectors.jdbc.JdbcWriter;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.ManagementAgent;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.model.FileResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class CreateMappingCommand extends SingleGfshCommand {
  static final String CREATE_MAPPING = "create jdbc-mapping";
  private static final String CREATE_MAPPING__HELP =
      EXPERIMENTAL + "Create a JDBC mapping for a region for use with a JDBC database.";
  private static final String CREATE_MAPPING__REGION_NAME = MappingConstants.REGION_NAME;
  private static final String CREATE_MAPPING__REGION_NAME__HELP =
      "Name of the region the JDBC mapping is being created for.";
  private static final String CREATE_MAPPING__PDX_NAME = MappingConstants.PDX_NAME;
  private static final String CREATE_MAPPING__PDX_NAME__HELP =
      "Name of pdx class for which values will be written to the database.";
  private static final String CREATE_MAPPING__TABLE_NAME = MappingConstants.TABLE_NAME;
  private static final String CREATE_MAPPING__TABLE_NAME__HELP =
      "Name of database table for values to be written to.";
  private static final String CREATE_MAPPING__DATA_SOURCE_NAME = MappingConstants.DATA_SOURCE_NAME;
  private static final String CREATE_MAPPING__DATA_SOURCE_NAME__HELP =
      "Name of JDBC data source to use.";
  private static final String CREATE_MAPPING__SYNCHRONOUS_NAME = MappingConstants.SYNCHRONOUS_NAME;
  private static final String CREATE_MAPPING__SYNCHRONOUS_NAME__HELP =
      "By default, writes will be asynchronous. If true, writes will be synchronous.";
  private static final String CREATE_MAPPING__ID_NAME = MappingConstants.ID_NAME;
  private static final String CREATE_MAPPING__ID_NAME__HELP =
      "The table column names to use as the region key for this JDBC mapping. If more than one column name is given then they must be separated by commas.";
  private static final String CREATE_MAPPING__CATALOG_NAME = MappingConstants.CATALOG_NAME;
  private static final String CREATE_MAPPING__CATALOG_NAME__HELP =
      "The catalog that contains the database table. By default, the catalog is the empty string causing the table to be referenced without a catalog prefix.";
  private static final String CREATE_MAPPING__SCHEMA_NAME = MappingConstants.SCHEMA_NAME;
  private static final String CREATE_MAPPING__SCHEMA_NAME__HELP =
      "The schema that contains the database table. By default, the schema is the empty string causing the table to be referenced without a schema prefix.";
  private static final String CREATE_MAPPING__GROUPS_NAME__HELP =
      "The names of the server groups on which this mapping should be created.";
  private static final String CREATE_MAPPING__PDX_CLASS_FILE = MappingConstants.PDX_CLASS_FILE;
  private static final String CREATE_MAPPING__PDX_CLASS_FILE__HELP =
      "The file that contains the PDX class. It must be a file with the \".jar\" or \".class\" extension. By default, the PDX class must be on the server's classpath or gfsh deployed.";
  public static final String CREATE_MAPPING__IFNOTEXISTS__HELP =
      "By default, an attempt to create a duplicate jdbc mapping is reported as an error. If this option is specified without a value or is specified with a value of true, then gfsh displays a \"Skipping...\" acknowledgement, but does not throw an error.";
  static final String IF_NOT_EXISTS_SKIPPING_EXCEPTION_MESSAGE = "Skipping: ";

  @CliCommand(value = CREATE_MAPPING, help = CREATE_MAPPING__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand$Interceptor",
      relatedTopic = {CliStrings.DEFAULT_TOPIC_GEODE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel createMapping(
      @CliOption(key = CREATE_MAPPING__REGION_NAME, mandatory = true,
          help = CREATE_MAPPING__REGION_NAME__HELP) String regionName,
      @CliOption(key = CREATE_MAPPING__DATA_SOURCE_NAME, mandatory = true,
          help = CREATE_MAPPING__DATA_SOURCE_NAME__HELP) String dataSourceName,
      @CliOption(key = CREATE_MAPPING__TABLE_NAME,
          help = CREATE_MAPPING__TABLE_NAME__HELP) String table,
      @CliOption(key = CREATE_MAPPING__PDX_NAME, mandatory = true,
          help = CREATE_MAPPING__PDX_NAME__HELP) String pdxName,
      @CliOption(key = CREATE_MAPPING__PDX_CLASS_FILE,
          help = CREATE_MAPPING__PDX_CLASS_FILE__HELP) String pdxClassFile,
      @CliOption(key = CREATE_MAPPING__SYNCHRONOUS_NAME,
          help = CREATE_MAPPING__SYNCHRONOUS_NAME__HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean synchronous,
      @CliOption(key = CREATE_MAPPING__ID_NAME, help = CREATE_MAPPING__ID_NAME__HELP) String id,
      @CliOption(key = CREATE_MAPPING__CATALOG_NAME,
          help = CREATE_MAPPING__CATALOG_NAME__HELP) String catalog,
      @CliOption(key = CREATE_MAPPING__SCHEMA_NAME,
          help = CREATE_MAPPING__SCHEMA_NAME__HELP) String schema,
      @CliOption(key = CliStrings.IFNOTEXISTS,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
          help = CREATE_MAPPING__IFNOTEXISTS__HELP) boolean ifNotExists,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CREATE_MAPPING__GROUPS_NAME__HELP) String[] groups)
      throws IOException {
    if (regionName.startsWith("/")) {
      regionName = regionName.substring(1);
    }

    String tempPdxClassFilePath = null;
    String remoteInputStreamName = null;
    RemoteInputStream remoteInputStream = null;
    if (pdxClassFile != null) {
      List<String> pdxClassFilePaths = getFilePathFromShell();
      if (pdxClassFilePaths.size() != 1) {
        throw new IllegalStateException(
            "Expected only one element in the list returned by getFilePathFromShell, but it returned: "
                + pdxClassFilePaths);
      }
      tempPdxClassFilePath = pdxClassFilePaths.get(0);
    }

    Set<DistributedMember> targetMembers = findMembers(groups, null);
    RegionMapping mapping =
        new RegionMapping(regionName, pdxName, table, dataSourceName, id, catalog, schema);

    try {
      ConfigurationPersistenceService configurationPersistenceService =
          checkForClusterConfiguration();
      if (groups == null) {
        groups = new String[] {ConfigurationPersistenceService.CLUSTER_CONFIG};
      }
      for (String group : groups) {
        CacheConfig cacheConfig =
            MappingCommandUtils.getCacheConfig(configurationPersistenceService, group);
        RegionConfig regionConfig = checkForRegion(regionName, cacheConfig, group);
        checkForExistingMapping(regionName, regionConfig);
        checkForCacheLoader(regionName, regionConfig);
        checkForCacheWriter(regionName, synchronous, regionConfig);
        checkForAsyncQueue(regionName, synchronous, cacheConfig);
        checkForAEQIdForAccessor(regionName, synchronous, regionConfig);
      }
    } catch (PreconditionException ex) {
      if (ifNotExists) {
        return ResultModel
            .createInfo(IF_NOT_EXISTS_SKIPPING_EXCEPTION_MESSAGE + ex.getMessage());
      } else {
        return ResultModel.createError(ex.getMessage());
      }
    }

    if (pdxClassFile != null) {
      ManagementAgent agent =
          ((SystemManagementService) getManagementService()).getManagementAgent();
      RemoteStreamExporter exporter = agent.getRemoteStreamExporter();
      remoteInputStreamName = FilenameUtils.getName(tempPdxClassFilePath);
      remoteInputStream =
          exporter.export(createSimpleRemoteInputStream(tempPdxClassFilePath));
    }

    CliFunctionResult preconditionCheckResult;
    try {
      preconditionCheckResult =
          executeFunctionAndGetFunctionResult(new CreateMappingPreconditionCheckFunction(),
              new Object[] {mapping, remoteInputStreamName, remoteInputStream},
              targetMembers.iterator().next());
    } finally {
      if (remoteInputStream != null) {
        try {
          remoteInputStream.close(true);
        } catch (IOException ex) {
          // Ignored. the stream may have already been closed.
        }
      }
    }
    if (preconditionCheckResult.isSuccessful()) {
      Object[] preconditionOutput = (Object[]) preconditionCheckResult.getResultObject();
      String computedIds = (String) preconditionOutput[0];
      if (computedIds != null) {
        mapping.setIds(computedIds);
      }
      @SuppressWarnings("unchecked")
      ArrayList<FieldMapping> fieldMappings = (ArrayList<FieldMapping>) preconditionOutput[1];
      for (FieldMapping fieldMapping : fieldMappings) {
        mapping.addFieldMapping(fieldMapping);
      }
    } else {
      String message = preconditionCheckResult.getStatusMessage();
      return ResultModel.createError(message);
    }

    // action
    Object[] arguments = new Object[] {mapping, synchronous};
    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new CreateMappingFunction(), arguments, targetMembers);

    ResultModel result =
        ResultModel.createMemberStatusResult(results, EXPERIMENTAL, null, false, true);
    result.setConfigObject(arguments);
    return result;
  }

  SimpleRemoteInputStream createSimpleRemoteInputStream(String tempPdxClassFilePath)
      throws FileNotFoundException {
    return new SimpleRemoteInputStream(new FileInputStream(tempPdxClassFilePath));
  }

  private ConfigurationPersistenceService checkForClusterConfiguration()
      throws PreconditionException {
    ConfigurationPersistenceService result = getConfigurationPersistenceService();
    if (result == null) {
      throw new PreconditionException("Cluster Configuration must be enabled.");
    }
    return result;
  }

  private RegionConfig checkForRegion(String regionName, CacheConfig cacheConfig, String groupName)
      throws PreconditionException {
    return MappingCommandUtils.checkForRegion(regionName, cacheConfig, groupName);
  }

  private void checkForExistingMapping(String regionName, RegionConfig regionConfig)
      throws PreconditionException {
    if (regionConfig.getCustomRegionElements().stream()
        .anyMatch(element -> element instanceof RegionMapping)) {
      throw new PreconditionException("A JDBC mapping for " + regionName + " already exists.");
    }
  }

  private void checkForCacheLoader(String regionName, RegionConfig regionConfig)
      throws PreconditionException {
    RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
    if (regionAttributes != null) {
      DeclarableType loaderDeclarable = regionAttributes.getCacheLoader();
      if (loaderDeclarable != null) {
        throw new PreconditionException("The existing region " + regionName
            + " must not already have a cache-loader, but it has "
            + loaderDeclarable.getClassName());
      }
    }
  }

  private void checkForCacheWriter(String regionName, boolean synchronous,
      RegionConfig regionConfig) throws PreconditionException {
    if (synchronous) {
      RegionAttributesType writerAttributes = regionConfig.getRegionAttributes();
      if (writerAttributes != null) {
        DeclarableType writerDeclarable = writerAttributes.getCacheWriter();
        if (writerDeclarable != null) {
          throw new PreconditionException("The existing region " + regionName
              + " must not already have a cache-writer, but it has "
              + writerDeclarable.getClassName());
        }
      }
    }
  }

  private void checkForAsyncQueue(String regionName, boolean synchronous, CacheConfig cacheConfig)
      throws PreconditionException {
    if (!synchronous) {
      String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
      AsyncEventQueue asyncEventQueue = cacheConfig.getAsyncEventQueues().stream()
          .filter(queue -> queue.getId().equals(queueName)).findFirst().orElse(null);
      if (asyncEventQueue != null) {
        throw new PreconditionException(
            "An async-event-queue named " + queueName + " must not already exist.");
      }
    }
  }

  private void checkForAEQIdForAccessor(String regionName, boolean synchronous,
      RegionConfig regionConfig)
      throws PreconditionException {
    RegionAttributesType regionAttributesType = regionConfig.getRegionAttributes();
    if (!synchronous && regionAttributesType != null) {
      boolean isAccessor = MappingCommandUtils.isAccessor(regionAttributesType);
      if (isAccessor) {
        String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
        if (regionAttributesType.getAsyncEventQueueIds() != null && regionAttributesType
            .getAsyncEventQueueIds().contains(queueName)) {
          throw new PreconditionException(
              "An async-event-queue named " + queueName + " must not already exist.");
        }
      }
    }
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig cacheConfig, Object element) {
    if (element == null) {
      return false;
    }
    Object[] arguments = (Object[]) element;
    RegionMapping regionMapping = (RegionMapping) arguments[0];
    boolean synchronous = (Boolean) arguments[1];
    String regionName = regionMapping.getRegionName();
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    RegionConfig regionConfig = findRegionConfig(cacheConfig, regionName);
    if (regionConfig == null) {
      return false;
    }
    RegionAttributesType attributes = getRegionAttribute(regionConfig);
    if (MappingCommandUtils.isAccessor(attributes)) {
      alterProxyRegion(queueName, attributes, synchronous);
    } else {
      addMappingToRegion(regionMapping, regionConfig);
      if (!synchronous) {
        createAsyncQueue(cacheConfig, attributes, queueName);
      }
      alterNonProxyRegion(queueName, attributes, synchronous);
    }

    return true;
  }

  private RegionAttributesType getRegionAttribute(RegionConfig config) {
    if (config.getRegionAttributes() == null) {
      config.setRegionAttributes(new RegionAttributesType());
    }

    return config.getRegionAttributes();
  }

  @CliAvailabilityIndicator({CREATE_MAPPING})
  @SuppressWarnings("unused")
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }

  private void alterProxyRegion(String queueName, RegionAttributesType attributes,
      boolean synchronous) {
    if (!synchronous) {
      addAsyncEventQueueId(queueName, attributes);
    }
  }

  private void alterNonProxyRegion(String queueName, RegionAttributesType attributes,
      boolean synchronous) {
    setCacheLoader(attributes);
    if (synchronous) {
      setCacheWriter(attributes);
    } else {
      addAsyncEventQueueId(queueName, attributes);
    }
  }

  private void addMappingToRegion(RegionMapping newCacheElement, RegionConfig regionConfig) {
    regionConfig.getCustomRegionElements().add(newCacheElement);
  }

  private RegionConfig findRegionConfig(CacheConfig cacheConfig, String regionName) {
    return cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(regionName)).findFirst().orElse(null);
  }

  private void createAsyncQueue(CacheConfig cacheConfig, RegionAttributesType attributes,
      String queueName) {
    AsyncEventQueue asyncEventQueue = new AsyncEventQueue();
    asyncEventQueue.setId(queueName);
    boolean isPartitioned = attributes.getDataPolicy().equals(RegionAttributesDataPolicy.PARTITION)
        || attributes.getDataPolicy().equals(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
    asyncEventQueue.setParallel(isPartitioned);
    DeclarableType listener = new DeclarableType();
    listener.setClassName(JdbcAsyncWriter.class.getName());
    asyncEventQueue.setAsyncEventListener(listener);
    cacheConfig.getAsyncEventQueues().add(asyncEventQueue);
  }

  private void addAsyncEventQueueId(String queueName, RegionAttributesType attributes) {
    String asyncEventQueueList = attributes.getAsyncEventQueueIds();
    if (asyncEventQueueList == null) {
      asyncEventQueueList = "";
    }
    if (!asyncEventQueueList.contains(queueName)) {
      if (asyncEventQueueList.length() > 0) {
        asyncEventQueueList += ',';
      }
      asyncEventQueueList += queueName;
      attributes.setAsyncEventQueueIds(asyncEventQueueList);
    }

  }

  private void setCacheLoader(RegionAttributesType attributes) {
    DeclarableType loader = new DeclarableType();
    loader.setClassName(JdbcLoader.class.getName());
    attributes.setCacheLoader(loader);
  }

  private void setCacheWriter(RegionAttributesType attributes) {
    DeclarableType writer = new DeclarableType();
    writer.setClassName(JdbcWriter.class.getName());
    attributes.setCacheWriter(writer);
  }

  /**
   * Interceptor used by gfsh to intercept execution of create jdbc-mapping command at "shell".
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {

    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String pdxClassFileName = (String) parseResult.getParamValue(CREATE_MAPPING__PDX_CLASS_FILE);

      if (StringUtils.isBlank(pdxClassFileName)) {
        return ResultModel.createInfo("");
      }

      ResultModel result = new ResultModel();
      File pdxClassFile = new File(pdxClassFileName);
      if (!pdxClassFile.exists()) {
        return ResultModel.createError(pdxClassFile + " not found.");
      }
      if (!pdxClassFile.isFile()) {
        return ResultModel.createError(pdxClassFile + " is not a file.");
      }
      String fileExtension = FilenameUtils.getExtension(pdxClassFileName);
      if (!fileExtension.equalsIgnoreCase("jar") && !fileExtension.equalsIgnoreCase("class")) {
        return ResultModel.createError(pdxClassFile + " must end with \".jar\" or \".class\".");
      }
      result.addFile(pdxClassFile, FileResultModel.FILE_TYPE_FILE);

      return result;
    }
  }

  // For testing purpose
  List<String> getFilePathFromShell() {
    return CommandExecutionContext.getFilePathFromShell();
  }

}
