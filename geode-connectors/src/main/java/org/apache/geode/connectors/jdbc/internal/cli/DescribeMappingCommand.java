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

import static org.apache.geode.connectors.util.internal.MappingConstants.CATALOG_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.util.internal.MappingConstants.TABLE_NAME;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.connectors.util.internal.DescribeMappingResult;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class DescribeMappingCommand extends GfshCommand {
  static final String DESCRIBE_MAPPING = "describe jdbc-mapping";
  private static final String DESCRIBE_MAPPING__HELP =
      EXPERIMENTAL + "Describe the specified JDBC mapping";
  private static final String DESCRIBE_MAPPING__REGION_NAME = REGION_NAME;
  private static final String DESCRIBE_MAPPING__REGION_NAME__HELP =
      "Region name of the JDBC mapping to be described.";
  private static final String CREATE_MAPPING__GROUPS_NAME__HELP =
      "Server Group(s) of the JDBC mapping to be described.";

  public static final String RESULT_SECTION_NAME = "MappingDescription";

  @CliCommand(value = DESCRIBE_MAPPING, help = DESCRIBE_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel describeMapping(@CliOption(key = DESCRIBE_MAPPING__REGION_NAME,
      mandatory = true, help = DESCRIBE_MAPPING__REGION_NAME__HELP) String regionName,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CREATE_MAPPING__GROUPS_NAME__HELP) String[] groups) {
    if (regionName.startsWith("/")) {
      regionName = regionName.substring(1);
    }

    ArrayList<DescribeMappingResult> describeMappingResults = new ArrayList<>();

    try {
      ConfigurationPersistenceService configService = checkForClusterConfiguration();
      if (groups == null) {
        groups = new String[] {ConfigurationPersistenceService.CLUSTER_CONFIG};
      }
      for (String group : groups) {
        CacheConfig cacheConfig = getCacheConfig(configService, group);
        RegionConfig regionConfig = checkForRegion(regionName, cacheConfig, group);
        describeMappingResults
            .addAll(getMappingsFromRegionConfig(cacheConfig, regionConfig, group));
      }
    } catch (PreconditionException ex) {
      return ResultModel.createError(ex.getMessage());
    }

    if (describeMappingResults.isEmpty()) {
      throw new EntityNotFoundException(
          EXPERIMENTAL + "\n" + "JDBC mapping for region '" + regionName + "' not found");
    }

    ResultModel resultModel = buildResultModel(describeMappingResults);
    resultModel.setHeader(EXPERIMENTAL);
    return resultModel;
  }

  private CacheConfig getCacheConfig(ConfigurationPersistenceService configService, String group)
      throws PreconditionException {
    CacheConfig result = configService.getCacheConfig(group);
    if (result == null) {
      throw new PreconditionException(
          "Cache Configuration not found"
              + ((group.equals(ConfigurationPersistenceService.CLUSTER_CONFIG)) ? "."
                  : " for group " + group + "."));
    }
    return result;
  }

  private ArrayList<DescribeMappingResult> getMappingsFromRegionConfig(CacheConfig cacheConfig,
      RegionConfig regionConfig, String group) {
    CacheConfig.AsyncEventQueue asyncEventQueue = findAsyncEventQueue(cacheConfig, regionConfig);
    ArrayList<DescribeMappingResult> results = new ArrayList<>();
    for (CacheElement element : regionConfig.getCustomRegionElements()) {
      if (element instanceof RegionMapping) {
        results.add(buildDescribeMappingResult((RegionMapping) element, regionConfig.getName(),
            asyncEventQueue == null, group));
      }
    }
    return results;
  }

  private CacheConfig.AsyncEventQueue findAsyncEventQueue(CacheConfig cacheConfig,
      RegionConfig regionConfig) {
    for (CacheConfig.AsyncEventQueue queue : cacheConfig.getAsyncEventQueues()) {
      if (queue.getId()
          .equals(CreateMappingCommand.createAsyncEventQueueName(regionConfig.getName()))) {
        return queue;
      }
    }
    return null;
  }

  private DescribeMappingResult buildDescribeMappingResult(RegionMapping regionMapping,
      String regionName, boolean synchronous, String group) {
    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
    attributes.put(REGION_NAME, regionName);
    attributes.put(PDX_NAME, regionMapping.getPdxName());
    attributes.put(TABLE_NAME, regionMapping.getTableName());
    attributes.put(DATA_SOURCE_NAME, regionMapping.getDataSourceName());
    attributes.put(SYNCHRONOUS_NAME, Boolean.toString(synchronous));
    attributes.put(ID_NAME, regionMapping.getIds());
    if (regionMapping.getCatalog() != null) {
      attributes.put(CATALOG_NAME, regionMapping.getCatalog());
    }
    if (regionMapping.getSchema() != null) {
      attributes.put(SCHEMA_NAME, regionMapping.getSchema());
    }
    DescribeMappingResult result = new DescribeMappingResult(attributes);
    result.setGroupName(group);
    result.setFieldMappings(regionMapping.getFieldMappings());
    return result;
  }

  private ResultModel buildResultModel(ArrayList<DescribeMappingResult> describeMappingResult) {
    ResultModel resultModel = new ResultModel();
    for (int i = 0; i < describeMappingResult.size(); i++) {
      DataResultModel sectionModel = resultModel.addData(RESULT_SECTION_NAME + String.valueOf(i));
      DescribeMappingResult result = describeMappingResult.get(i);
      if (!result.getGroupName().equals(ConfigurationPersistenceService.CLUSTER_CONFIG)) {
        sectionModel.addData("Mapping for group", result.getGroupName());
      }
      result.getAttributeMap().forEach(sectionModel::addData);

      InfoResultModel sectionModel2 =
          resultModel.addInfo(RESULT_SECTION_NAME + "Field Mappings" + String.valueOf(i));
      List<FieldMapping> fieldMappings = result.getFieldMappings();

      Map<String, String> attributeMap = result.getAttributeMap();
      sectionModel2
          .setHeader("PDX field to JDBC column mappings for class " + attributeMap.get("pdx-name"));
      sectionModel2.addLine("\n");
      buildFieldInfo(sectionModel2, fieldMappings);
    }
    return resultModel;
  }

  private void buildFieldInfo(InfoResultModel sectionModel2, List<FieldMapping> fieldMappings) {
    int pdxLen = 8;
    int jdbcLen = 8;
    for (FieldMapping fieldMapping : fieldMappings) {
      if (fieldMapping.getPdxName().trim().length() > pdxLen) {
        pdxLen = fieldMapping.getPdxName().trim().length();
      }
      if (fieldMapping.getJdbcName().trim().length() > jdbcLen) {
        jdbcLen = fieldMapping.getJdbcName().trim().length();
      }
    }
    pdxLen = pdxLen + 4;
    jdbcLen = jdbcLen + 4;
    String headerRow = buildFieldHeaderRow(pdxLen, jdbcLen);
    String headerText = buildFieldHeaderText(pdxLen, jdbcLen);
    sectionModel2.addLine(headerRow);
    sectionModel2.addLine(headerText);
    sectionModel2.addLine(headerRow);
    String outPDXFormat = " %-" + pdxLen + "s %-12s";
    String outJDBCFormat = " %-" + jdbcLen + "s %-12s %b";
    for (FieldMapping fieldMapping : fieldMappings) {
      sectionModel2.addLine(
          String.format(outPDXFormat, fieldMapping.getPdxName(), fieldMapping.getPdxType()) +
              String.format(outJDBCFormat, fieldMapping.getJdbcName(), fieldMapping.getJdbcType(),
                  fieldMapping.isJdbcNullable()));
    }
  }

  private String buildFieldHeaderRow(int pdxLen, int jdbcLen) {
    StringBuilder sb = new StringBuilder();
    String rowItem = new String("|");
    for (int i = 0; i < pdxLen; i++) {
      rowItem = rowItem + "-";
    }
    sb.append(rowItem);
    sb.append("|------------");
    rowItem = new String("|");
    for (int i = 0; i < jdbcLen; i++) {
      rowItem = rowItem + "-";
    }
    sb.append(rowItem);
    sb.append("|------------");
    sb.append("|---------");
    return sb.toString();
  }

  private String buildFieldHeaderText(int pdxLen, int jdbcLen) {
    StringBuilder sb = new StringBuilder();
    int pdxPadded = pdxLen + 1;
    int jdbcPadded = jdbcLen + 1;
    sb.append(String.format("%-" + pdxPadded + "s", "|PDX Field"));
    sb.append(String.format("%-13s", "|PDX Type"));
    sb.append(String.format("%-" + jdbcPadded + "s", "|JDBC Column"));
    sb.append(String.format("%-13s", "|JDBC Type"));
    sb.append("|Nullable");
    return sb.toString();
  }

  public ConfigurationPersistenceService checkForClusterConfiguration()
      throws PreconditionException {
    ConfigurationPersistenceService result = getConfigurationPersistenceService();
    if (result == null) {
      throw new PreconditionException("Cluster Configuration must be enabled.");
    }
    return result;
  }

  private RegionConfig checkForRegion(String regionName, CacheConfig cacheConfig, String groupName)
      throws PreconditionException {
    RegionConfig regionConfig = findRegionConfig(cacheConfig, regionName);
    if (regionConfig == null) {
      String groupClause = "A region named " + regionName + " must already exist"
          + (!groupName.equals(ConfigurationPersistenceService.CLUSTER_CONFIG)
              ? " for group " + groupName + "." : ".");
      throw new PreconditionException(groupClause);
    }
    return regionConfig;
  }

  private RegionConfig findRegionConfig(CacheConfig cacheConfig, String regionName) {
    return cacheConfig.getRegions().stream()
        .filter(region -> region.getName().equals(regionName)).findFirst().orElse(null);
  }

  @CliAvailabilityIndicator({DESCRIBE_MAPPING})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }
}
