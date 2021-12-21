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
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.CATALOG_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.DATA_SOURCE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.ID_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.PDX_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SCHEMA_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SPECIFIED_ID_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.SYNCHRONOUS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.MappingConstants.TABLE_NAME;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.i18n.CliStrings;
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
  private static final String DESCRIBE_MAPPING__GROUPS_NAME__HELP =
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
          help = DESCRIBE_MAPPING__GROUPS_NAME__HELP) String[] groups) {
    if (regionName.startsWith(SEPARATOR)) {
      regionName = regionName.substring(1);
    }

    ArrayList<DescribeMappingResult> describeMappingResults = new ArrayList<>();

    try {
      ConfigurationPersistenceService configService = checkForClusterConfiguration();
      if (groups == null) {
        groups = new String[] {ConfigurationPersistenceService.CLUSTER_CONFIG};
      }
      ArrayList<String> groupsArray = new ArrayList<String>();
      boolean isProxyRegion = false;
      for (String group : groups) {
        CacheConfig cacheConfig = getCacheConfig(configService, group);
        RegionConfig regionConfig = checkForRegion(regionName, cacheConfig, group);
        if (MappingCommandUtils.isAccessor(regionConfig.getRegionAttributes())) {
          isProxyRegion = true;
          continue;
        }
        groupsArray.add(group);
        describeMappingResults
            .addAll(getMappingsFromRegionConfig(cacheConfig, regionConfig, group));
      }
      if (groupsArray.size() == 0 && isProxyRegion) {
        return ResultModel.createInfo(MappingConstants.THERE_IS_NO_JDBC_MAPPING_ON_PROXY_REGION);
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
    return MappingCommandUtils.getCacheConfig(configService, group);
  }

  private DescribeMappingResult buildDescribeMappingResult(RegionMapping regionMapping,
      String regionName, boolean synchronous,
      String group) {
    LinkedHashMap<String, String> attributes = new LinkedHashMap<>();
    attributes.put(REGION_NAME, regionName);
    attributes.put(PDX_NAME, regionMapping.getPdxName());
    attributes.put(TABLE_NAME, regionMapping.getTableName());
    attributes.put(DATA_SOURCE_NAME, regionMapping.getDataSourceName());
    attributes.put(SYNCHRONOUS_NAME, Boolean.toString(synchronous));
    attributes.put(ID_NAME, regionMapping.getIds());
    attributes.put(SPECIFIED_ID_NAME, Boolean.toString(regionMapping.getSpecifiedIds()));
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
      DataResultModel sectionModel = resultModel.addData(RESULT_SECTION_NAME + i);
      DescribeMappingResult result = describeMappingResult.get(i);
      if (!result.getGroupName().equals(ConfigurationPersistenceService.CLUSTER_CONFIG)) {
        sectionModel.addData("Mapping for group", result.getGroupName());
      }
      result.getAttributeMap().forEach(sectionModel::addData);

      TabularResultModel fieldMappingTable =
          resultModel.addTable(RESULT_SECTION_NAME + "Field Mappings" + i);
      List<FieldMapping> fieldMappings = result.getFieldMappings();

      fieldMappingTable.setHeader("PDX Field to JDBC Column Mappings");
      buildFieldMappingTable(fieldMappingTable, fieldMappings);
    }
    return resultModel;
  }

  private void buildFieldMappingTable(TabularResultModel fieldMappingTable,
      List<FieldMapping> fieldMappings) {
    fieldMappingTable.setColumnHeader("PDX Field", "PDX Type", "JDBC Column", "JDBC Type",
        "Nullable");
    for (FieldMapping fieldMapping : fieldMappings) {
      fieldMappingTable.addRow(fieldMapping.getPdxName(), fieldMapping.getPdxType(),
          fieldMapping.getJdbcName(), fieldMapping.getJdbcType(),
          Boolean.toString(fieldMapping.isJdbcNullable()));
    }
  }

  private ArrayList<DescribeMappingResult> getMappingsFromRegionConfig(CacheConfig cacheConfig,
      RegionConfig regionConfig,
      String group) {
    ArrayList<DescribeMappingResult> results = new ArrayList<>();
    for (RegionMapping mapping : MappingCommandUtils.getMappingsFromRegionConfig(cacheConfig,
        regionConfig,
        group)) {
      results.add(buildDescribeMappingResult(mapping, regionConfig.getName(),
          MappingCommandUtils.isMappingSynchronous(cacheConfig, regionConfig), group));
    }
    return results;
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

  @CliAvailabilityIndicator({DESCRIBE_MAPPING})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }
}
