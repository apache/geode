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

import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__CONNECTION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__PDX_CLASS_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__REGION_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__TABLE_NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateMappingCommand.CREATE_MAPPING__VALUE_CONTAINS_PRIMARY_KEY;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ClusterConfigurationService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.InternalGfshCommand;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DescribeMappingCommand extends InternalGfshCommand {
  static final String DESCRIBE_MAPPING = "describe jdbc-mapping";
  static final String DESCRIBE_MAPPING__HELP = "Describe the specified jdbc mapping";
  static final String DESCRIBE_MAPPING__REGION_NAME = "region";
  static final String DESCRIBE_MAPPING__REGION_NAME__HELP =
      "Region name of the jdbc mapping to be described.";

  static final String RESULT_SECTION_NAME = "MappingDescription";
  static final String FIELD_TO_COLUMN_TABLE = "fieldToColumnTable";

  @CliCommand(value = DESCRIBE_MAPPING, help = DESCRIBE_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result describeMapping(@CliOption(key = DESCRIBE_MAPPING__REGION_NAME, mandatory = true,
      help = DESCRIBE_MAPPING__REGION_NAME__HELP) String regionName) {
    ConnectorService.RegionMapping mapping = null;

    // check if CC is available and use it to describe the connection
    ClusterConfigurationService ccService = getConfigurationService();
    if (ccService != null) {
      CacheConfig cacheConfig = ccService.getCacheConfig("cluster");
      ConnectorService service =
          cacheConfig.findCustomCacheElement("connector-service", ConnectorService.class);
      if (service != null) {
        mapping = CacheElement.findElement(service.getRegionMapping(), regionName);
      }
    } else {
      // otherwise get it from any member
      Set<DistributedMember> members = findMembers(null, null);
      if (members.size() > 0) {
        DistributedMember targetMember = members.iterator().next();
        List<?> result =
            (List<?>) executeFunction(new DescribeMappingFunction(), regionName, targetMember)
                .getResult();
        if (!result.isEmpty()) {
          mapping = (ConnectorService.RegionMapping) result.get(0);
        }
      }
    }

    if (mapping == null) {
      throw new EntityNotFoundException("mapping for region '" + regionName + "' not found");
    }

    CompositeResultData resultData = ResultBuilder.createCompositeResultData();
    fillResultData(mapping, resultData);
    return ResultBuilder.buildResult(resultData);
  }

  private void fillResultData(ConnectorService.RegionMapping mapping,
      CompositeResultData resultData) {
    CompositeResultData.SectionResultData sectionResult =
        resultData.addSection(RESULT_SECTION_NAME);
    sectionResult.addSeparator('-');
    sectionResult.addData(CREATE_MAPPING__REGION_NAME, mapping.getRegionName());
    sectionResult.addData(CREATE_MAPPING__CONNECTION_NAME, mapping.getConnectionConfigName());
    sectionResult.addData(CREATE_MAPPING__TABLE_NAME, mapping.getTableName());
    sectionResult.addData(CREATE_MAPPING__PDX_CLASS_NAME, mapping.getPdxClassName());
    sectionResult.addData(CREATE_MAPPING__VALUE_CONTAINS_PRIMARY_KEY,
        mapping.isPrimaryKeyInValue());

    TabularResultData tabularResultData = sectionResult.addTable(FIELD_TO_COLUMN_TABLE);
    tabularResultData.setHeader("Field to Column Mappings:");
    if (mapping.getFieldMapping() != null) {
      mapping.getFieldMapping().forEach((entry) -> {
        tabularResultData.accumulate("Field", entry.getFieldName());
        tabularResultData.accumulate("Column", entry.getColumnName());
      });
    }
  }
}
