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

import static org.apache.geode.distributed.ConfigurationPersistenceService.CLUSTER_CONFIG;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class AlterMappingCommand extends SingleGfshCommand {
  static final String ALTER_MAPPING = "alter jdbc-mapping";
  static final String ALTER_MAPPING__HELP =
      EXPERIMENTAL + "Alter properties for an existing jdbc mapping.";

  static final String ALTER_MAPPING__REGION_NAME = "region";
  static final String ALTER_MAPPING__REGION_NAME__HELP =
      "Name of the region the mapping to be altered.";
  static final String ALTER_MAPPING__PDX_CLASS_NAME = "pdx-class-name";
  static final String ALTER_MAPPING__PDX_CLASS_NAME__HELP =
      "Name of new pdx class for which values with be written to the database.";
  static final String ALTER_MAPPING__TABLE_NAME = "table";
  static final String ALTER_MAPPING__TABLE_NAME__HELP =
      "Name of new database table for values to be written to.";
  static final String ALTER_MAPPING__CONNECTION_NAME = "connection";
  static final String ALTER_MAPPING__CONNECTION_NAME__HELP = "Name of new JDBC connection to use.";
  static final String ALTER_MAPPING__PRIMARY_KEY_IN_VALUE = "primary-key-in-value";
  static final String ALTER_MAPPING__PRIMARY_KEY_IN_VALUE__HELP =
      "If false, the entry value does not contain the data used for the database table's primary key, instead the entry key will be used for the primary key column value.";
  static final String ALTER_MAPPING__FIELD_MAPPING = "field-mapping";
  static final String ALTER_MAPPING__FIELD_MAPPING__HELP =
      "New key value pairs of entry value fields to database columns.";

  @CliCommand(value = ALTER_MAPPING, help = ALTER_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel alterMapping(
      @CliOption(key = ALTER_MAPPING__REGION_NAME, mandatory = true,
          help = ALTER_MAPPING__REGION_NAME__HELP) String regionName,
      @CliOption(key = ALTER_MAPPING__CONNECTION_NAME, specifiedDefaultValue = "",
          help = ALTER_MAPPING__CONNECTION_NAME__HELP) String connectionName,
      @CliOption(key = ALTER_MAPPING__TABLE_NAME, help = ALTER_MAPPING__TABLE_NAME__HELP,
          specifiedDefaultValue = "") String table,
      @CliOption(key = ALTER_MAPPING__PDX_CLASS_NAME, help = ALTER_MAPPING__PDX_CLASS_NAME__HELP,
          specifiedDefaultValue = "") String pdxClassName,
      @CliOption(key = ALTER_MAPPING__PRIMARY_KEY_IN_VALUE,
          help = ALTER_MAPPING__PRIMARY_KEY_IN_VALUE__HELP,
          specifiedDefaultValue = "true") Boolean keyInValue,
      @CliOption(key = ALTER_MAPPING__FIELD_MAPPING, help = ALTER_MAPPING__FIELD_MAPPING__HELP,
          specifiedDefaultValue = "") String[] fieldMappings) {
    // input
    Set<DistributedMember> targetMembers = getMembers(null, null);
    ConnectorService.RegionMapping newMapping = new ConnectorService.RegionMapping(regionName,
        pdxClassName, table, connectionName, keyInValue);
    newMapping.setFieldMapping(fieldMappings);

    ConfigurationPersistenceService ccService = getConfigurationPersistenceService();
    // if cc is running, you can only alter connection available in cc service.
    if (ccService != null) {
      // search for the connection that has this id to see if it exists
      CacheConfig cacheConfig = ccService.getCacheConfig(CLUSTER_CONFIG);
      ConnectorService service =
          cacheConfig.findCustomCacheElement("connector-service", ConnectorService.class);
      if (service == null) {
        throw new EntityNotFoundException("mapping with name '" + regionName + "' does not exist.");
      }
      ConnectorService.RegionMapping mapping =
          CacheElement.findElement(service.getRegionMapping(), regionName);
      if (mapping == null) {
        throw new EntityNotFoundException("mapping with name '" + regionName + "' does not exist.");
      }
    }

    // action
    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new AlterMappingFunction(), newMapping, targetMembers);
    ResultModel result =
        ResultModel.createMemberStatusResult(results, EXPERIMENTAL, null, false, true);

    // find the merged regionMapping from the function result
    CliFunctionResult successResult =
        results.stream().filter(CliFunctionResult::isSuccessful).findAny().get();
    ConnectorService.RegionMapping mergedMapping =
        (ConnectorService.RegionMapping) successResult.getResultObject();
    result.setConfigObject(mergedMapping);
    return result;
  }

  @Override
  public void updateClusterConfig(String group, CacheConfig config, Object element) {
    ConnectorService.RegionMapping mapping = (ConnectorService.RegionMapping) element;
    ConnectorService service =
        config.findCustomCacheElement("connector-service", ConnectorService.class);
    // service is not nul at this point
    CacheElement.removeElement(service.getRegionMapping(), mapping.getId());
    service.getRegionMapping().add(mapping);
  }
}
