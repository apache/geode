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

import java.util.Collection;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ConfigurationPersistenceService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliFunctionResult;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class ListMappingCommand extends GfshCommand {
  public static final String JDBC_MAPPINGS_SECTION = "jdbc-mappings";
  static final String LIST_MAPPING = "list jdbc-mappings";
  static final String LIST_MAPPING__HELP = EXPERIMENTAL + "Display jdbc mappings for all members.";

  static final String LIST_OF_MAPPINGS = "List of mappings";
  static final String NO_MAPPINGS_FOUND = "No mappings found";
  static final String LIST_MAPPINGS_MEMBER__HELP =
      "Member from which the jdbc mappings are retrieved.";

  @CliCommand(value = LIST_MAPPING, help = LIST_MAPPING__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel listMapping() {
    Collection<ConnectorService.RegionMapping> mappings = null;

    // check if CC is available and use it to describe the connection
    ConfigurationPersistenceService ccService = getConfigurationPersistenceService();
    if (ccService != null) {
      CacheConfig cacheConfig = ccService.getCacheConfig(CLUSTER_CONFIG);
      if (cacheConfig != null) {
        ConnectorService service =
            cacheConfig.findCustomCacheElement("connector-service", ConnectorService.class);
        if (service != null) {
          mappings = service.getRegionMapping();
        }
      }
    } else {
      // otherwise get it from any member
      Set<DistributedMember> members = findMembers(null, null);
      if (members.size() > 0) {
        DistributedMember targetMember = members.iterator().next();
        CliFunctionResult result =
            executeFunctionAndGetFunctionResult(new ListMappingFunction(), null, targetMember);
        if (result != null) {
          mappings = (Collection<ConnectorService.RegionMapping>) result.getResultObject();
        }
      }
    }

    if (mappings == null) {
      return ResultModel.createInfo(EXPERIMENTAL + "\n" + "No mappings found");
    }

    // output
    ResultModel resultModel = new ResultModel();
    boolean mappingsExist =
        fillTabularResultData(mappings, resultModel.addTable(JDBC_MAPPINGS_SECTION));
    if (mappingsExist) {
      resultModel.setHeader(EXPERIMENTAL);
      return resultModel;
    } else {
      return ResultModel.createInfo(EXPERIMENTAL + "\n" + NO_MAPPINGS_FOUND);
    }
  }

  /**
   * Returns true if any connections exist
   */
  private boolean fillTabularResultData(Collection<ConnectorService.RegionMapping> mappings,
      TabularResultModel tableModel) {
    for (ConnectorService.RegionMapping mapping : mappings) {
      tableModel.accumulate(LIST_OF_MAPPINGS, mapping.getRegionName());
    }
    return !mappings.isEmpty();
  }
}
