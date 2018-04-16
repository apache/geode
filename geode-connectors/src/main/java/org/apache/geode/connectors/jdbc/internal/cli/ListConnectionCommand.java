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

import java.util.List;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ClusterConfigurationService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.InternalGfshCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class ListConnectionCommand extends InternalGfshCommand {
  static final String LIST_JDBC_CONNECTION = "list jdbc-connections";
  static final String LIST_JDBC_CONNECTION__HELP =
      EXPERIMENTAL + "Display jdbc connections for all members.";

  static final String LIST_OF_CONNECTIONS = "List of connections";
  static final String NO_CONNECTIONS_FOUND = "No connections found";

  @CliCommand(value = LIST_JDBC_CONNECTION, help = LIST_JDBC_CONNECTION__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result listConnection() {

    ClusterConfigurationService ccService = getConfigurationService();
    if (ccService == null) {
      return ResultBuilder.createInfoResult("cluster configuration service is not running");
    }

    ConnectorService service =
        ccService.getCustomCacheElement("cluster", "connector-service", ConnectorService.class);
    if (service == null) {
      return ResultBuilder.createInfoResult(EXPERIMENTAL + "\n" + NO_CONNECTIONS_FOUND);
    }

    // output
    TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
    boolean connectionsExist = fillTabularResultData(service.getConnection(), tabularResultData);
    return createResult(tabularResultData, connectionsExist);
  }

  private Result createResult(TabularResultData tabularResultData, boolean connectionsExist) {
    if (connectionsExist) {
      tabularResultData.setHeader(EXPERIMENTAL);
      return ResultBuilder.buildResult(tabularResultData);
    } else {
      return ResultBuilder.createInfoResult(EXPERIMENTAL + "\n" + NO_CONNECTIONS_FOUND);
    }
  }

  /**
   * Returns true if any connections exist
   */
  private boolean fillTabularResultData(List<ConnectorService.Connection> connections,
      TabularResultData tabularResultData) {
    for (ConnectorService.Connection connectionConfig : connections) {
      tabularResultData.accumulate(LIST_OF_CONNECTIONS, connectionConfig.getName());
    }
    return !connections.isEmpty();
  }
}
