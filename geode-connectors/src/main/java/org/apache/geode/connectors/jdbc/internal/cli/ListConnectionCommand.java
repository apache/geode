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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.GfshCommand;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class ListConnectionCommand extends GfshCommand {
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

    // input
    Set<DistributedMember> targetMembers = getMembers(null, null);
    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    // action
    ResultCollector<ConnectionConfiguration, List<ConnectionConfiguration[]>> resultCollector =
        execute(new ListConnectionFunction(), targetMembers.iterator().next());

    // output
    TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
    boolean connectionsExist = fillTabularResultData(resultCollector, tabularResultData);
    return createResult(tabularResultData, connectionsExist);
  }

  ResultCollector<ConnectionConfiguration, List<ConnectionConfiguration[]>> execute(
      ListConnectionFunction function, DistributedMember targetMember) {
    return (ResultCollector<ConnectionConfiguration, List<ConnectionConfiguration[]>>) executeFunction(
        function, null, targetMember);
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
  private boolean fillTabularResultData(
      ResultCollector<ConnectionConfiguration, List<ConnectionConfiguration[]>> resultCollector,
      TabularResultData tabularResultData) {
    Set<ConnectionConfiguration> connectionConfigs = new HashSet<>();

    for (Object resultObject : resultCollector.getResult()) {
      if (resultObject instanceof ConnectionConfiguration[]) {
        connectionConfigs.addAll(Arrays.asList((ConnectionConfiguration[]) resultObject));
      } else if (resultObject instanceof Throwable) {
        throw new IllegalStateException((Throwable) resultObject);
      } else {
        throw new IllegalStateException(resultObject.getClass().getName());
      }
    }

    for (ConnectionConfiguration connectionConfig : connectionConfigs) {
      tabularResultData.accumulate(LIST_OF_CONNECTIONS, connectionConfig.getName());
    }

    return !connectionConfigs.isEmpty();
  }
}
