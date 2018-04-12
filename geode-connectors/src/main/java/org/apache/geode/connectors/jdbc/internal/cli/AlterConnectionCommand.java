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
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.InternalGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;


public class AlterConnectionCommand extends InternalGfshCommand {
  static final String ALTER_JDBC_CONNECTION = "alter jdbc-connection";
  static final String ALTER_JDBC_CONNECTION__HELP =
      "Alter properties for an existing jdbc connection.";

  static final String ALTER_CONNECTION__NAME = "name";
  static final String ALTER_CONNECTION__NAME__HELP = "Name of the connection to be altered.";
  static final String ALTER_CONNECTION__URL = "url";
  static final String ALTER_CONNECTION__URL__HELP = "New URL location for the database.";
  static final String ALTER_CONNECTION__USER = "user";
  static final String ALTER_CONNECTION__USER__HELP =
      "New user name to use when connecting to database.";
  static final String ALTER_CONNECTION__PASSWORD = "password";
  static final String ALTER_CONNECTION__PASSWORD__HELP =
      "New password to use when connecting to database.";
  static final String ALTER_CONNECTION__PARAMS = "params";
  static final String ALTER_CONNECTION__PARAMS__HELP =
      "New additional parameters to use when connecting to the database. This replaces all previously existing parameters.";

  private static final String ERROR_PREFIX = "ERROR: ";

  @CliCommand(value = ALTER_JDBC_CONNECTION, help = ALTER_JDBC_CONNECTION__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result alterConnection(
      @CliOption(key = ALTER_CONNECTION__NAME, mandatory = true,
          help = ALTER_CONNECTION__NAME__HELP) String name,
      @CliOption(key = ALTER_CONNECTION__URL, specifiedDefaultValue = "",
          help = ALTER_CONNECTION__URL__HELP) String url,
      @CliOption(key = ALTER_CONNECTION__USER, specifiedDefaultValue = "",
          help = ALTER_CONNECTION__USER__HELP) String user,
      @CliOption(key = ALTER_CONNECTION__PASSWORD, specifiedDefaultValue = "",
          help = ALTER_CONNECTION__PASSWORD__HELP) String password,
      @CliOption(key = ALTER_CONNECTION__PARAMS, specifiedDefaultValue = "",
          help = ALTER_CONNECTION__PARAMS__HELP) String[] params) {
    // input
    Set<DistributedMember> targetMembers = getMembers(null, null);
    ConnectionConfiguration configuration = getArguments(name, url, user, password, params);

    // action
    ResultCollector<CliFunctionResult, List<CliFunctionResult>> resultCollector =
        execute(new AlterConnectionFunction(), configuration, targetMembers);

    // output
    TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
    XmlEntity xmlEntity = fillTabularResultData(resultCollector, tabularResultData);
    Result result = ResultBuilder.buildResult(tabularResultData);
    updateClusterConfiguration(result, xmlEntity);
    return result;
  }

  ResultCollector<CliFunctionResult, List<CliFunctionResult>> execute(
      AlterConnectionFunction function, ConnectionConfiguration configuration,
      Set<DistributedMember> targetMembers) {
    return (ResultCollector<CliFunctionResult, List<CliFunctionResult>>) executeFunction(function,
        configuration, targetMembers);
  }

  private ConnectionConfiguration getArguments(String name, String url, String user,
      String password, String[] params) {
    ConnectionConfigBuilder builder = new ConnectionConfigBuilder().withName(name).withUrl(url)
        .withUser(user).withPassword(password).withParameters(params);
    return builder.build();
  }

  private XmlEntity fillTabularResultData(
      ResultCollector<CliFunctionResult, List<CliFunctionResult>> resultCollector,
      TabularResultData tabularResultData) {
    XmlEntity xmlEntity = null;

    for (CliFunctionResult oneResult : resultCollector.getResult()) {
      if (oneResult.isSuccessful()) {
        xmlEntity = addSuccessToResults(tabularResultData, oneResult);
      } else {
        addErrorToResults(tabularResultData, oneResult);
      }
    }

    return xmlEntity;
  }

  private XmlEntity addSuccessToResults(TabularResultData tabularResultData,
      CliFunctionResult oneResult) {
    tabularResultData.accumulate("Member", oneResult.getMemberIdOrName());
    tabularResultData.accumulate("Status", oneResult.getMessage());
    return oneResult.getXmlEntity();
  }

  private void addErrorToResults(TabularResultData tabularResultData, CliFunctionResult oneResult) {
    tabularResultData.accumulate("Member", oneResult.getMemberIdOrName());
    tabularResultData.accumulate("Status", ERROR_PREFIX + oneResult.getMessage());
    tabularResultData.setStatus(Result.Status.ERROR);
  }

  private void updateClusterConfiguration(final Result result, final XmlEntity xmlEntity) {
    if (xmlEntity != null) {
      persistClusterConfiguration(result,
          () -> ((InternalClusterConfigurationService) getConfigurationService())
              .addXmlEntity(xmlEntity, null));
    }
  }
}
