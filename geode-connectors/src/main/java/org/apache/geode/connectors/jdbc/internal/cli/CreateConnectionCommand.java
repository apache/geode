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

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class CreateConnectionCommand extends SingleGfshCommand {
  private static final Logger logger = LogService.getLogger();

  static final String CREATE_CONNECTION = "create jdbc-connection";
  static final String CREATE_CONNECTION__HELP =
      EXPERIMENTAL + "Create a connection for communicating with a database through jdbc.";
  static final String CREATE_CONNECTION__NAME = "name";
  static final String CREATE_CONNECTION__NAME__HELP = "Name of the connection to be created.";
  static final String CREATE_CONNECTION__URL = "url";
  static final String CREATE_CONNECTION__URL__HELP = "URL location for the database.";
  static final String CREATE_CONNECTION__USER = "user";
  static final String CREATE_CONNECTION__USER__HELP =
      "User name to use when connecting to database.";
  static final String CREATE_CONNECTION__PASSWORD = "password";
  static final String CREATE_CONNECTION__PASSWORD__HELP =
      "Password to use when connecting to database.";
  static final String CREATE_CONNECTION__PARAMS = "params";
  static final String CREATE_CONNECTION__PARAMS__HELP =
      "Additional parameters to use when connecting to the database formatted like \"key:value(,key:value)*\".";

  @CliCommand(value = CREATE_CONNECTION, help = CREATE_CONNECTION__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE,
      interceptor = "org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand$Interceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel createConnection(
      @CliOption(key = CREATE_CONNECTION__NAME, mandatory = true,
          help = CREATE_CONNECTION__NAME__HELP) String name,
      @CliOption(key = CREATE_CONNECTION__URL, mandatory = true,
          help = CREATE_CONNECTION__URL__HELP) String url,
      @CliOption(key = CREATE_CONNECTION__USER, help = CREATE_CONNECTION__USER__HELP) String user,
      @CliOption(key = CREATE_CONNECTION__PASSWORD,
          help = CREATE_CONNECTION__PASSWORD__HELP) String password,
      @CliOption(key = CREATE_CONNECTION__PARAMS,
          help = CREATE_CONNECTION__PARAMS__HELP) String[] params) {

    // input
    Set<DistributedMember> targetMembers = getMembers(null, null);
    ConnectorService.Connection connection =
        new ConnectorService.Connection(name, url, user, password, params);

    // action
    List<CliFunctionResult> results =
        executeAndGetFunctionResult(new CreateConnectionFunction(), connection, targetMembers);

    ResultModel result =
        ResultModel.createMemberStatusResult(results, EXPERIMENTAL, null, false, true);
    result.setConfigObject(connection);
    return result;
  }

  @Override
  public void updateClusterConfig(String group, CacheConfig config, Object element) {
    ConnectorService.Connection connection = (ConnectorService.Connection) element;
    ConnectorService service =
        config.findCustomCacheElement("connector-service", ConnectorService.class);
    if (service == null) {
      service = new ConnectorService();
      config.getCustomCacheElements().add(service);
    }
    service.getConnection().add(connection);
  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      String user = parseResult.getParamValueAsString(CREATE_CONNECTION__USER);
      String password = parseResult.getParamValueAsString(CREATE_CONNECTION__PASSWORD);

      if (StringUtils.isNotBlank(password) && StringUtils.isBlank(user)) {
        return ResultBuilder
            .createUserErrorResult("need to specify a user if a password is specified.");
      }
      return ResultBuilder.createInfoResult("");
    }
  }

  @CliAvailabilityIndicator({AlterConnectionCommand.ALTER_JDBC_CONNECTION,
      AlterMappingCommand.ALTER_MAPPING, CreateConnectionCommand.CREATE_CONNECTION,
      CreateMappingCommand.CREATE_MAPPING, DescribeConnectionCommand.DESCRIBE_CONNECTION,
      DescribeMappingCommand.DESCRIBE_MAPPING, DestroyConnectionCommand.DESTROY_CONNECTION,
      DestroyMappingCommand.DESTROY_MAPPING, ListConnectionCommand.LIST_JDBC_CONNECTION,
      ListMappingCommand.LIST_MAPPING})

  public boolean available() {
    return isOnlineCommandAvailable();
  }
}
