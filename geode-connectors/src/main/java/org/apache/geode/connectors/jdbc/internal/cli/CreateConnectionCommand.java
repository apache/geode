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
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.GfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CreateConnectionCommand implements GfshCommand {
  private static final Logger logger = LogService.getLogger();

  static final String CREATE_CONNECTION = "create jdbc-connection";
  static final String CREATE_CONNECTION__HELP = "Create JDBC connection for JDBC Connector.";
  static final String CREATE_CONNECTION__NAME = "name";
  static final String CREATE_CONNECTION__NAME__HELP = "Name of the JDBC connection to be created.";
  static final String CREATE_CONNECTION__URL = "url";
  static final String CREATE_CONNECTION__URL__HELP = "URL location for the database";
  static final String CREATE_CONNECTION__USER = "user";
  static final String CREATE_CONNECTION__USER__HELP =
      "Name of user to use when connecting to the database";
  static final String CREATE_CONNECTION__PASSWORD = "password";
  static final String CREATE_CONNECTION__PASSWORD__HELP =
      "Password of user to use when connecting to the database";
  static final String CREATE_CONNECTION__PARAMS = "params";
  static final String CREATE_CONNECTION__PARAMS__HELP =
      "Comma delimited list of additional parameters to use when connecting to the database";

  private static final String ERROR_PREFIX = "ERROR: ";

  @CliCommand(value = CREATE_CONNECTION, help = CREATE_CONNECTION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result createConnection(
      @CliOption(key = CREATE_CONNECTION__NAME, mandatory = true,
          help = CREATE_CONNECTION__NAME__HELP) String name,
      @CliOption(key = CREATE_CONNECTION__URL, mandatory = true,
          help = CREATE_CONNECTION__URL__HELP) String url,
      @CliOption(key = CREATE_CONNECTION__USER, help = CREATE_CONNECTION__USER__HELP) String user,
      @CliOption(key = CREATE_CONNECTION__PASSWORD,
          help = CREATE_CONNECTION__PASSWORD__HELP) String password,
      @CliOption(key = CREATE_CONNECTION__PARAMS,
          help = CREATE_CONNECTION__PARAMS__HELP) String[] params) {

    Set<DistributedMember> membersToCreateConnectionOn = getMembers(null, null);

    ConnectionConfigBuilder builder = new ConnectionConfigBuilder().withName(name).withUrl(url)
        .withUser(user).withPassword(password).withParameters(params);
    ConnectionConfiguration configuration = builder.build();

    ResultCollector<?, ?> resultCollector =
        executeFunction(new CreateConnectionFunction(), configuration, membersToCreateConnectionOn);

    Object resultCollectorResult = resultCollector.getResult();

    List<CliFunctionResult> regionCreateResults = (List<CliFunctionResult>) resultCollectorResult;

    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
    TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
    for (CliFunctionResult regionCreateResult : regionCreateResults) {
      boolean success = regionCreateResult.isSuccessful();
      tabularResultData.accumulate("Member", regionCreateResult.getMemberIdOrName());
      tabularResultData.accumulate("Status",
          (success ? "" : ERROR_PREFIX) + regionCreateResult.getMessage());

      if (success) {
        xmlEntity.set(regionCreateResult.getXmlEntity());
      } else {
        tabularResultData.setStatus(Result.Status.ERROR);
      }
    }

    Result result = ResultBuilder.buildResult(tabularResultData);

    if (xmlEntity.get() != null) {
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().addXmlEntity(xmlEntity.get(), null));
    }

    return result;
  }
}
