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

import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__NAME;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__PARAMS;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__PASSWORD;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__URL;
import static org.apache.geode.connectors.jdbc.internal.cli.CreateConnectionCommand.CREATE_CONNECTION__USER;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.distributed.ClusterConfigurationService;
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

public class DescribeConnectionCommand extends InternalGfshCommand {
  static final String DESCRIBE_CONNECTION = "describe jdbc-connection";
  static final String DESCRIBE_CONNECTION__HELP =
      "Describe the specified jdbc connection found in cluster configuration.";
  static final String DESCRIBE_CONNECTION__NAME = "name";
  static final String DESCRIBE_CONNECTION__NAME__HELP =
      "Name of the jdbc connection to be described.";

  static final String OBSCURED_PASSWORD = "********";
  static final String RESULT_SECTION_NAME = "ConnectionDescription";

  @CliCommand(value = DESCRIBE_CONNECTION, help = DESCRIBE_CONNECTION__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result describeConnection(@CliOption(key = DESCRIBE_CONNECTION__NAME, mandatory = true,
      help = DESCRIBE_CONNECTION__NAME__HELP) String name) {

    ClusterConfigurationService ccService = getConfigurationService();
    if (ccService == null) {
      return ResultBuilder.createInfoResult("cluster configuration service is not running");
    }
    // search for the connection that has this id to see if it exists
    ConnectorService service =
        ccService.getCustomCacheElement("cluster", "connector-service", ConnectorService.class);
    if (service == null) {
      throw new EntityNotFoundException("connection named '" + name + "' not found");
    }
    ConnectorService.Connection connection =
        ccService.findIdentifiable(service.getConnection(), name);
    if (connection == null) {
      throw new EntityNotFoundException("connection named '" + name + "' not found");
    }

    CompositeResultData resultData = ResultBuilder.createCompositeResultData();
    fillResultData(connection, resultData);
    return ResultBuilder.buildResult(resultData);
  }

  private void fillResultData(ConnectorService.Connection config, CompositeResultData resultData) {
    CompositeResultData.SectionResultData sectionResult =
        resultData.addSection(RESULT_SECTION_NAME);
    sectionResult.addSeparator('-');
    sectionResult.addData(CREATE_CONNECTION__NAME, config.getName());
    sectionResult.addData(CREATE_CONNECTION__URL, config.getUrl());
    if (config.getUser() != null) {
      sectionResult.addData(CREATE_CONNECTION__USER, config.getUser());
    }
    if (config.getPassword() != null) {
      sectionResult.addData(CREATE_CONNECTION__PASSWORD, OBSCURED_PASSWORD);
    }
    TabularResultData tabularResultData = sectionResult.addTable(CREATE_CONNECTION__PARAMS);
    tabularResultData.setHeader("Additional connection parameters:");
    config.getParameterMap().entrySet().forEach((entry) -> {
      tabularResultData.accumulate("Param Name", entry.getKey());
      tabularResultData.accumulate("Value", entry.getValue());
    });
  }
}
