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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
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

@Experimental
public class DestroyConnectionCommand extends GfshCommand {
  static final String DESTROY_CONNECTION = "destroy jdbc-connection";
  static final String DESTROY_CONNECTION__HELP =
      EXPERIMENTAL + "Destroy/Remove the specified jdbc connection.";
  static final String DESTROY_CONNECTION__NAME = "name";
  static final String DESTROY_CONNECTION__NAME__HELP =
      "Name of the jdbc connection to be destroyed.";

  private static final String ERROR_PREFIX = "ERROR: ";

  @CliCommand(value = DESTROY_CONNECTION, help = DESTROY_CONNECTION__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result destroyConnection(@CliOption(key = DESTROY_CONNECTION__NAME, mandatory = true,
      help = DESTROY_CONNECTION__NAME__HELP) String name) {

    // input
    Set<DistributedMember> targetMembers = getMembers(null, null);

    // action
    ResultCollector<CliFunctionResult, List<CliFunctionResult>> resultCollector =
        execute(new DestroyConnectionFunction(), name, targetMembers);

    // output
    TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
    XmlEntity xmlEntity = fillTabularResultData(resultCollector, tabularResultData);
    tabularResultData.setHeader(EXPERIMENTAL);
    Result result = ResultBuilder.buildResult(tabularResultData);
    updateClusterConfiguration(result, xmlEntity);
    return result;
  }

  ResultCollector<CliFunctionResult, List<CliFunctionResult>> execute(
      DestroyConnectionFunction function, String connectionName,
      Set<DistributedMember> targetMembers) {
    return (ResultCollector<CliFunctionResult, List<CliFunctionResult>>) executeFunction(function,
        connectionName, targetMembers);
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
          () -> getConfigurationService().addXmlEntity(xmlEntity, null));
    }
  }
}
