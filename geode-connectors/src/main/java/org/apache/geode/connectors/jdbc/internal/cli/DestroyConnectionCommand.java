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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

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

public class DestroyConnectionCommand implements GfshCommand {

  static final String DESTROY_CONNECTION = "destroy jdbc-connection";
  static final String DESTROY_CONNECTION__HELP = "Destroy/Remove the specified jdbc connection.";
  static final String DESTROY_CONNECTION__NAME = "name";
  static final String DESTROY_CONNECTION__NAME__HELP =
      "Name of the jdbc connection to be destroyed.";

  private static final String ERROR_PREFIX = "ERROR: ";

  @CliCommand(value = DESTROY_CONNECTION, help = DESTROY_CONNECTION__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result destroyConnection(@CliOption(key = DESTROY_CONNECTION__NAME, mandatory = true,
      help = DESTROY_CONNECTION__NAME__HELP) String name) {

    Set<DistributedMember> membersToDestroyConnectionOn = getMembers(null, null);

    ResultCollector<?, ?> resultCollector =
        executeFunction(new DestroyConnectionFunction(), name, membersToDestroyConnectionOn);

    Object resultCollectorResult = resultCollector.getResult();

    List<CliFunctionResult> connectionDestroyResults =
        (List<CliFunctionResult>) resultCollectorResult;

    AtomicReference<XmlEntity> xmlEntity = new AtomicReference<>();
    TabularResultData tabularResultData = ResultBuilder.createTabularResultData();
    for (CliFunctionResult connectionDestroyResult : connectionDestroyResults) {
      boolean success = connectionDestroyResult.isSuccessful();
      tabularResultData.accumulate("Member", connectionDestroyResult.getMemberIdOrName());
      tabularResultData.accumulate("Status",
          (success ? "" : ERROR_PREFIX) + connectionDestroyResult.getMessage());

      if (success) {
        xmlEntity.set(connectionDestroyResult.getXmlEntity());
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
