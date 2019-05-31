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

import org.apache.http.annotation.Experimental;
import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class ListDriversCommand extends SingleGfshCommand {

  static final String LIST_DRIVERS = "list drivers";
  static final String LIST_DRIVERS__HELP = EXPERIMENTAL
      + "Lists all drivers currently registered by the cluster's Driver Manager.";
  static final String LIST_OF_DRIVERS = "List of registered JDBC drivers";
  static final String NO_MEMBERS_FOUND = "No members found";
  static final String LIST_DRIVERS_SECTION = "LIST_DRIVERS";

  @CliCommand(value = LIST_DRIVERS, help = LIST_DRIVERS__HELP)
  @CliMetaData(relatedTopic = CliStrings.DEFAULT_TOPIC_GEODE)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)

  public ResultModel listDrivers() {
    ResultModel resultModel = new ResultModel();
    return createTabularResultDataAndGetResult(resultModel);
  }

  private ResultModel createTabularResultDataAndGetResult(ResultModel resultModel) {
    TabularResultModel tableModel = resultModel.addTable(LIST_DRIVERS_SECTION);
    List<String> drivers;

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.size() > 0) {
      Object[] arguments = new Object[] {};
      CliFunctionResult listDriversResult = executeAndGetFunctionResult(
          new ListDriversFunction(), arguments, targetMembers).get(0);

      if (listDriversResult.isSuccessful()) {
        drivers = getListOfDrivers(listDriversResult);
      } else {
        return ResultModel
            .createError("Error when listing drivers: " + listDriversResult.getStatusMessage());
      }

    } else {
      return ResultModel.createInfo(EXPERIMENTAL + "\n" + NO_MEMBERS_FOUND);
    }

    for (String driver : drivers) {
      tableModel.accumulate(LIST_OF_DRIVERS, driver);
    }
    return resultModel;
  }

  List<String> getListOfDrivers(CliFunctionResult listDriversResult) {
    return (List<String>) listDriversResult.getResultObject();
  }
}
