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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.http.annotation.Experimental;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

@Experimental
public class ListDriversCommand extends SingleGfshCommand {

  static final String LIST_DRIVERS = "list drivers";
  static final String LIST_DRIVERS__HELP = EXPERIMENTAL
      + "Lists all drivers currently registered by the cluster's Driver Manager.";
  static final String NO_MEMBERS_FOUND = "No members found";
  static final String MEMBER_NAME_NOT_FOUND = "No member found with name: ";
  static final String LIST_DRIVERS_SECTION = "LIST_DRIVERS";
  static final String MEMBER_NAME = "member-name";
  static final String MEMBER_NAME__HELP = "Name of specific member to list drivers for.";


  @CliCommand(value = LIST_DRIVERS, help = LIST_DRIVERS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)

  public ResultModel listDrivers(
      @CliOption(key = MEMBER_NAME, help = MEMBER_NAME__HELP) String memberName) {
    ResultModel resultModel = new ResultModel();
    return createTabularResultDataAndGetResult(resultModel, memberName);
  }

  private ResultModel createTabularResultDataAndGetResult(ResultModel resultModel,
      String memberName) {
    TabularResultModel tableModel = resultModel.addTable(LIST_DRIVERS_SECTION);
    List<String> drivers = new ArrayList<>();

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.size() > 0) {
      Object[] arguments = new Object[] {};
      List<CliFunctionResult> listDriversResults = executeAndGetFunctionResult(
          new ListDriversFunction(), arguments, targetMembers);

      if (memberName == null) {
        return ResultModel.createMemberStatusResult(listDriversResults, EXPERIMENTAL, null,
            false, true);
      }

      for (CliFunctionResult result : listDriversResults) {
        if (memberName.equals(result.getMemberIdOrName())) {
          if (result.isSuccessful()) {
            drivers = getListOfDrivers(result);
          } else {
            return ResultModel
                .createError("Error when listing drivers: " + result.getStatusMessage());
          }
        }
      }
      if (drivers.isEmpty()) {
        return ResultModel.createError(EXPERIMENTAL + "\n" + MEMBER_NAME_NOT_FOUND + memberName);
      }
    } else {
      return ResultModel.createInfo(EXPERIMENTAL + "\n" + NO_MEMBERS_FOUND);
    }

    for (String driver : drivers) {
      tableModel.accumulate("Driver names for: " + memberName, driver);
    }
    return resultModel;
  }

  @SuppressWarnings("unchecked")
  List<String> getListOfDrivers(CliFunctionResult listDriversResult) {
    return (List<String>) listDriversResult.getResultObject();
  }
}
