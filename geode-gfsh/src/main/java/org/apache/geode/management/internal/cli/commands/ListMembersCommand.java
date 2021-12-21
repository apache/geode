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

package org.apache.geode.management.internal.cli.commands;

import java.util.Set;
import java.util.TreeSet;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.Distribution;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ListMembersCommand extends GfshCommand {

  public static final String MEMBERS_SECTION = "members";

  @CliCommand(value = {CliStrings.LIST_MEMBER}, help = CliStrings.LIST_MEMBER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_SERVER)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel listMember(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      optionContext = ConverterHint.MEMBERGROUP,
      help = CliStrings.LIST_MEMBER__GROUP__HELP) String[] groups) {

    ResultModel crm = new ResultModel();
    Set<DistributedMember> memberSet = new TreeSet<>(
        findMembersIncludingLocators(groups, null));

    if (memberSet.isEmpty()) {
      crm.addInfo().addLine(CliStrings.LIST_MEMBER__MSG__NO_MEMBER_FOUND);
      return crm;
    }
    crm.addInfo().addLine("Member Count : " + memberSet.size());
    TabularResultModel resultData = crm.addTable(MEMBERS_SECTION);
    final String coordinatorMemberId = getCoordinatorId();
    for (DistributedMember member : memberSet) {
      resultData.accumulate("Name", member.getName());

      if (member.getUniqueId().equals(coordinatorMemberId)) {
        resultData.accumulate("Id", member.getId() + " [Coordinator]");
      } else {
        resultData.accumulate("Id", member.getId());
      }
    }

    return crm;
  }

  String getCoordinatorId() {
    InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
    if (ids == null || !ids.isConnected()) {
      return null;
    }

    Distribution mmgr = ids.getDistributionManager().getDistribution();
    if (mmgr == null) {
      return null;
    }

    return mmgr.getCoordinator().getUniqueId();
  }
}
