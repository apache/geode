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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.GetMemberInformationFunction;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.runtime.CacheServerInfo;
import org.apache.geode.management.runtime.MemberInformation;
import org.apache.geode.security.ResourcePermission;

public class DescribeMemberCommand extends GfshCommand {
  @Immutable
  private static final GetMemberInformationFunction getMemberInformation =
      new GetMemberInformationFunction();

  @CliCommand(value = {CliStrings.DESCRIBE_MEMBER}, help = CliStrings.DESCRIBE_MEMBER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_SERVER)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel describeMember(@CliOption(key = CliStrings.DESCRIBE_MEMBER__IDENTIFIER,
      optionContext = ConverterHint.ALL_MEMBER_IDNAME, help = CliStrings.DESCRIBE_MEMBER__HELP,
      mandatory = true) String memberNameOrId) {
    DistributedMember memberToBeDescribed = getMember(memberNameOrId);

    ResultCollector<?, ?> rc = executeFunction(getMemberInformation, null, memberToBeDescribed);

    ArrayList<?> output = (ArrayList<?>) rc.getResult();
    Object obj = output.get(0);

    if (!(obj instanceof MemberInformation)) {
      return ResultModel.createInfo(String.format(
          CliStrings.DESCRIBE_MEMBER__MSG__INFO_FOR__0__COULD_NOT_BE_RETRIEVED, memberNameOrId));
    }
    ResultModel result = new ResultModel();
    DataResultModel memberInfo = result.addData("memberInfo");

    MemberInformation memberInformation = (MemberInformation) obj;
    memberInfo.addData("Name", memberInformation.getMemberName());
    memberInfo.addData("Id", memberInformation.getId());
    memberInfo.addData("Host", memberInformation.getHost());
    memberInfo.addData("Regions", StringUtils.join(memberInformation.getHostedRegions(), '\n'));
    memberInfo.addData("PID", memberInformation.getProcessId());
    memberInfo.addData("Groups", memberInformation.getGroups());
    memberInfo.addData("Used Heap", memberInformation.getHeapUsage() + "M");
    memberInfo.addData("Max Heap", memberInformation.getMaxHeapSize() + "M");

    String offHeapMemorySize = memberInformation.getOffHeapMemorySize();
    if (offHeapMemorySize != null && !offHeapMemorySize.isEmpty()) {
      memberInfo.addData("Off Heap Size", offHeapMemorySize);
    }

    memberInfo.addData("Working Dir", memberInformation.getWorkingDirPath());
    memberInfo.addData("Log file", memberInformation.getLogFilePath());

    memberInfo.addData("Locators", memberInformation.getLocators());

    if (memberInformation.isServer()) {
      List<CacheServerInfo> csList = memberInformation.getCacheServerInfo();
      if (csList != null) {
        int serverCount = 0;
        for (CacheServerInfo cacheServerInfo : csList) {
          DataResultModel serverInfo = result.addData("serverInfo" + serverCount++);
          if (csList.size() == 1) {
            serverInfo.setHeader("Cache Server Information");
          } else {
            serverInfo.setHeader("Cache Server " + serverCount + " Information");
          }
          serverInfo.addData("Server Bind", cacheServerInfo.getBindAddress());
          serverInfo.addData("Server Port", cacheServerInfo.getPort());
          serverInfo.addData("Running", cacheServerInfo.isRunning());
        }

        DataResultModel connectionInfo = result.addData("connectionInfo");
        connectionInfo.addData("Client Connections", memberInformation.getClientCount());
      }
    }
    return result;
  }
}
