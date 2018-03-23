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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.CacheServerInfo;
import org.apache.geode.management.internal.cli.domain.MemberInformation;
import org.apache.geode.management.internal.cli.functions.GetMemberInformationFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DescribeMemberCommand extends InternalGfshCommand {
  private static final GetMemberInformationFunction getMemberInformation =
      new GetMemberInformationFunction();

  @CliCommand(value = {CliStrings.DESCRIBE_MEMBER}, help = CliStrings.DESCRIBE_MEMBER__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_SERVER)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result describeMember(@CliOption(key = CliStrings.DESCRIBE_MEMBER__IDENTIFIER,
      optionContext = ConverterHint.ALL_MEMBER_IDNAME, help = CliStrings.DESCRIBE_MEMBER__HELP,
      mandatory = true) String memberNameOrId) {
    Result result = null;

    try {
      DistributedMember memberToBeDescribed = getMember(memberNameOrId);

      ResultCollector<?, ?> rc = executeFunction(getMemberInformation, null, memberToBeDescribed);

      ArrayList<?> output = (ArrayList<?>) rc.getResult();
      Object obj = output.get(0);

      if (obj != null && (obj instanceof MemberInformation)) {
        CompositeResultData crd = ResultBuilder.createCompositeResultData();

        MemberInformation memberInformation = (MemberInformation) obj;
        memberInformation.setName(memberToBeDescribed.getName());
        memberInformation.setId(memberToBeDescribed.getId());
        memberInformation.setHost(memberToBeDescribed.getHost());
        memberInformation.setProcessId("" + memberToBeDescribed.getProcessId());

        CompositeResultData.SectionResultData section = crd.addSection();
        section.addData("Name", memberInformation.getName());
        section.addData("Id", memberInformation.getId());
        section.addData("Host", memberInformation.getHost());
        section.addData("Regions", StringUtils.join(memberInformation.getHostedRegions(), '\n'));
        section.addData("PID", memberInformation.getProcessId());
        section.addData("Groups", memberInformation.getGroups());
        section.addData("Used Heap", memberInformation.getHeapUsage() + "M");
        section.addData("Max Heap", memberInformation.getMaxHeapSize() + "M");

        String offHeapMemorySize = memberInformation.getOffHeapMemorySize();
        if (offHeapMemorySize != null && !offHeapMemorySize.isEmpty()) {
          section.addData("Off Heap Size", offHeapMemorySize);
        }

        section.addData("Working Dir", memberInformation.getWorkingDirPath());
        section.addData("Log file", memberInformation.getLogFilePath());

        section.addData("Locators", memberInformation.getLocators());

        if (memberInformation.isServer()) {
          CompositeResultData.SectionResultData clientServiceSection = crd.addSection();
          List<CacheServerInfo> csList = memberInformation.getCacheServeInfo();

          if (csList != null) {
            Iterator<CacheServerInfo> iters = csList.iterator();
            clientServiceSection.setHeader("Cache Server Information");

            while (iters.hasNext()) {
              CacheServerInfo cacheServerInfo = iters.next();
              clientServiceSection.addData("Server Bind", cacheServerInfo.getBindAddress());
              clientServiceSection.addData("Server Port", cacheServerInfo.getPort());
              clientServiceSection.addData("Running", cacheServerInfo.isRunning());
            }

            clientServiceSection.addData("Client Connections", memberInformation.getClientCount());
          }
        }
        result = ResultBuilder.buildResult(crd);

      } else {
        result = ResultBuilder.createInfoResult(
            CliStrings.format(CliStrings.DESCRIBE_MEMBER__MSG__INFO_FOR__0__COULD_NOT_BE_RETRIEVED,
                new Object[] {memberNameOrId}));
      }
    } catch (CacheClosedException ignored) {
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
    return result;
  }
}
