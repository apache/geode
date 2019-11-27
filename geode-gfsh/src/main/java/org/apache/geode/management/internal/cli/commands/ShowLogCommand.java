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

import javax.management.ObjectName;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ShowLogCommand extends GfshCommand {
  @CliCommand(value = CliStrings.SHOW_LOG, help = CliStrings.SHOW_LOG_HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel showLog(
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.SHOW_LOG_MEMBER_HELP, mandatory = true) String memberNameOrId,
      @CliOption(key = CliStrings.SHOW_LOG_LINE_NUM, unspecifiedDefaultValue = "0",
          help = CliStrings.SHOW_LOG_LINE_NUM_HELP) int numberOfLines) {
    DistributedMember targetMember = getMember(memberNameOrId);
    MemberMXBean targetMemberMXBean = getMemberMxBean(targetMember);

    if (numberOfLines > ManagementConstants.MAX_SHOW_LOG_LINES) {
      numberOfLines = ManagementConstants.MAX_SHOW_LOG_LINES;
    }

    if (numberOfLines == 0 || numberOfLines < 0) {
      numberOfLines = ManagementConstants.DEFAULT_SHOW_LOG_LINES;
    }

    ResultModel result = new ResultModel();
    InfoResultModel resultInfo = result.addInfo();
    if (targetMemberMXBean != null) {
      String log = targetMemberMXBean.showLog(numberOfLines);
      if (log != null) {
        resultInfo.addLine(log);
      } else {
        resultInfo.addLine(CliStrings.SHOW_LOG_NO_LOG);
      }
    } else {
      return ResultModel.createError(
          String.format("%s: %s", CliStrings.SHOW_LOG_MSG_MEMBER_NOT_FOUND, memberNameOrId));
    }

    return result;
  }

  public MemberMXBean getMemberMxBean(DistributedMember targetMember) {
    SystemManagementService service = getManagementService();

    if (getCache().getDistributedSystem().getDistributedMember().equals(targetMember)) {
      return service.getMemberMXBean();
    } else {
      ObjectName objectName = service.getMemberMBeanName(targetMember);
      return service.getMBeanProxy(objectName, MemberMXBean.class);
    }
  }
}
