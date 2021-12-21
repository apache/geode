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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtils;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.functions.NetstatFunction;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.security.ResourcePermission;

public class NetstatCommand extends GfshCommand {
  private static final String NETSTAT_FILE_REQUIRED_EXTENSION = ".txt";

  @CliCommand(value = CliStrings.NETSTAT, help = CliStrings.NETSTAT__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.NetstatCommand$Interceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  // TODO : Verify the auto-completion for multiple values.
  public ResultModel netstat(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.NETSTAT__MEMBER__HELP) String[] members,
      @CliOption(key = CliStrings.GROUP, optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.NETSTAT__GROUP__HELP) String group,
      @CliOption(key = CliStrings.NETSTAT__FILE,
          help = CliStrings.NETSTAT__FILE__HELP) String saveAs,
      @CliOption(key = CliStrings.NETSTAT__WITHLSOF, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.NETSTAT__WITHLSOF__HELP) boolean withlsof) {
    ResultModel result = new ResultModel();

    Map<String, DistributedMember> hostMemberMap = new HashMap<>();
    Map<String, List<String>> hostMemberListMap = new HashMap<>();

    try {
      if (members != null && members.length > 0 && group != null) {
        throw new IllegalArgumentException(
            CliStrings.NETSTAT__MSG__ONLY_ONE_OF_MEMBER_OR_GROUP_SHOULD_BE_SPECIFIED);
      }
      StringBuilder resultInfo = new StringBuilder();

      // Execute for remote members whose id or name matches
      InternalDistributedSystem system = InternalDistributedSystem.getConnectedInstance();

      if (members != null) {
        Set<String> notFoundMembers = new HashSet<>();
        for (String memberIdOrName : members) {
          Set<DistributedMember> membersToExecuteOn = ManagementUtils.getAllMembers(system);
          boolean memberFound = false;
          for (DistributedMember distributedMember : membersToExecuteOn) {
            String memberName = distributedMember.getName();
            String memberId = distributedMember.getId();
            if (memberName.equals(memberIdOrName) || memberId.equals(memberIdOrName)) {
              buildMaps(hostMemberMap, hostMemberListMap, memberIdOrName, distributedMember);

              memberFound = true;
              break;
            }
          }
          if (!memberFound) {
            notFoundMembers.add(memberIdOrName);
          }
        }
        // if there are not found members, it's probably unknown member or member has departed
        if (!notFoundMembers.isEmpty()) {
          throw new IllegalArgumentException(
              CliStrings.format(CliStrings.NETSTAT__MSG__COULD_NOT_FIND_MEMBERS_0,
                  new Object[] {collectionToString(notFoundMembers, -1)}));
        }
      } else {
        Set<DistributedMember> membersToExecuteOn;
        if (group != null) {
          membersToExecuteOn = system.getGroupMembers(group);
        } else {
          // consider all members
          membersToExecuteOn = ManagementUtils.getAllMembers(system);
        }

        for (DistributedMember distributedMember : membersToExecuteOn) {
          String memberName = distributedMember.getName();
          String memberId = distributedMember.getId();
          String memberIdOrName =
              memberName != null && !memberName.isEmpty() ? memberName : memberId;

          buildMaps(hostMemberMap, hostMemberListMap, memberIdOrName, distributedMember);
        }
      }

      String lineSeparatorToUse;
      lineSeparatorToUse = CommandExecutionContext.getShellLineSeparator();
      if (lineSeparatorToUse == null) {
        lineSeparatorToUse = GfshParser.LINE_SEPARATOR;
      }
      NetstatFunction.NetstatFunctionArgument nfa =
          new NetstatFunction.NetstatFunctionArgument(lineSeparatorToUse, withlsof);

      if (!hostMemberMap.isEmpty()) {
        Set<DistributedMember> membersToExecuteOn = new HashSet<>(hostMemberMap.values());
        ResultCollector<?, ?> netstatResult =
            ManagementUtils.executeFunction(NetstatFunction.INSTANCE, nfa, membersToExecuteOn);
        List<?> resultList = (List<?>) netstatResult.getResult();
        for (Object aResultList : resultList) {
          NetstatFunction.NetstatFunctionResult netstatFunctionResult =
              (NetstatFunction.NetstatFunctionResult) aResultList;
          CliUtils.DeflaterInflaterData deflaterInflaterData =
              netstatFunctionResult.getCompressedBytes();
          try {
            String remoteHost = netstatFunctionResult.getHost();
            List<String> membersList = hostMemberListMap.get(remoteHost);
            resultInfo.append(MessageFormat.format(netstatFunctionResult.getHeaderInfo(),
                collectionToString(membersList, 120)));
            CliUtils.DeflaterInflaterData uncompressedBytes = CliUtils.uncompressBytes(
                deflaterInflaterData.getData(), deflaterInflaterData.getDataLength());
            resultInfo.append(new String(uncompressedBytes.getData()));
          } catch (DataFormatException e) {
            resultInfo.append("Error in some data. Reason : ").append(e.getMessage());
          }
        }
      }
      if (saveAs != null && !saveAs.isEmpty()) {
        String saveToFile = saveAs;
        if (!saveAs.endsWith(NETSTAT_FILE_REQUIRED_EXTENSION)) {
          saveToFile = saveAs + NETSTAT_FILE_REQUIRED_EXTENSION;
        }

        result.addFile(FilenameUtils.getName(saveToFile), resultInfo.toString()); // Note:
                                                                                  // substitution
                                                                                  // for {0} will
      } else {
        result.addInfo().addLine(resultInfo.toString());
      }
    } finally {
      hostMemberMap.clear();
      hostMemberListMap.clear();
    }
    return result;
  }

  String collectionToString(Collection<?> col, int newlineAfter) {
    if (col != null) {
      StringBuilder builder = new StringBuilder();
      int lastNewlineAt = 0;

      for (Iterator<?> it = col.iterator(); it.hasNext();) {
        Object object = it.next();
        builder.append(object);
        if (it.hasNext()) {
          builder.append(", ");
        }
        if (newlineAfter > 0 && (builder.length() - lastNewlineAt) / newlineAfter >= 1) {
          builder.append(GfshParser.LINE_SEPARATOR);
        }
      }
      return builder.toString();
    } else {
      return "" + null;
    }
  }

  private void buildMaps(Map<String, DistributedMember> hostMemberMap,
      Map<String, List<String>> hostMemberListMap, String memberIdOrName,
      DistributedMember distributedMember) {
    String host = distributedMember.getHost();

    // Maintain one member for a host - function execution purpose - once only for a host
    if (!hostMemberMap.containsKey(host)) {
      hostMemberMap.put(host, distributedMember);
    }

    // Maintain all members for a host - display purpose
    List<String> list;
    if (!hostMemberListMap.containsKey(host)) {
      list = new ArrayList<>();
      hostMemberListMap.put(host, list);
    } else {
      list = hostMemberListMap.get(host);
    }
    list.add(memberIdOrName);
  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String saveAs = parseResult.getParamValueAsString(CliStrings.NETSTAT__FILE);

      if (saveAs != null && StringUtils.isEmpty(FilenameUtils.getName(saveAs))) {
        return ResultModel.createError("Invalid file name: " + saveAs);
      }

      return ResultModel.createInfo("");
    }

    @Override
    public ResultModel postExecution(GfshParseResult parseResult, ResultModel result, Path tempFile)
        throws IOException {
      // save the content to the file specified by the user
      String saveAs = parseResult.getParamValueAsString(CliStrings.NETSTAT__FILE);
      if (saveAs == null) {
        return result;
      }

      File file = new File(saveAs).getAbsoluteFile();
      result.saveFileTo(file.getParentFile());
      return result;
    }
  }
}
