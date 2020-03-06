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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.domain.StackTracesPerMember;
import org.apache.geode.management.internal.cli.functions.GetStackTracesFunction;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ExportStackTraceCommand extends GfshCommand {
  private final GetStackTracesFunction getStackTracesFunction = new GetStackTracesFunction();

  private final DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS")
          .withZone(ZoneId.systemDefault());

  /**
   * Current implementation supports writing it to a locator/server side file and returning the
   * location of the file
   */
  @CliCommand(value = CliStrings.EXPORT_STACKTRACE, help = CliStrings.EXPORT_STACKTRACE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel exportStackTrace(@CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
      optionContext = ConverterHint.ALL_MEMBER_IDNAME,
      help = CliStrings.EXPORT_STACKTRACE__HELP) String[] memberNameOrId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.ALL_MEMBER_IDNAME, help = CliStrings.GROUP) String[] group,

      @CliOption(key = CliStrings.EXPORT_STACKTRACE__FILE,
          help = CliStrings.EXPORT_STACKTRACE__FILE__HELP) String fileName,

      @CliOption(key = CliStrings.EXPORT_STACKTRACE__FAIL__IF__FILE__PRESENT,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.EXPORT_STACKTRACE__FAIL__IF__FILE__PRESENT__HELP) boolean failIfFilePresent)
      throws IOException {

    if (fileName == null) {
      StringBuilder filePrefix = new StringBuilder("stacktrace");
      fileName = filePrefix.append("_").append(System.currentTimeMillis()).toString();
    }
    final File outFile = new File(fileName);

    if (outFile.exists() && failIfFilePresent) {
      return ResultModel.createError(CliStrings
          .format(CliStrings.EXPORT_STACKTRACE__ERROR__FILE__PRESENT, outFile.getCanonicalPath()));
    }

    Map<String, byte[]> dumps = new HashMap<>();
    Set<DistributedMember> targetMembers = getMembers(group, memberNameOrId);

    ResultModel result = new ResultModel();
    InfoResultModel resultData = result.addInfo();

    ResultCollector<?, ?> rc = executeFunction(getStackTracesFunction, null, targetMembers);
    @SuppressWarnings("unchecked")
    ArrayList<Object> resultList = (ArrayList<Object>) rc.getResult();

    for (Object resultObj : resultList) {
      if (resultObj instanceof StackTracesPerMember) {
        StackTracesPerMember stackTracePerMember = (StackTracesPerMember) resultObj;
        dumps.put(getHeaderMessage(stackTracePerMember),
            stackTracePerMember.getStackTraces());
      }
    }

    InternalDistributedSystem ads = ((InternalCache) getCache()).getInternalDistributedSystem();
    String filePath = writeStacksToFile(dumps, fileName);
    resultData.addLine(CliStrings.format(CliStrings.EXPORT_STACKTRACE__SUCCESS, filePath));
    resultData.addLine(CliStrings.EXPORT_STACKTRACE__HOST + ads.getDistributedMember().getHost());

    return result;

  }

  String getHeaderMessage(StackTracesPerMember stackTracesPerMember) {
    String headerMessage = stackTracesPerMember.getMemberNameOrId();

    Instant timestamp = stackTracesPerMember.getTimestamp();
    if (timestamp != null) {
      headerMessage += " at " + formatter.format(timestamp);
    }
    return headerMessage;
  }

  /***
   * Writes the Stack traces member-wise to a text file
   *
   * @param dumps - Map containing key : member , value : zipped stack traces
   * @param fileName - Name of the file to which the stack-traces are written to
   * @return Canonical path of the file which contains the stack-traces
   */
  public String writeStacksToFile(Map<String, byte[]> dumps, String fileName) throws IOException {
    String filePath;
    PrintWriter ps;
    File outputFile;

    outputFile = new File(fileName);
    try (OutputStream os = new FileOutputStream(outputFile)) {
      ps = new PrintWriter(os);

      for (Map.Entry<String, byte[]> entry : dumps.entrySet()) {
        ps.append(CliStrings.STACK_TRACE_FOR_MEMBER).append(entry.getKey()).append(" ***")
            .append(System.lineSeparator());
        ps.flush();
        GZIPInputStream zipIn = new GZIPInputStream(new ByteArrayInputStream(entry.getValue()));
        BufferedInputStream bin = new BufferedInputStream(zipIn);
        byte[] buffer = new byte[10000];
        int count;
        while ((count = bin.read(buffer)) != -1) {
          os.write(buffer, 0, count);
        }
        ps.append('\n');
      }
      ps.flush();
      filePath = outputFile.getCanonicalPath();
    }

    return filePath;
  }
}
