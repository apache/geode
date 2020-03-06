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
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.ExportConfigFunction;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.security.ResourcePermission;

public class ExportConfigCommand extends GfshCommand {
  private final ExportConfigFunction exportConfigFunction = new ExportConfigFunction();

  /**
   * Export the cache configuration in XML format.
   *
   * @param member Member for which to write the configuration
   * @param group Group or groups for which to write the configuration
   * @return Results of the attempt to write the configuration
   */
  @CliCommand(value = {CliStrings.EXPORT_CONFIG}, help = CliStrings.EXPORT_CONFIG__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.ExportConfigCommand$Interceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  @SuppressWarnings("deprecation")
  public ResultModel exportConfig(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.EXPORT_CONFIG__MEMBER__HELP) String[] member,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.EXPORT_CONFIG__GROUP__HELP) String[] group,
      @CliOption(key = {CliStrings.EXPORT_CONFIG__DIR},
          help = CliStrings.EXPORT_CONFIG__DIR__HELP) String dir) {

    ResultModel crm = new ResultModel();
    InfoResultModel infoData = crm.addInfo(ResultModel.INFO_SECTION);

    Set<DistributedMember> targetMembers = findMembers(group, member);
    if (targetMembers.isEmpty()) {
      ResultModel.createError(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      return crm;
    }

    ResultCollector<?, ?> rc =
        ManagementUtils.executeFunction(this.exportConfigFunction, null, targetMembers);
    List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

    for (CliFunctionResult result : results) {
      if (result.getThrowable() != null) {
        infoData.addLine(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__EXCEPTION,
            result.getMemberIdOrName(), result.getThrowable()));
      } else if (result.isSuccessful()) {
        String cacheFileName = result.getMemberIdOrName() + "-cache.xml";
        String propsFileName = result.getMemberIdOrName() + "-gf.properties";
        String[] fileContent = (String[]) result.getSerializables();
        crm.addFile(cacheFileName, fileContent[0]);
        crm.addFile(propsFileName, fileContent[1]);
      }
    }

    return crm;
  }

  /**
   * Interceptor used by gfsh to intercept execution of export config command at "shell".
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    private File saveDirFile;

    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String dir = parseResult.getParamValueAsString("dir");
      if (StringUtils.isBlank(dir)) {
        saveDirFile = new File(".").getAbsoluteFile();
        return new ResultModel();
      }

      saveDirFile = new File(dir.trim()).getAbsoluteFile();

      if (!saveDirFile.exists() && !saveDirFile.mkdirs()) {
        return ResultModel
            .createError(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__CANNOT_CREATE_DIR, dir));
      }

      if (!saveDirFile.isDirectory()) {
        return ResultModel
            .createError(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__NOT_A_DIRECTORY, dir));
      }

      try {
        if (!saveDirFile.canWrite()) {
          return ResultModel.createError(CliStrings.format(
              CliStrings.EXPORT_CONFIG__MSG__NOT_WRITEABLE, saveDirFile.getCanonicalPath()));
        }
      } catch (IOException ioex) {
        return ResultModel.createError(
            CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__NOT_WRITEABLE, saveDirFile.getName()));
      }
      return new ResultModel();
    }

    @Override
    public ResultModel postExecution(GfshParseResult parseResult, ResultModel commandResult,
        Path tempFile) throws Exception {
      commandResult.saveFileTo(saveDirFile);
      return commandResult;
    }
  }
}
