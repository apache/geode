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

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.ExportConfigFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ExportConfigCommand implements GfshCommand {
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
  public Result exportConfig(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.EXPORT_CONFIG__MEMBER__HELP) String[] member,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.EXPORT_CONFIG__GROUP__HELP) String[] group,
      @CliOption(key = {CliStrings.EXPORT_CONFIG__DIR},
          help = CliStrings.EXPORT_CONFIG__DIR__HELP) String dir) {
    InfoResultData infoData = ResultBuilder.createInfoResultData();

    Set<DistributedMember> targetMembers = findMembers(group, member, getCache());
    if (targetMembers.isEmpty()) {
      return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }

    ResultCollector<?, ?> rc = executeFunction(this.exportConfigFunction, null, targetMembers);
    List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

    for (CliFunctionResult result : results) {
      if (result.getThrowable() != null) {
        infoData.addLine(CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__EXCEPTION,
            result.getMemberIdOrName(), result.getThrowable()));
      } else if (result.isSuccessful()) {
        String cacheFileName = result.getMemberIdOrName() + "-cache.xml";
        String propsFileName = result.getMemberIdOrName() + "-gf.properties";
        String[] fileContent = (String[]) result.getSerializables();
        infoData.addAsFile(cacheFileName, fileContent[0], "Downloading Cache XML file: {0}", false);
        infoData.addAsFile(propsFileName, fileContent[1], "Downloading properties file: {0}",
            false);
      }
    }
    return ResultBuilder.buildResult(infoData);

  }

  /**
   * Interceptor used by gfsh to intercept execution of export config command at "shell".
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    private String saveDirString;

    @Override
    public Result preExecution(GfshParseResult parseResult) {
      String dir = parseResult.getParamValueAsString("dir");
      if (StringUtils.isBlank(dir)) {
        saveDirString = new File(".").getAbsolutePath();
        return ResultBuilder.createInfoResult("OK");
      }

      File saveDirFile = new File(dir.trim());

      if (!saveDirFile.exists() && !saveDirFile.mkdirs()) {
        return ResultBuilder.createGemFireErrorResult(
            CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__CANNOT_CREATE_DIR, dir));
      }

      if (!saveDirFile.isDirectory()) {
        return ResultBuilder.createGemFireErrorResult(
            CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__NOT_A_DIRECTORY, dir));
      }

      try {
        if (!saveDirFile.canWrite()) {
          return ResultBuilder.createGemFireErrorResult(CliStrings.format(
              CliStrings.EXPORT_CONFIG__MSG__NOT_WRITEABLE, saveDirFile.getCanonicalPath()));
        }
      } catch (IOException ioex) {
        return ResultBuilder.createGemFireErrorResult(
            CliStrings.format(CliStrings.EXPORT_CONFIG__MSG__NOT_WRITEABLE, saveDirFile.getName()));
      }

      saveDirString = saveDirFile.getAbsolutePath();
      return ResultBuilder.createInfoResult("OK");
    }

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult, Path tempFile) {
      if (commandResult.hasIncomingFiles()) {
        try {
          commandResult.saveIncomingFiles(saveDirString);
        } catch (IOException ioex) {
          Gfsh.getCurrentInstance().logSevere("Unable to export config", ioex);
        }
      }
      return commandResult;
    }
  }
}
