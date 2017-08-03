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

import static org.apache.commons.io.FileUtils.ONE_MB;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DeployFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.FileResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission;

public class DeployCommand implements GfshCommand {
  private final DeployFunction deployFunction = new DeployFunction();

  /**
   * Deploy one or more JAR files to members of a group or all members.
   *
   * @param groups Group(s) to deploy the JAR to or null for all members
   * @param jars JAR file to deploy
   * @param dir Directory of JAR files to deploy
   * @return The result of the attempt to deploy
   */
  @CliCommand(value = {CliStrings.DEPLOY}, help = CliStrings.DEPLOY__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.DeployCommand$Interceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.JAR)
  public Result deploy(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS}, help = CliStrings.DEPLOY__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) String[] groups,
      @CliOption(key = {CliStrings.JAR, CliStrings.JARS},
          help = CliStrings.DEPLOY__JAR__HELP) String[] jars,
      @CliOption(key = {CliStrings.DEPLOY__DIR}, help = CliStrings.DEPLOY__DIR__HELP) String dir) {
    try {

      // since deploy function can potentially do a lot of damage to security, this action should
      // require these following privileges
      SecurityService securityService = getSecurityService();

      TabularResultData tabularData = ResultBuilder.createTabularResultData();

      byte[][] shellBytesData = CommandExecutionContext.getBytesFromShell();
      String[] jarNames = CliUtil.bytesToNames(shellBytesData);
      byte[][] jarBytes = CliUtil.bytesToData(shellBytesData);

      Set<DistributedMember> targetMembers;

      targetMembers = CliUtil.findMembers(groups, null);

      if (targetMembers.size() > 0) {
        // this deploys the jars to all the matching servers
        ResultCollector<?, ?> resultCollector = CliUtil.executeFunction(this.deployFunction,
            new Object[] {jarNames, jarBytes}, targetMembers);

        List<CliFunctionResult> results =
            CliFunctionResult.cleanResults((List<?>) resultCollector.getResult());

        for (CliFunctionResult result : results) {
          if (result.getThrowable() != null) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("Deployed JAR", "");
            tabularData.accumulate("Deployed JAR Location",
                "ERROR: " + result.getThrowable().getClass().getName() + ": "
                    + result.getThrowable().getMessage());
            tabularData.setStatus(Result.Status.ERROR);
          } else {
            String[] strings = (String[]) result.getSerializables();
            for (int i = 0; i < strings.length; i += 2) {
              tabularData.accumulate("Member", result.getMemberIdOrName());
              tabularData.accumulate("Deployed JAR", strings[i]);
              tabularData.accumulate("Deployed JAR Location", strings[i + 1]);
            }
          }
        }
      }

      Result result = ResultBuilder.buildResult(tabularData);
      persistClusterConfiguration(result,
          () -> getSharedConfiguration().addJarsToThisLocator(jarNames, jarBytes, groups));
      return result;
    } catch (NotAuthorizedException e) {
      // for NotAuthorizedException, will catch this later in the code
      throw e;
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(String
          .format("Exception while attempting to deploy: (%1$s)", toString(t, isDebugging())));
    }
  }

  /**
   * Interceptor used by gfsh to intercept execution of deploy command at "shell".
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    private final DecimalFormat numFormatter = new DecimalFormat("###,##0.00");

    @Override
    public Result preExecution(GfshParseResult parseResult) {
      // 2nd argument is the jar
      String[] jars = (String[]) parseResult.getArguments()[1];
      // 3rd argument is the dir
      String dir = (String) parseResult.getArguments()[2];

      if (ArrayUtils.isEmpty(jars) && StringUtils.isBlank(dir)) {
        return ResultBuilder.createUserErrorResult(
            "Parameter \"jar\" or \"dir\" is required. Use \"help <command name>\" for assistance.");
      }

      if (ArrayUtils.isNotEmpty(jars) && StringUtils.isNotBlank(dir)) {
        return ResultBuilder
            .createUserErrorResult("Parameters \"jar\" and \"dir\" can not both be specified.");
      }

      FileResult fileResult;
      String[] filesToUpload = jars;
      if (filesToUpload == null) {
        filesToUpload = new String[] {dir};
      }
      try {

        fileResult = new FileResult(filesToUpload);
      } catch (FileNotFoundException fnfex) {
        return ResultBuilder
            .createGemFireErrorResult("'" + Arrays.toString(filesToUpload) + "' not found.");
      } catch (IOException ioex) {
        return ResultBuilder.createGemFireErrorResult("I/O error when reading jar/dir: "
            + ioex.getClass().getName() + ": " + ioex.getMessage());
      }

      // Only do this additional check if a dir was provided
      if (dir != null) {
        String message =
            "\nDeploying files: " + fileResult.getFormattedFileList() + "\nTotal file size is: "
                + this.numFormatter.format((double) fileResult.computeFileSizeTotal() / ONE_MB)
                + "MB\n\nContinue? ";

        if (readYesNo(message, Response.YES) == Response.NO) {
          return ResultBuilder.createShellClientAbortOperationResult(
              "Aborted deploy of " + Arrays.toString(filesToUpload) + ".");
        }
      }
      return fileResult;
    }
  }
}
