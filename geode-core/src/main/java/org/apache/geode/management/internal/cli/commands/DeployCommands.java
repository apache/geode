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
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DeployFunction;
import org.apache.geode.management.internal.cli.functions.ListDeployedFunction;
import org.apache.geode.management.internal.cli.functions.UndeployFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.FileResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;



/**
 * Commands for deploying, un-deploying and listing files deployed using the command line shell.
 *
 * @see GfshCommand
 * @since GemFire 7.0
 */
public class DeployCommands implements GfshCommand {

  private final DeployFunction deployFunction = new DeployFunction();
  private final UndeployFunction undeployFunction = new UndeployFunction();
  private final ListDeployedFunction listDeployedFunction = new ListDeployedFunction();

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
      interceptor = "org.apache.geode.management.internal.cli.commands.DeployCommands$Interceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE, target = Target.JAR)
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
            tabularData.setStatus(Status.ERROR);
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
   * Undeploy one or more JAR files from members of a group or all members.
   * 
   * @param groups Group(s) to undeploy the JAR from or null for all members
   * @param jars JAR(s) to undeploy (separated by comma)
   * @return The result of the attempt to undeploy
   */
  @CliCommand(value = {CliStrings.UNDEPLOY}, help = CliStrings.UNDEPLOY__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE, target = Target.JAR)
  public Result undeploy(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.UNDEPLOY__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) String[] groups,
      @CliOption(key = {CliStrings.JAR, CliStrings.JARS},
          help = CliStrings.UNDEPLOY__JAR__HELP) String[] jars) {

    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers = CliUtil.findMembers(groups, null);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(this.undeployFunction, new Object[] {jars}, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {

        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Un-Deployed JAR", "");
          tabularData.accumulate("Un-Deployed JAR Location",
              "ERROR: " + result.getThrowable().getClass().getName() + ": "
                  + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else {
          String[] strings = (String[]) result.getSerializables();
          for (int i = 0; i < strings.length; i += 2) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("Un-Deployed JAR", strings[i]);
            tabularData.accumulate("Un-Deployed From JAR Location", strings[i + 1]);
            accumulatedData = true;
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult(CliStrings.UNDEPLOY__NO_JARS_FOUND_MESSAGE);
      }

      Result result = ResultBuilder.buildResult(tabularData);
      if (tabularData.getStatus().equals(Status.OK)) {
        persistClusterConfiguration(result,
            () -> getSharedConfiguration().removeJars(jars, groups));
      }
      return result;
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult("Exception while attempting to un-deploy: "
          + th.getClass().getName() + ": " + th.getMessage());
    }
  }

  /**
   * List all currently deployed JARs for members of a group or for all members.
   * 
   * @param group Group for which to list JARs or null for all members
   * @return List of deployed JAR files
   */
  @CliCommand(value = {CliStrings.LIST_DEPLOYED}, help = CliStrings.LIST_DEPLOYED__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listDeployed(@CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
      help = CliStrings.LIST_DEPLOYED__GROUP__HELP) String[] group) {

    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, null);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      ResultCollector<?, ?> rc =
          CliUtil.executeFunction(this.listDeployedFunction, null, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("JAR", "");
          tabularData.accumulate("JAR Location",
              "ERROR: " + result.getThrowable().getClass().getName() + ": "
                  + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else {
          String[] strings = (String[]) result.getSerializables();
          for (int i = 0; i < strings.length; i += 2) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("JAR", strings[i]);
            tabularData.accumulate("JAR Location", strings[i + 1]);
            accumulatedData = true;
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult(CliStrings.LIST_DEPLOYED__NO_JARS_FOUND_MESSAGE);
      }
      return ResultBuilder.buildResult(tabularData);

    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult("Exception while attempting to list deployed: "
          + th.getClass().getName() + ": " + th.getMessage());
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
        return ResultBuilder.createGemFireErrorResult("'" + filesToUpload + "' not found.");
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
          return ResultBuilder
              .createShellClientAbortOperationResult("Aborted deploy of " + filesToUpload + ".");
        }
      }

      return fileResult;
    }
  }
}
