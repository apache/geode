/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.commands;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.AbstractCliAroundInterceptor;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.DeployFunction;
import com.gemstone.gemfire.management.internal.cli.functions.ListDeployedFunction;
import com.gemstone.gemfire.management.internal.cli.functions.UndeployFunction;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.remote.CommandExecutionContext;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.FileResult;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.configuration.SharedConfigurationWriter;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;


/**
 * Commands for deploying, un-deploying and listing files deployed using the command line shell.
 * <p/>
 * @see com.gemstone.gemfire.management.internal.cli.commands.AbstractCommandsSupport
 * @since GemFire 7.0
 */
public final class DeployCommands extends AbstractCommandsSupport implements CommandMarker {

  private final DeployFunction deployFunction = new DeployFunction();
  private final UndeployFunction undeployFunction = new UndeployFunction();
  private final ListDeployedFunction listDeployedFunction = new ListDeployedFunction();

  /**
   * Deploy one or more JAR files to members of a group or all members.
   * 
   * @param groups
   *          Group(s) to deploy the JAR to or null for all members
   * @param jar
   *          JAR file to deploy
   * @param dir
*          Directory of JAR files to deploy
   * @return The result of the attempt to deploy
   */
  @CliCommand(value = { CliStrings.DEPLOY }, help = CliStrings.DEPLOY__HELP)
  @CliMetaData(interceptor = "com.gemstone.gemfire.management.internal.cli.commands.DeployCommands$Interceptor", relatedTopic={CliStrings.TOPIC_GEODE_CONFIG}, writesToSharedConfiguration=true)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public final Result deploy(
    @CliOption(key = { CliStrings.DEPLOY__GROUP }, help = CliStrings.DEPLOY__GROUP__HELP, optionContext=ConverterHint.MEMBERGROUP)
    @CliMetaData (valueSeparator = ",")
    String[] groups,
    @CliOption(key = { CliStrings.DEPLOY__JAR }, help = CliStrings.DEPLOY__JAR__HELP)
    String jar,
    @CliOption(key = { CliStrings.DEPLOY__DIR }, help = CliStrings.DEPLOY__DIR__HELP)
    String dir)
  {
    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();

      byte[][] shellBytesData = CommandExecutionContext.getBytesFromShell();
      String[] jarNames = CliUtil.bytesToNames(shellBytesData);
      byte[][] jarBytes = CliUtil.bytesToData(shellBytesData);

      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findAllMatchingMembers(groups, null);
      } catch (CommandResultException e) {
        return e.getResult();
}

      ResultCollector<?, ?> resultCollector = CliUtil.executeFunction(this.deployFunction,
        new Object[] { jarNames, jarBytes }, targetMembers);

      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) resultCollector.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Deployed JAR", "");
          tabularData.accumulate("Deployed JAR Location", "ERROR: " + result.getThrowable().getClass().getName() + ": "
            + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else {
          String[] strings = (String[]) result.getSerializables();
          for (int i = 0; i < strings.length; i += 2) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("Deployed JAR", strings[i]);
            tabularData.accumulate("Deployed JAR Location", strings[i + 1]);
            accumulatedData = true;
          }
        }
      }

     
      
      if (!accumulatedData) {
        // This really should never happen since if a JAR file is already deployed a result is returned indicating that.
        return ResultBuilder.createInfoResult("Unable to deploy JAR file(s)");
      }
      
      Result result = ResultBuilder.buildResult(tabularData);
      if (tabularData.getStatus().equals(Status.OK)){
        result.setCommandPersisted((new SharedConfigurationWriter()).addJars(jarNames, jarBytes, groups));
      }
      return result;
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(String.format("Exception while attempting to deploy: (%1$s)",
        toString(t, isDebugging())));
    }
  }

  /**
   * Undeploy one or more JAR files from members of a group or all members.
   * 
   * @param groups
   *          Group(s) to undeploy the JAR from or null for all members
   * @param jars
   *          JAR(s) to undeploy (separated by comma)
   * @return The result of the attempt to undeploy
   */
  @CliCommand(value = { CliStrings.UNDEPLOY }, help = CliStrings.UNDEPLOY__HELP)
  @CliMetaData(relatedTopic={CliStrings.TOPIC_GEODE_CONFIG}, writesToSharedConfiguration=true)
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public final Result undeploy(
      @CliOption(key = { CliStrings.UNDEPLOY__GROUP },
                 help = CliStrings.UNDEPLOY__GROUP__HELP, 
                 optionContext=ConverterHint.MEMBERGROUP)
      @CliMetaData (valueSeparator = ",") String[] groups,
      @CliOption(key = { CliStrings.UNDEPLOY__JAR },
                 help = CliStrings.UNDEPLOY__JAR__HELP, unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE)
      @CliMetaData (valueSeparator = ",") String jars) {

    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findAllMatchingMembers(groups, null);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }

      ResultCollector<?, ?> rc = CliUtil.executeFunction(this.undeployFunction, new Object[] { jars }, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Un-Deployed JAR", "");
          tabularData.accumulate("Un-Deployed JAR Location", "ERROR: " + result.getThrowable().getClass().getName() + ": "
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
        result.setCommandPersisted((new SharedConfigurationWriter()).deleteJars(jars == null ? null : jars.split(","), groups));
      }
      return result;
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult("Exception while attempting to un-deploy: " + th.getClass().getName() + ": "
          + th.getMessage());
    }
  }

  /**
   * List all currently deployed JARs for members of a group or for all members.
   * 
   * @param group
   *          Group for which to list JARs or null for all members
   * @return List of deployed JAR files
   */
  @CliCommand(value = { CliStrings.LIST_DEPLOYED }, help = CliStrings.LIST_DEPLOYED__HELP)
  @CliMetaData(relatedTopic={CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation= Operation.READ)
  public final Result listDeployed(
      @CliOption(key = { CliStrings.LIST_DEPLOYED__GROUP },
                 help = CliStrings.LIST_DEPLOYED__GROUP__HELP)
      @CliMetaData (valueSeparator = ",") String group) {

    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findAllMatchingMembers(group, null);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }

      ResultCollector<?, ?> rc = CliUtil.executeFunction(this.listDeployedFunction, null, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("JAR", "");
          tabularData.accumulate("JAR Location", "ERROR: " + result.getThrowable().getClass().getName() + ": "
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
      return ResultBuilder.createGemFireErrorResult("Exception while attempting to list deployed: " + th.getClass().getName() + ": "
          + th.getMessage());
    }
  }

  @CliAvailabilityIndicator({ CliStrings.DEPLOY, CliStrings.UNDEPLOY, CliStrings.LIST_DEPLOYED })
  public final boolean isConnected() {
    if (!CliUtil.isGfshVM()) {
      return true;
    }

    return (getGfsh() != null && getGfsh().isConnectedAndReady());
  }
  
  /**
   * Interceptor used by gfsh to intercept execution of deploy command at "shell".
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    private final DecimalFormat numFormatter = new DecimalFormat("###,##0.00");

    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Map<String, String> paramValueMap = parseResult.getParamValueStrings();

      String jar = paramValueMap.get("jar");
      jar = (jar == null) ? null : jar.trim();
      
      String dir = paramValueMap.get("dir");
      dir = (dir == null) ? null : dir.trim();
      
      String group = paramValueMap.get("group");
      group = (group == null) ? null : group.trim();

      String jarOrDir = (jar != null ? jar : dir);

      if (jar == null && dir == null) {
        return ResultBuilder.createUserErrorResult("Parameter \"jar\" or \"dir\" is required. Use \"help <command name>\" for assistance.");
      }

      FileResult fileResult;
      try {
        fileResult = new FileResult(new String[] { jar != null ? jar : dir });
      } catch (FileNotFoundException fnfex) {
        return ResultBuilder.createGemFireErrorResult("'" + jarOrDir + "' not found.");
      } catch (IOException ioex) {
        return ResultBuilder.createGemFireErrorResult("I/O error when reading jar/dir: " + ioex.getClass().getName() + ": "
            + ioex.getMessage());
      }

      // Only do this additional check if a dir was provided
      if (dir != null) {
        String message = "\nDeploying files: " + fileResult.getFormattedFileList() + "\nTotal file size is: "
            + this.numFormatter.format(((double) fileResult.computeFileSizeTotal() / 1048576)) + "MB\n\nContinue? ";

        if (readYesNo(message, Response.YES) == Response.NO) {
          return ResultBuilder.createShellClientAbortOperationResult("Aborted deploy of " + jarOrDir + ".");
        }
      }

      return fileResult;
    }

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult) {
      return null;
    }
  }
}
