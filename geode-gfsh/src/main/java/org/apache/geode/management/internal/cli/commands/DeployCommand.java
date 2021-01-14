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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.healthmarketscience.rmiio.exporter.RemoteStreamExporter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.ManagementAgent;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.domain.DeploymentInfo;
import org.apache.geode.management.internal.cli.functions.DeployFunction;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.model.FileResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.cli.util.DeploymentInfoTableUtil;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.management.internal.utils.JarFileUtils;
import org.apache.geode.security.ResourcePermission;

public class DeployCommand extends GfshCommand {
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
      isFileUploaded = true, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DEPLOY)
  public ResultModel deploy(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS}, help = CliStrings.DEPLOY__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) String[] groups,
      @CliOption(key = {CliStrings.JAR, CliStrings.JARS}, optionContext = ConverterHint.JARFILES,
          help = CliStrings.DEPLOY__JAR__HELP) String[] jars,
      @CliOption(key = {CliStrings.DEPLOY__DIR}, optionContext = ConverterHint.JARDIR,
          help = CliStrings.DEPLOY__DIR__HELP) String dir)
      throws IOException {

    ResultModel result = new ResultModel();
    TabularResultModel deployResult = result.addTable("deployResult");

    List<String> jarFullPaths = CommandExecutionContext.getFilePathFromShell();

    verifyJarContent(jarFullPaths);

    verifyDeploymentNameOrJarNamesDontContainGeodeName(deploymentName, jarFullPaths);

    Set<DistributedMember> targetMembers;
    targetMembers = findMembers(groups, null);

    List<List<Object>> results = new LinkedList<>();
    ManagementAgent agent = ((SystemManagementService) getManagementService()).getManagementAgent();
    RemoteStreamExporter exporter = agent.getRemoteStreamExporter();

    results = deployJars(jarFullPaths, targetMembers, results, exporter);

    List<CliFunctionResult> cleanedResults = CliFunctionResult.cleanResults(results);

    List<DeploymentInfo> deploymentInfos =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(cleanedResults);
    DeploymentInfoTableUtil.writeDeploymentInfoToTable(
        new String[] {"Member", "JAR", "JAR Location"}, deployResult,
        deploymentInfos);

    if (result.getStatus() == Result.Status.OK) {
      InternalConfigurationPersistenceService sc = getConfigurationPersistenceService();
      if (sc == null) {
        result.addInfo().addLine(CommandExecutor.SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      } else {
        sc.addJarsToThisLocator(jarFullPaths, groups);
      }
    }
    return result;
  }

  private List<List<Object>> deployJars(List<String> jarFullPaths,
      Set<DistributedMember> targetMembers,
      List<List<Object>> results,
      RemoteStreamExporter exporter)
      throws FileNotFoundException, java.rmi.RemoteException {
    for (DistributedMember member : targetMembers) {
      List<RemoteInputStream> remoteStreams = new ArrayList<>();
      List<String> jarNames = new ArrayList<>();
      try {
        for (String jarFullPath : jarFullPaths) {
          FileInputStream fileInputStream = null;
          try {
            fileInputStream = new FileInputStream(jarFullPath);
            remoteStreams.add(exporter.export(new SimpleRemoteInputStream(fileInputStream)));
            jarNames.add(FilenameUtils.getName(jarFullPath));
          } catch (Exception ex) {
            if (fileInputStream != null) {
              try {
                fileInputStream.close();
              } catch (IOException ignore) {
              }
            }
            throw ex;
          }
        }

        // this deploys the jars to all the matching servers
        ResultCollector<?, ?> resultCollector =
            executeFunction(this.deployFunction,
                new Object[] {jarNames, remoteStreams}, member);

        @SuppressWarnings("unchecked")
        final List<List<Object>> resultCollectorResult =
            (List<List<Object>>) resultCollector.getResult();
        results.add(resultCollectorResult.get(0));
      } finally {
        for (RemoteInputStream ris : remoteStreams) {
          try {
            ris.close(true);
          } catch (IOException ex) {
            // Ignored. the stream may have already been closed.
          }
        }
      }
    }
    return results;
  }

  private void verifyJarContent(List<String> jarNames) {
    for (String jarName : jarNames) {
      File jar = new File(jarName);
      if (!JarFileUtils.hasValidJarContent(jar)) {
        throw new IllegalArgumentException(
            "File does not contain valid JAR content: " + jar.getName());
      }
    }
  }

  @Override
  public boolean affectsClusterConfiguration() {
    return true;
  }

  /**
   * Interceptor used by gfsh to intercept execution of deploy command at "shell".
   */
  public static class Interceptor extends AbstractCliAroundInterceptor {
    private final DecimalFormat numFormatter = new DecimalFormat("###,##0.00");

    /**
     *
     * @return FileResult object or ResultModel in case of errors
     */
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      String[] jars = (String[]) parseResult.getParamValue("jar");
      String dir = (String) parseResult.getParamValue("dir");

      if (ArrayUtils.isEmpty(jars) && StringUtils.isBlank(dir)) {
        return ResultModel.createError(
            "Parameter \"jar\" or \"dir\" is required. Use \"help <command name>\" for assistance.");
      }

      if (ArrayUtils.isNotEmpty(jars) && StringUtils.isNotBlank(dir)) {
        return ResultModel.createError("Parameters \"jar\" and \"dir\" can not both be specified.");
      }

      ResultModel result = new ResultModel();
      if (jars != null) {
        for (String jar : jars) {
          File jarFile = new File(jar);
          if (!jarFile.exists()) {
            return ResultModel.createError(jar + " not found.");
          }
          result.addFile(jarFile, FileResultModel.FILE_TYPE_FILE);
        }
      } else {
        File fileDir = new File(dir);
        if (!fileDir.isDirectory()) {
          return ResultModel.createError(dir + " is not a directory");
        }
        File[] childJarFile = fileDir.listFiles(ManagementUtils.JAR_FILE_FILTER);
        for (File file : childJarFile) {
          result.addFile(file, FileResultModel.FILE_TYPE_FILE);
        }
      }

      // check if user wants to upload with the computed file size
      String message =
          "\nDeploying files: " + result.getFormattedFileList() + "\nTotal file size is: "
              + this.numFormatter.format((double) result.computeFileSizeTotal() / ONE_MB)
              + "MB\n\nContinue? ";

      if (readYesNo(message, Response.YES) == Response.NO) {
        return ResultModel.createError(
            "Aborted deploy of " + result.getFormattedFileList() + ".");
      }

      return result;
    }
  }
}
