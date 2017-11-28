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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.FileResult;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.functions.GetRegionNamesFunction;
import org.apache.geode.management.internal.configuration.functions.RecreateCacheFunction;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * Commands for the cluster configuration
 */
@SuppressWarnings("unused")
public class ExportImportClusterConfigurationCommands implements GfshCommand {

  @CliCommand(value = {CliStrings.EXPORT_SHARED_CONFIG},
      help = CliStrings.EXPORT_SHARED_CONFIG__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.ExportImportClusterConfigurationCommands$ExportInterceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result exportSharedConfig(@CliOption(key = {CliStrings.EXPORT_SHARED_CONFIG__FILE},
      mandatory = true, help = CliStrings.EXPORT_SHARED_CONFIG__FILE__HELP) String zipFileName) {

    InternalLocator locator = InternalLocator.getLocator();
    if (locator == null || !locator.isSharedConfigurationRunning()) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.SHARED_CONFIGURATION_NOT_STARTED);
    }

    Path tempDir;
    try {
      tempDir = Files.createTempDirectory("clusterConfig");
    } catch (IOException e) {
      if (Gfsh.getCurrentInstance() != null) {
        Gfsh.getCurrentInstance().logSevere(e.getMessage(), e);
      }
      ErrorResultData errorData =
          ResultBuilder.createErrorResultData().addLine("Unable to create temp directory");
      return ResultBuilder.buildResult(errorData);
    }

    File zipFile = tempDir.resolve("exportedCC.zip").toFile();
    ClusterConfigurationService sc = locator.getSharedConfiguration();

    Result result;
    try {
      for (Configuration config : sc.getConfigurationRegion().values()) {
        sc.writeConfigToFile(config);
      }
      ZipUtils.zipDirectory(sc.getSharedConfigurationDirPath(), zipFile.getCanonicalPath());

      InfoResultData infoData = ResultBuilder.createInfoResultData();
      byte[] byteData = FileUtils.readFileToByteArray(zipFile);
      infoData.addAsFile(zipFileName, byteData, InfoResultData.FILE_TYPE_BINARY,
          CliStrings.EXPORT_SHARED_CONFIG__DOWNLOAD__MSG, false);
      result = ResultBuilder.buildResult(infoData);
    } catch (Exception e) {
      ErrorResultData errorData = ResultBuilder.createErrorResultData();
      errorData.addLine("Export failed");
      if (Gfsh.getCurrentInstance() != null) {
        Gfsh.getCurrentInstance().logSevere(e.getMessage(), e);
      }
      result = ResultBuilder.buildResult(errorData);
    } finally {
      zipFile.delete();
    }

    return result;
  }

  @CliCommand(value = {CliStrings.IMPORT_SHARED_CONFIG},
      help = CliStrings.IMPORT_SHARED_CONFIG__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.ExportImportClusterConfigurationCommands$ImportInterceptor",
      isFileUploaded = true, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  @SuppressWarnings("unchecked")
  public Result importSharedConfig(@CliOption(key = {CliStrings.IMPORT_SHARED_CONFIG__ZIP},
      mandatory = true, help = CliStrings.IMPORT_SHARED_CONFIG__ZIP__HELP) String zip) {

    InternalLocator locator = InternalLocator.getLocator();

    if (!locator.isSharedConfigurationRunning()) {
      ErrorResultData errorData = ResultBuilder.createErrorResultData();
      errorData.addLine(CliStrings.SHARED_CONFIGURATION_NOT_STARTED);
      return ResultBuilder.buildResult(errorData);
    }

    InternalCache cache = getCache();

    Set<DistributedMember> servers = CliUtil.getAllNormalMembers(cache);

    Set<String> regionsWithData = servers.stream().map(this::getRegionNamesOnServer)
        .flatMap(Collection::stream).collect(toSet());

    if (!regionsWithData.isEmpty()) {
      return ResultBuilder
          .createGemFireErrorResult("Cannot import cluster configuration with existing regions: "
              + regionsWithData.stream().collect(joining(",")));
    }

    byte[][] shellBytesData = CommandExecutionContext.getBytesFromShell();
    String zipFileName = CliUtil.bytesToNames(shellBytesData)[0];
    byte[] zipBytes = CliUtil.bytesToData(shellBytesData)[0];

    Result result;
    InfoResultData infoData = ResultBuilder.createInfoResultData();
    File zipFile = new File(zipFileName);
    try {
      ClusterConfigurationService sc = locator.getSharedConfiguration();

      // backup the old config
      for (Configuration config : sc.getConfigurationRegion().values()) {
        sc.writeConfigToFile(config);
      }
      sc.renameExistingSharedConfigDirectory();

      FileUtils.writeByteArrayToFile(zipFile, zipBytes);
      ZipUtils.unzip(zipFileName, sc.getSharedConfigurationDirPath());

      // load it from the disk
      sc.loadSharedConfigurationFromDisk();
      infoData.addLine(CliStrings.IMPORT_SHARED_CONFIG__SUCCESS__MSG);

    } catch (Exception e) {
      ErrorResultData errorData = ResultBuilder.createErrorResultData();
      errorData.addLine("Import failed");
      if (Gfsh.getCurrentInstance() != null) {
        Gfsh.getCurrentInstance().logSevere(e.getMessage(), e);
      }
      result = ResultBuilder.buildResult(errorData);
      // if import is unsuccessful, don't need to bounce the server.
      return result;
    } finally {
      FileUtils.deleteQuietly(zipFile);
    }

    // Bounce the cache of each member
    Set<CliFunctionResult> functionResults =
        servers.stream().map(this::reCreateCache).collect(toSet());

    for (CliFunctionResult functionResult : functionResults) {
      if (functionResult.isSuccessful()) {
        infoData.addLine("Successfully applied the imported cluster configuration on "
            + functionResult.getMemberIdOrName());
      } else {
        infoData.addLine("Failed to apply the imported cluster configuration on "
            + functionResult.getMemberIdOrName() + " due to " + functionResult.getMessage());
      }
    }

    result = ResultBuilder.buildResult(infoData);
    return result;
  }

  private Set<String> getRegionNamesOnServer(DistributedMember server) {
    ResultCollector rc = executeFunction(new GetRegionNamesFunction(), null, server);
    List<Set<String>> results = (List<Set<String>>) rc.getResult();

    return results.get(0);
  }

  private CliFunctionResult reCreateCache(DistributedMember server) {
    ResultCollector rc = executeFunction(new RecreateCacheFunction(), null, server);
    List<CliFunctionResult> results = (List<CliFunctionResult>) rc.getResult();

    return results.get(0);
  }

  /**
   * Interceptor used by gfsh to intercept execution of export shared config command at "shell".
   */
  public static class ExportInterceptor extends AbstractCliAroundInterceptor {
    private String saveDirString;
    private static final Logger logger = LogService.getLogger();

    @Override
    public Result preExecution(GfshParseResult parseResult) {
      String zip = parseResult.getParamValueAsString(CliStrings.EXPORT_SHARED_CONFIG__FILE);

      if (!zip.endsWith(".zip")) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, ".zip"));
      }
      return ResultBuilder.createInfoResult("OK");
    }

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult, Path tempFile) {
      if (commandResult.hasIncomingFiles()) {
        try {
          commandResult.saveIncomingFiles(System.getProperty("user.dir"));
          return commandResult;
        } catch (IOException ioex) {
          logger.error(ioex);
          return ResultBuilder.createShellClientErrorResult(
              CliStrings.EXPORT_SHARED_CONFIG__UNABLE__TO__EXPORT__CONFIG + ": "
                  + ioex.getMessage());
        }
      }
      return null;
    }
  }

  public static class ImportInterceptor extends AbstractCliAroundInterceptor {

    public Result preExecution(GfshParseResult parseResult) {
      String zip = parseResult.getParamValueAsString(CliStrings.IMPORT_SHARED_CONFIG__ZIP);

      zip = StringUtils.trim(zip);

      if (zip == null) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(
            CliStrings.IMPORT_SHARED_CONFIG__PROVIDE__ZIP, CliStrings.IMPORT_SHARED_CONFIG__ZIP));
      }
      if (!zip.endsWith(CliStrings.ZIP_FILE_EXTENSION)) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, CliStrings.ZIP_FILE_EXTENSION));
      }

      FileResult fileResult;

      try {
        fileResult = new FileResult(new String[] {zip});
      } catch (FileNotFoundException fnfex) {
        return ResultBuilder.createUserErrorResult("'" + zip + "' not found.");
      } catch (IOException ioex) {
        return ResultBuilder
            .createGemFireErrorResult(ioex.getClass().getName() + ": " + ioex.getMessage());
      }

      return fileResult;
    }
  }

}
