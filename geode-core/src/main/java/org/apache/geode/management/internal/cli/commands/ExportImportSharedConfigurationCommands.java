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

import org.apache.commons.io.FileUtils;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.FileResult;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

/****
 * Commands for the shared configuration
 *
 */
@SuppressWarnings("unused")
public class ExportImportSharedConfigurationCommands extends AbstractCommandsSupport {
  @CliCommand(value = {CliStrings.EXPORT_SHARED_CONFIG},
      help = CliStrings.EXPORT_SHARED_CONFIG__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.ExportImportSharedConfigurationCommands$ExportInterceptor",
      readsSharedConfiguration = true, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result exportSharedConfig(@CliOption(key = {CliStrings.EXPORT_SHARED_CONFIG__FILE},
      mandatory = true, help = CliStrings.EXPORT_SHARED_CONFIG__FILE__HELP) String zipFileName,

      @CliOption(key = {CliStrings.EXPORT_SHARED_CONFIG__DIR},
          help = CliStrings.EXPORT_SHARED_CONFIG__DIR__HELP) String dir) {

    InternalLocator locator = InternalLocator.getLocator();
    if (!locator.isSharedConfigurationRunning()) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.SHARED_CONFIGURATION_NOT_STARTED);
    }

    ClusterConfigurationService sc = locator.getSharedConfiguration();
    File zipFile = new File(zipFileName);
    zipFile.getParentFile().mkdirs();

    Result result;
    try {
      for (Configuration config : sc.getEntireConfiguration().values()) {
        sc.writeConfig(config);
      }
      ZipUtils.zip(sc.getSharedConfigurationDirPath(), zipFile.getCanonicalPath());

      InfoResultData infoData = ResultBuilder.createInfoResultData();
      byte[] byteData = FileUtils.readFileToByteArray(zipFile);
      infoData.addAsFile(zipFileName, byteData, InfoResultData.FILE_TYPE_BINARY,
          CliStrings.EXPORT_SHARED_CONFIG__DOWNLOAD__MSG, false);
      result = ResultBuilder.buildResult(infoData);
    } catch (Exception e) {
      ErrorResultData errorData = ResultBuilder.createErrorResultData();
      errorData.addLine("Export failed");
      logSevere(e);
      result = ResultBuilder.buildResult(errorData);
    } finally {
      zipFile.delete();
    }

    return result;
  }


  @CliCommand(value = {CliStrings.IMPORT_SHARED_CONFIG},
      help = CliStrings.IMPORT_SHARED_CONFIG__HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.ExportImportSharedConfigurationCommands$ImportInterceptor",
      writesToSharedConfiguration = true, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  @SuppressWarnings("unchecked")
  public Result importSharedConfig(@CliOption(key = {CliStrings.IMPORT_SHARED_CONFIG__ZIP},
      mandatory = true, help = CliStrings.IMPORT_SHARED_CONFIG__ZIP__HELP) String zip) {

    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

    if (!CliUtil.getAllNormalMembers(cache).isEmpty()) {
      return ResultBuilder
          .createGemFireErrorResult(CliStrings.IMPORT_SHARED_CONFIG__CANNOT__IMPORT__MSG);
    }

    byte[][] shellBytesData = CommandExecutionContext.getBytesFromShell();
    String zipFileName = CliUtil.bytesToNames(shellBytesData)[0];
    byte[] zipBytes = CliUtil.bytesToData(shellBytesData)[0];

    InternalLocator locator = InternalLocator.getLocator();

    if (!locator.isSharedConfigurationRunning()) {
      ErrorResultData errorData = ResultBuilder.createErrorResultData();
      errorData.addLine(CliStrings.SHARED_CONFIGURATION_NOT_STARTED);
      return ResultBuilder.buildResult(errorData);
    }

    Result result;
    File zipFile = new File(zipFileName);
    try {
      ClusterConfigurationService sc = locator.getSharedConfiguration();

      // backup the old config
      for (Configuration config : sc.getEntireConfiguration().values()) {
        sc.writeConfig(config);
      }
      sc.renameExistingSharedConfigDirectory();

      sc.clearSharedConfiguration();
      FileUtils.writeByteArrayToFile(zipFile, zipBytes);
      ZipUtils.unzip(zipFileName, sc.getSharedConfigurationDirPath());

      // load it from the disk
      sc.loadSharedConfigurationFromDisk();

      InfoResultData infoData = ResultBuilder.createInfoResultData();
      infoData.addLine(CliStrings.IMPORT_SHARED_CONFIG__SUCCESS__MSG);
      result = ResultBuilder.buildResult(infoData);
    } catch (Exception e) {
      ErrorResultData errorData = ResultBuilder.createErrorResultData();
      errorData.addLine("Import failed");
      logSevere(e);
      result = ResultBuilder.buildResult(errorData);
    } finally {
      FileUtils.deleteQuietly(zipFile);
    }
    return result;
  }

  @CliAvailabilityIndicator({CliStrings.EXPORT_SHARED_CONFIG, CliStrings.IMPORT_SHARED_CONFIG})
  public boolean sharedConfigCommandsAvailable() {
    boolean isAvailable = true; // always available on server
    if (CliUtil.isGfshVM()) { // in gfsh check if connected
      isAvailable = getGfsh() != null && getGfsh().isConnectedAndReady();
    }
    return isAvailable;
  }

  /**
   * Interceptor used by gfsh to intercept execution of export shared config command at "shell".
   */
  public static class ExportInterceptor extends AbstractCliAroundInterceptor {
    private String saveDirString;
    private static final Logger logger = LogService.getLogger();

    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Map<String, String> paramValueMap = parseResult.getParamValueStrings();
      String zip = paramValueMap.get(CliStrings.EXPORT_SHARED_CONFIG__FILE);

      if (!zip.endsWith(".zip")) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENSION, ".zip"));
      }
      return ResultBuilder.createInfoResult("OK");
    }

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult) {
      if (commandResult.hasIncomingFiles()) {
        try {
          Map<String, String> paramValueMap = parseResult.getParamValueStrings();
          String dir = paramValueMap.get(CliStrings.EXPORT_SHARED_CONFIG__DIR);
          dir = (dir == null) ? null : dir.trim();

          File saveDirFile = new File(".");

          if (dir != null && !dir.isEmpty()) {
            saveDirFile = new File(dir);
            if (saveDirFile.exists()) {
              if (!saveDirFile.isDirectory()) {
                return ResultBuilder.createGemFireErrorResult(
                    CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__MSG__NOT_A_DIRECTORY, dir));
              }
            } else if (!saveDirFile.mkdirs()) {
              return ResultBuilder.createGemFireErrorResult(
                  CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__MSG__CANNOT_CREATE_DIR, dir));
            }
          }
          if (!saveDirFile.canWrite()) {
            return ResultBuilder.createGemFireErrorResult(
                CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__MSG__NOT_WRITEABLE,
                    saveDirFile.getCanonicalPath()));
          }
          saveDirString = saveDirFile.getAbsolutePath();
          commandResult.saveIncomingFiles(saveDirString);
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
      Map<String, String> paramValueMap = parseResult.getParamValueStrings();

      String zip = paramValueMap.get(CliStrings.IMPORT_SHARED_CONFIG__ZIP);

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

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult) {
      return null;
    }
  }

}
