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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.AbstractCliAroundInterceptor;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.ExportSharedConfigurationFunction;
import com.gemstone.gemfire.management.internal.cli.functions.ImportSharedConfigurationArtifactsFunction;
import com.gemstone.gemfire.management.internal.cli.functions.LoadSharedConfigurationFunction;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.remote.CommandExecutionContext;
import com.gemstone.gemfire.management.internal.cli.result.FileResult;
import com.gemstone.gemfire.management.internal.cli.result.InfoResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

/****
 * Commands for the shared configuration
 *
 */
@SuppressWarnings("unused")
public class ExportImportSharedConfigurationCommands extends AbstractCommandsSupport {

  private final ExportSharedConfigurationFunction exportSharedConfigurationFunction = new ExportSharedConfigurationFunction();
  private final ImportSharedConfigurationArtifactsFunction importSharedConfigurationFunction = new ImportSharedConfigurationArtifactsFunction();
  private final LoadSharedConfigurationFunction loadSharedConfiguration = new LoadSharedConfigurationFunction();

  @CliCommand(value = { CliStrings.EXPORT_SHARED_CONFIG }, help = CliStrings.EXPORT_SHARED_CONFIG__HELP)
  @CliMetaData(interceptor = "com.gemstone.gemfire.management.internal.cli.commands.ExportImportSharedConfigurationCommands$ExportInterceptor",  readsSharedConfiguration=true, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result exportSharedConfig(
      @CliOption(key = { CliStrings.EXPORT_SHARED_CONFIG__FILE}, 
      mandatory = true,
      help = CliStrings.EXPORT_SHARED_CONFIG__FILE__HELP)
      String zipFileName,

      @CliOption(key = { CliStrings.EXPORT_SHARED_CONFIG__DIR},
      help = CliStrings.EXPORT_SHARED_CONFIG__DIR__HELP)
      String dir
      ) {
    Result result;

    InfoResultData infoData = ResultBuilder.createInfoResultData();
    TabularResultData errorTable = ResultBuilder.createTabularResultData();
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    Set<DistributedMember> locators = new HashSet<DistributedMember>(cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration().keySet());
    byte [] byteData;
    boolean success = false;

    if (!locators.isEmpty()) {
      for (DistributedMember locator : locators) {
        ResultCollector<?, ?> rc = CliUtil.executeFunction(exportSharedConfigurationFunction, null, locator);
        @SuppressWarnings("unchecked")
        List<CliFunctionResult> results = (List<CliFunctionResult>) rc.getResult();
        CliFunctionResult functionResult = results.get(0);

        if (functionResult.isSuccessful()) {
          byteData = functionResult.getByteData();
          infoData.addAsFile(zipFileName, byteData, InfoResultData.FILE_TYPE_BINARY, CliStrings.EXPORT_SHARED_CONFIG__DOWNLOAD__MSG, false);
          success = true;
          break;
        } else {
          errorTable.accumulate(CliStrings.LOCATOR_HEADER, functionResult.getMemberIdOrName());
          errorTable.accumulate(CliStrings.ERROR__MSG__HEADER, functionResult.getMessage());
        }
      }
      if (success) {
        result = ResultBuilder.buildResult(infoData);
      } else {
        errorTable.setStatus(Result.Status.ERROR);
        result = ResultBuilder.buildResult(errorTable);
      }
    } else {
      result = ResultBuilder.createGemFireErrorResult(CliStrings.SHARED_CONFIGURATION_NO_LOCATORS_WITH_SHARED_CONFIGURATION);
    }
    return result;
  }

  @CliCommand(value = { CliStrings.IMPORT_SHARED_CONFIG }, help = CliStrings.IMPORT_SHARED_CONFIG__HELP)
  @CliMetaData(interceptor = "com.gemstone.gemfire.management.internal.cli.commands.ExportImportSharedConfigurationCommands$ImportInterceptor", writesToSharedConfiguration=true, relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE)
  @SuppressWarnings("unchecked")
  public Result importSharedConfig(
      @CliOption(key = { CliStrings.IMPORT_SHARED_CONFIG__ZIP},
      mandatory = true,
      help = CliStrings.IMPORT_SHARED_CONFIG__ZIP__HELP)
      String zip) {

    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

    if (!CliUtil.getAllNormalMembers(cache).isEmpty()) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.IMPORT_SHARED_CONFIG__CANNOT__IMPORT__MSG);
    }

    Set<DistributedMember> locators = new HashSet<DistributedMember>(cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration().keySet());

    if (locators.isEmpty()) {
      return ResultBuilder.createGemFireErrorResult(CliStrings.NO_LOCATORS_WITH_SHARED_CONFIG);
    }

    Result result;
    byte[][] shellBytesData = CommandExecutionContext.getBytesFromShell();
    String[] names = CliUtil.bytesToNames(shellBytesData);
    byte[][] bytes = CliUtil.bytesToData(shellBytesData);

    String zipFileName = names[0];
    byte[] zipBytes = bytes[0];

    Object [] args = new Object[] {zipFileName, zipBytes};

    InfoResultData infoData = ResultBuilder.createInfoResultData();
    TabularResultData errorTable = ResultBuilder.createTabularResultData();

    boolean success = false;
    boolean copySuccess = false;

    ResultCollector<?, ?> rc =  CliUtil.executeFunction(importSharedConfigurationFunction, args, locators);
    List<CliFunctionResult> functionResults =  CliFunctionResult.cleanResults((List<CliFunctionResult>) rc.getResult());

    for (CliFunctionResult functionResult : functionResults) {
      if (!functionResult.isSuccessful()) {
        errorTable.accumulate(CliStrings.LOCATOR_HEADER, functionResult.getMemberIdOrName());
        errorTable.accumulate(CliStrings.ERROR__MSG__HEADER, functionResult.getMessage());
      } else {
        copySuccess = true;
      }
    }

    if (!copySuccess ) {
      errorTable.setStatus(Result.Status.ERROR);
      return ResultBuilder.buildResult(errorTable);
    }
    
    errorTable = ResultBuilder.createTabularResultData();

    for (DistributedMember locator : locators) {
      rc = CliUtil.executeFunction(loadSharedConfiguration, args, locator);
      functionResults = (List<CliFunctionResult>) rc.getResult();
      CliFunctionResult functionResult = functionResults.get(0);
      if (functionResult.isSuccessful()) {
        success = true;
        infoData.addLine(functionResult.getMessage());
        break; 
      } else {
        errorTable.accumulate(CliStrings.LOCATOR_HEADER, functionResult.getMemberIdOrName());
        errorTable.accumulate(CliStrings.ERROR__MSG__HEADER, functionResult.getMessage());
      }
    }

    if (success) {
      result = ResultBuilder.buildResult(infoData);
    } else {
      errorTable.setStatus(Result.Status.ERROR);
      result = ResultBuilder.buildResult(errorTable);
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

    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Map<String, String> paramValueMap = parseResult.getParamValueStrings();
      String zip = paramValueMap.get(CliStrings.EXPORT_SHARED_CONFIG__FILE);
      
      if (!zip.endsWith(".zip")) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENTION, ".zip"));
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
              if (!saveDirFile.isDirectory())
                return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__MSG__NOT_A_DIRECTORY, dir));
            } else if (!saveDirFile.mkdirs()) {
              return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__MSG__CANNOT_CREATE_DIR, dir));
            }
          }
          try {
            if (!saveDirFile.canWrite()) {
              return ResultBuilder.createGemFireErrorResult(CliStrings.format(CliStrings.EXPORT_SHARED_CONFIG__MSG__NOT_WRITEABLE, saveDirFile
                  .getCanonicalPath()));
            }
          } catch (IOException ioex) {
          }
          saveDirString = saveDirFile.getAbsolutePath();
          commandResult.saveIncomingFiles(saveDirString);
          return commandResult;
        } catch (IOException ioex) {
          return ResultBuilder.createShellClientErrorResult(CliStrings.EXPORT_SHARED_CONFIG__UNABLE__TO__EXPORT__CONFIG);
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
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.IMPORT_SHARED_CONFIG__PROVIDE__ZIP, CliStrings.IMPORT_SHARED_CONFIG__ZIP));
      } 
      if (!zip.endsWith(CliStrings.ZIP_FILE_EXTENSION)) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.INVALID_FILE_EXTENTION, CliStrings.ZIP_FILE_EXTENSION));
      }

      FileResult fileResult;

      try {
        fileResult = new FileResult(new String[] { zip });
      } catch (FileNotFoundException fnfex) {
        return ResultBuilder.createUserErrorResult("'" + zip + "' not found.");
      } catch (IOException ioex) {
        return ResultBuilder.createGemFireErrorResult(ioex.getClass().getName() + ": "
            + ioex.getMessage());
      }

      return fileResult;
    }
    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult) {
      return null;
    }
  }
  
}
