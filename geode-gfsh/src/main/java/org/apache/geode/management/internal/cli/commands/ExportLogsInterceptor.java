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
import java.nio.file.Paths;
import java.time.LocalDateTime;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.LogLevel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.ExportLogsFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

/**
 * after the export logs, will need to copy the tempFile to the desired location and delete the temp
 * file.
 */
public class ExportLogsInterceptor extends AbstractCliAroundInterceptor {
  private static final Logger logger = LogService.getLogger();

  @Override
  public ResultModel preExecution(GfshParseResult parseResult) {

    // validates groupId and memberIds not both set
    if (parseResult.getParamValueAsString("group") != null
        && parseResult.getParamValueAsString("member") != null) {
      return ResultModel.createError("Can't specify both group and member.");
    }

    // validate log level
    String logLevel = parseResult.getParamValueAsString("log-level");
    if (StringUtils.isBlank(logLevel) || LogLevel.getLevel(logLevel) == null) {
      return ResultModel.createError("Invalid log level: " + logLevel);
    }

    // validate start date and end date
    String start = parseResult.getParamValueAsString("start-time");
    String end = parseResult.getParamValueAsString("end-time");
    if (start != null && end != null) {
      // need to make sure end is later than start
      LocalDateTime startTime = ExportLogsFunction.parseTime(start);
      LocalDateTime endTime = ExportLogsFunction.parseTime(end);
      if (startTime.isAfter(endTime)) {
        return ResultModel.createError("start-time has to be earlier than end-time.");
      }
    }

    // validate onlyLogs and onlyStats
    boolean onlyLogs = (boolean) parseResult.getParamValue("logs-only");
    boolean onlyStats = (boolean) parseResult.getParamValue("stats-only");
    if (onlyLogs && onlyStats) {
      return ResultModel.createError("logs-only and stats-only can't both be true");
    }

    return ResultModel.createInfo("");
  }

  @Override
  public ResultModel postExecution(GfshParseResult parseResult, ResultModel commandResult,
      Path tempFile) {
    if (tempFile != null) {
      // in the command over http case, the command result is in the downloaded temp file
      Path dirPath;
      String dirName = parseResult.getParamValueAsString("dir");
      if (StringUtils.isBlank(dirName)) {
        dirPath = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
      } else {
        dirPath = Paths.get(dirName).toAbsolutePath();
      }
      String fileName = "exportedLogs_" + System.currentTimeMillis() + ".zip";
      File exportedLogFile = dirPath.resolve(fileName).toFile();
      try {
        FileUtils.copyFile(tempFile.toFile(), exportedLogFile);
        FileUtils.deleteQuietly(tempFile.toFile());
        return ResultModel.createInfo("Logs exported to: " + exportedLogFile.getAbsolutePath());
      } catch (IOException e) {
        logger.error(e.getMessage(), e);
        return ResultModel.createError(e.getMessage());
      }
    }

    // otherwise if result status is false, return it
    if (commandResult.getStatus() != Result.Status.OK) {
      return commandResult;
    }

    // if there is no downloaded file. File is saved on the locator/manager.
    return ResultModel.createInfo(
        "Logs exported to the connected member's file system: "
            + commandResult.getFileToDownload().toString());
  }
}
