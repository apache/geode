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

import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.FORMAT;
import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.ONLY_DATE_FORMAT;
import static org.apache.geode.management.internal.i18n.CliStrings.EXPORT_LOGS__ENDTIME;
import static org.apache.geode.management.internal.i18n.CliStrings.EXPORT_LOGS__STARTTIME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.regex.Pattern;

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

  /** To match {@link ExportLogsCommand#FORMAT} */
  private static final Pattern DATE_AND_TIME_PATTERN =
      Pattern.compile("^\\d{4}(/\\d{2}){5}/\\d{3}/.{3,}");

  /** To match {@link ExportLogsCommand#ONLY_DATE_FORMAT} */
  private static final Pattern DATE_ONLY_PATTERN = Pattern.compile("^\\d{4}(/\\d{2}){2}$");

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

    String start = parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME);
    String end = parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME);

    // First check start date and end date format
    ResultModel formatErrorResult = checkStartAndEndFormat(start, end);
    if (formatErrorResult != null) {
      return formatErrorResult;
    }

    // Check if start date and end date are able to be parsed correctly
    ResultModel parseErrorResult = checkStartAndEndParsing(start, end);
    if (parseErrorResult != null) {
      return parseErrorResult;
    }

    // Check that end is later than start
    LocalDateTime startTime = ExportLogsFunction.parseTime(start);
    LocalDateTime endTime = ExportLogsFunction.parseTime(end);
    if (startTime != null && endTime != null) {
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

    StringBuilder output = new StringBuilder();

    // Output start time as used by ExportLogsFunction, if specified
    String startTime = parseResult.getParamValueAsString(EXPORT_LOGS__STARTTIME);
    if (startTime != null && !startTime.isEmpty()) {
      output.append("Start time parsed as ")
          .append(ExportLogsFunction.parseTime(startTime))
          .append(" ")
          .append(TimeZone.getDefault().getDisplayName(false, TimeZone.SHORT))
          .append("\n");
    }

    // Output end time as used by ExportLogsFunction, if specified
    String endTime = parseResult.getParamValueAsString(EXPORT_LOGS__ENDTIME);
    if (endTime != null && !endTime.isEmpty()) {
      output.append("End time parsed as ")
          .append(ExportLogsFunction.parseTime(endTime))
          .append(" ")
          .append(TimeZone.getDefault().getDisplayName(false, TimeZone.SHORT))
          .append("\n");
    }

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
        output.append("Logs exported to: ")
            .append(exportedLogFile.getAbsolutePath());
        return ResultModel.createInfo(output.toString());
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
    output.append("Logs exported to the connected member's file system: ")
        .append(commandResult.getFileToDownload().toString());
    return ResultModel.createInfo(output.toString());
  }

  private ResultModel checkStartAndEndFormat(String start, String end) {
    StringBuilder formatErrorMessage = new StringBuilder();
    boolean formatError = false;
    if (start != null && !DATE_AND_TIME_PATTERN.matcher(start).matches()
        && !DATE_ONLY_PATTERN.matcher(start).matches()) {
      formatErrorMessage.append(EXPORT_LOGS__STARTTIME);
      formatError = true;
    }

    if (end != null && !DATE_AND_TIME_PATTERN.matcher(end).matches()
        && !DATE_ONLY_PATTERN.matcher(end).matches()) {
      if (formatError) {
        formatErrorMessage.append(" and ");
      }
      formatErrorMessage.append(EXPORT_LOGS__ENDTIME);
      formatError = true;
    }

    if (formatError) {
      formatErrorMessage.append(" had incorrect format. Valid formats are ")
          .append(ONLY_DATE_FORMAT).append(" and ").append(FORMAT);
      return ResultModel.createError(formatErrorMessage.toString());
    }
    return null;
  }

  private ResultModel checkStartAndEndParsing(String start, String end) {
    StringBuilder parseErrorMessage = new StringBuilder();
    boolean parseError = false;

    SimpleDateFormat dateAndTimeFormat = new SimpleDateFormat(FORMAT);
    SimpleDateFormat dateOnlyFormat = new SimpleDateFormat(ONLY_DATE_FORMAT);
    if (start != null) {
      try {
        // If the input is intended to be parsed as date and time, use the date and time format
        if (start.length() > 10) {
          dateAndTimeFormat.parse(start);
        } else {
          dateOnlyFormat.parse(start);
        }
      } catch (ParseException e) {
        parseErrorMessage.append(EXPORT_LOGS__STARTTIME);
        parseError = true;
      }
    }

    if (end != null) {
      try {
        // If the input is intended to be parsed as date and time, use the date and time format
        if (end.length() > 10) {
          dateAndTimeFormat.parse(end);
        } else {
          dateOnlyFormat.parse(end);
        }
      } catch (ParseException e) {
        if (parseError) {
          parseErrorMessage.append(" and ");
        }
        parseErrorMessage.append(EXPORT_LOGS__ENDTIME);
        parseError = true;
      }
    }

    if (parseError) {
      parseErrorMessage.append(" could not be parsed to valid date/time.");
      return ResultModel.createError(parseErrorMessage.toString());
    }
    return null;
  }
}
