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
import org.apache.commons.lang.StringUtils;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.ExportLogsFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.util.ExportLogsCacheWriter;
import org.apache.geode.internal.logging.log4j.LogLevel;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ExportLogCommand implements CommandMarker {
  public final static String FORMAT = "yyyy/MM/dd/HH/mm/ss/SSS/z";
  public final static String ONLY_DATE_FORMAT = "yyyy/MM/dd";
  private final static Logger logger = LogService.getLogger();
  public final static String DEFAULT_EXPORT_LOG_LEVEL = "ALL";

  @CliCommand(value = CliStrings.EXPORT_LOGS, help = CliStrings.EXPORT_LOGS__HELP)
  @CliMetaData(shellOnly = false, isFileDownloadOverHttp = true,
      interceptor = "org.apache.geode.management.internal.cli.commands.ExportLogCommand$ExportLogsInterceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public Result exportLogs(
      @CliOption(key = CliStrings.EXPORT_LOGS__DIR, help = CliStrings.EXPORT_LOGS__DIR__HELP,
          mandatory = false) String dirName,
      @CliOption(key = CliStrings.EXPORT_LOGS__GROUP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.EXPORT_LOGS__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.EXPORT_LOGS__MEMBER,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.EXPORT_LOGS__MEMBER__HELP) String[] memberIds,
      @CliOption(key = CliStrings.EXPORT_LOGS__LOGLEVEL,
          unspecifiedDefaultValue = DEFAULT_EXPORT_LOG_LEVEL,
          optionContext = ConverterHint.LOG_LEVEL,
          help = CliStrings.EXPORT_LOGS__LOGLEVEL__HELP) String logLevel,
      @CliOption(key = CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL, unspecifiedDefaultValue = "false",
          help = CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL__HELP) boolean onlyLogLevel,
      @CliOption(key = CliStrings.EXPORT_LOGS__MERGELOG, unspecifiedDefaultValue = "false",
          help = CliStrings.EXPORT_LOGS__MERGELOG__HELP) boolean mergeLog,
      @CliOption(key = CliStrings.EXPORT_LOGS__STARTTIME,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.EXPORT_LOGS__STARTTIME__HELP) String start,
      @CliOption(key = CliStrings.EXPORT_LOGS__ENDTIME,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.EXPORT_LOGS__ENDTIME__HELP) String end,
      @CliOption(key = CliStrings.EXPORT_LOGS__LOGSONLY, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.EXPORT_LOGS__LOGSONLY__HELP) boolean logsOnly,
      @CliOption(key = CliStrings.EXPORT_LOGS__STATSONLY, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.EXPORT_LOGS__STATSONLY__HELP) boolean statsOnly) {
    Result result = null;
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    try {
      Set<DistributedMember> targetMembers =
          CliUtil.findMembersIncludingLocators(groups, memberIds);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      Map<String, Path> zipFilesFromMembers = new HashMap<>();
      for (DistributedMember server : targetMembers) {
        Region region = ExportLogsFunction.createOrGetExistingExportLogsRegion(true, cache);

        ExportLogsCacheWriter cacheWriter =
            (ExportLogsCacheWriter) region.getAttributes().getCacheWriter();

        cacheWriter.startFile(server.getName());

        CliUtil.executeFunction(new ExportLogsFunction(),
            new ExportLogsFunction.Args(start, end, logLevel, onlyLogLevel, logsOnly, statsOnly),
            server).getResult();
        Path zipFile = cacheWriter.endFile();
        ExportLogsFunction.destroyExportLogsRegion(cache);

        // only put the zipfile in the map if it is not null
        if (zipFile != null) {
          logger.info("Received zip file from member {}: {}", server.getId(), zipFile);
          zipFilesFromMembers.put(server.getId(), zipFile);
        }
      }

      if (zipFilesFromMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult("No files to be exported.");
      }

      Path tempDir = Files.createTempDirectory("exportedLogs");
      // make sure the directory is created, so that even if there is no files unzipped to this dir,
      // we can
      // still zip it and send an empty zip file back to the client
      Path exportedLogsDir = tempDir.resolve("exportedLogs");
      FileUtils.forceMkdir(exportedLogsDir.toFile());

      for (Path zipFile : zipFilesFromMembers.values()) {
        Path unzippedMemberDir =
            exportedLogsDir.resolve(zipFile.getFileName().toString().replace(".zip", ""));
        ZipUtils.unzip(zipFile.toAbsolutePath().toString(), unzippedMemberDir.toString());
        FileUtils.deleteQuietly(zipFile.toFile());
      }

      Path workingDir = Paths.get(System.getProperty("user.dir"));
      Path exportedLogsZipFile = workingDir
          .resolve("exportedLogs_" + System.currentTimeMillis() + ".zip").toAbsolutePath();

      logger.info("Zipping into: " + exportedLogsZipFile.toString());
      ZipUtils.zipDirectory(exportedLogsDir, exportedLogsZipFile);
      FileUtils.deleteDirectory(tempDir.toFile());
      result = ResultBuilder.createInfoResult(exportedLogsZipFile.toString());
    } catch (Exception ex) {
      logger.error(ex, ex);
      result = ResultBuilder.createGemFireErrorResult(ex.getMessage());
    } finally {
      ExportLogsFunction.destroyExportLogsRegion(cache);
    }
    logger.debug("Exporting logs returning = {}", result);
    return result;
  }

  /**
   * after the export logs, will need to copy the tempFile to the desired location and delete the
   * temp file.
   */
  public static class ExportLogsInterceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      // the arguments are in the order of it's being declared
      Map<String, String> arguments = parseResult.getParamValueStrings();

      // validates groupId and memberIds not both set
      if (arguments.get("group") != null && arguments.get("member") != null) {
        return ResultBuilder.createUserErrorResult("Can't specify both group and member.");
      }

      // validate log level
      String logLevel = arguments.get("log-level");
      if (StringUtils.isBlank(logLevel) || LogLevel.getLevel(logLevel) == null) {
        return ResultBuilder.createUserErrorResult("Invalid log level: " + logLevel);
      }

      // validate start date and end date
      String start = arguments.get("start-time");
      String end = arguments.get("end-time");
      if (start != null && end != null) {
        // need to make sure end is later than start
        LocalDateTime startTime = ExportLogsFunction.parseTime(start);
        LocalDateTime endTime = ExportLogsFunction.parseTime(end);
        if (startTime.isAfter(endTime)) {
          return ResultBuilder.createUserErrorResult("start-time has to be earlier than end-time.");
        }
      }

      // validate onlyLogs and onlyStats
      boolean onlyLogs = Boolean.parseBoolean(arguments.get("logs-only"));
      boolean onlyStats = Boolean.parseBoolean(arguments.get("stats-only"));
      if (onlyLogs && onlyStats) {
        return ResultBuilder.createUserErrorResult("logs-only and stats-only can't both be true");
      }

      return ResultBuilder.createInfoResult("");
    }

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult, Path tempFile) {
      // in the command over http case, the command result is in the downloaded temp file
      if (tempFile != null) {
        Path dirPath;
        String dirName = parseResult.getParamValueStrings().get("dir");
        if (StringUtils.isBlank(dirName)) {
          dirPath = Paths.get(System.getProperty("user.dir"));
        } else {
          dirPath = Paths.get(dirName);
        }
        String fileName = "exportedLogs_" + System.currentTimeMillis() + ".zip";
        File exportedLogFile = dirPath.resolve(fileName).toFile();
        try {
          FileUtils.copyFile(tempFile.toFile(), exportedLogFile);
          FileUtils.deleteQuietly(tempFile.toFile());
          commandResult = ResultBuilder
              .createInfoResult("Logs exported to: " + exportedLogFile.getAbsolutePath());
        } catch (IOException e) {
          logger.error(e.getMessage(), e);
          commandResult = ResultBuilder.createGemFireErrorResult(e.getMessage());
        }
      } else if (commandResult.getStatus() == Result.Status.OK) {
        commandResult = ResultBuilder.createInfoResult(
            "Logs exported to the connected member's file system: " + commandResult.nextLine());
      }
      return commandResult;
    }
  }

}
