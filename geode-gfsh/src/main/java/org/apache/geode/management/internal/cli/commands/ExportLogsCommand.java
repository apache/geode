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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.ExportLogsFunction;
import org.apache.geode.management.internal.cli.functions.SizeExportLogsFunction;
import org.apache.geode.management.internal.cli.result.model.FileResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.ExportLogsCacheWriter;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ExportLogsCommand extends GfshCommand {

  private static final Logger logger = LogService.getLogger();

  public static final String FORMAT = "yyyy/MM/dd/HH/mm/ss/SSS/z";
  public static final String ONLY_DATE_FORMAT = "yyyy/MM/dd";

  public static final String DEFAULT_EXPORT_LOG_LEVEL = "ALL";

  private static final Pattern DISK_SPACE_LIMIT_PATTERN = Pattern.compile("(\\d+)([kmgtKMGT]?)");

  @CliCommand(value = CliStrings.EXPORT_LOGS, help = CliStrings.EXPORT_LOGS__HELP)
  @CliMetaData(isFileDownloadOverHttp = true,
      interceptor = "org.apache.geode.management.internal.cli.commands.ExportLogsInterceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.READ)
  public ResultModel exportLogs(
      @CliOption(key = CliStrings.EXPORT_LOGS__DIR,
          help = CliStrings.EXPORT_LOGS__DIR__HELP) String dirName,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.EXPORT_LOGS__GROUP__HELP) String[] groups,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
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
          help = CliStrings.EXPORT_LOGS__STARTTIME__HELP) String start,
      @CliOption(key = CliStrings.EXPORT_LOGS__ENDTIME,
          help = CliStrings.EXPORT_LOGS__ENDTIME__HELP) String end,
      @CliOption(key = CliStrings.EXPORT_LOGS__LOGSONLY, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.EXPORT_LOGS__LOGSONLY__HELP) boolean logsOnly,
      @CliOption(key = CliStrings.EXPORT_LOGS__STATSONLY, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.EXPORT_LOGS__STATSONLY__HELP) boolean statsOnly,
      @CliOption(key = CliStrings.EXPORT_LOGS__FILESIZELIMIT,
          unspecifiedDefaultValue = CliStrings.EXPORT_LOGS__FILESIZELIMIT__UNSPECIFIED_DEFAULT,
          specifiedDefaultValue = CliStrings.EXPORT_LOGS__FILESIZELIMIT__SPECIFIED_DEFAULT,
          help = CliStrings.EXPORT_LOGS__FILESIZELIMIT__HELP) String fileSizeLimit)
      throws Exception {

    long totalEstimatedExportSize = 0;
    InternalCache cache = (InternalCache) getCache();
    try {
      Set<DistributedMember> targetMembers = getMembersIncludingLocators(groups, memberIds);

      long userSpecifiedLimit = parseFileSizeLimit(fileSizeLimit);
      if (userSpecifiedLimit > 0) {
        // Get estimated size of exported logs from all servers before exporting anything
        for (DistributedMember server : targetMembers) {
          SizeExportLogsFunction.Args args = new SizeExportLogsFunction.Args(start, end, logLevel,
              onlyLogLevel, logsOnly, statsOnly);

          List<Object> results = estimateLogSize(args, server).getResult();
          if (!results.isEmpty()) {
            if (results.get(0) instanceof Long) {
              long estimatedSize = (Long) results.get(0);
              logger.info("Received estimated export size from member {}: {}", server.getId(),
                  estimatedSize);
              totalEstimatedExportSize += estimatedSize;
            } else if (results.get(0) instanceof ManagementException) {
              ManagementException exception = (ManagementException) results.get(0);
              return ResultModel.createError(exception.getMessage());
            }
          }
        }

        // first check if totalEstimate file size exceeds available disk space on locator
        if (totalEstimatedExportSize > getLocalDiskAvailable()) {
          return ResultModel.createError(
              "Estimated logs size will exceed the available disk space on the locator.");
        }
        // then check if total estimated file size exceeds user specified value
        if (totalEstimatedExportSize > userSpecifiedLimit) {
          String sb = "Estimated exported logs expanded file size = " +
              totalEstimatedExportSize + ", " +
              CliStrings.EXPORT_LOGS__FILESIZELIMIT + " = " +
              userSpecifiedLimit +
              ". To disable exported logs file size check use option \"--file-size-limit=0\".";
          return ResultModel.createError(sb);
        }
      }

      // get zipped files from all servers next
      Map<String, Path> zipFilesFromMembers = new HashMap<>();
      for (DistributedMember server : targetMembers) {
        Region<String, byte[]> region =
            ExportLogsFunction.createOrGetExistingExportLogsRegion(true, cache);

        ExportLogsCacheWriter cacheWriter =
            (ExportLogsCacheWriter) region.getAttributes().getCacheWriter();

        cacheWriter.startFile(server.getName());

        executeFunction(new ExportLogsFunction(),
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
        return ResultModel.createError("No files to be exported.");
      }

      Path tempDir = Files.createTempDirectory("exportedLogs");
      // make sure the directory is created, so that even if there is no files unzipped to this
      // dir, we can still zip it and send an empty zip file back to the client
      Path exportedLogsDir = tempDir.resolve("exportedLogs");
      FileUtils.forceMkdir(exportedLogsDir.toFile());

      for (Path zipFile : zipFilesFromMembers.values()) {
        Path unzippedMemberDir =
            exportedLogsDir.resolve(zipFile.getFileName().toString().replace(".zip", ""));
        ZipUtils.unzip(zipFile.toAbsolutePath().toString(), unzippedMemberDir.toString());
        FileUtils.deleteQuietly(zipFile.toFile());
      }

      Path dirPath;
      if (StringUtils.isBlank(dirName)) {
        dirPath = Paths.get(System.getProperty("user.dir"));
      } else {
        dirPath = Paths.get(dirName);
      }
      Path exportedLogsZipFile =
          dirPath.resolve("exportedLogs_" + System.currentTimeMillis() + ".zip").toAbsolutePath();

      logger.info("Zipping into: " + exportedLogsZipFile.toString());
      ZipUtils.zipDirectory(exportedLogsDir, exportedLogsZipFile);
      FileUtils.deleteDirectory(tempDir.toFile());

      ResultModel result = new ResultModel();
      result.addFile(exportedLogsZipFile.toFile(), FileResultModel.FILE_TYPE_FILE);
      return result;
    } finally {
      ExportLogsFunction.destroyExportLogsRegion(cache);
    }
  }

  /**
   * Wrapper to enable stubbing of static method call for unit testing
   */
  @SuppressWarnings("unchecked")
  ResultCollector<Object, List<Object>> estimateLogSize(SizeExportLogsFunction.Args args,
      DistributedMember member) {
    return (ResultCollector<Object, List<Object>>) executeFunction(new SizeExportLogsFunction(),
        args, member);
  }

  /**
   * Wrapper to enable stubbing of static method call for unit testing
   */
  long getLocalDiskAvailable() {
    return FileUtils.getUserDirectory().getUsableSpace();
  }

  /**
   * Returns file size limit in bytes
   */
  long parseFileSizeLimit(String fileSizeLimit) {
    if (StringUtils.isEmpty(fileSizeLimit)) {
      return 0;
    }

    long sizeLimit = parseSize(fileSizeLimit);
    long byteMultiplier = parseByteMultiplier(fileSizeLimit);

    return sizeLimit * byteMultiplier;
  }

  static int parseSize(String diskSpaceLimit) {
    Matcher matcher = DISK_SPACE_LIMIT_PATTERN.matcher(diskSpaceLimit);
    if (matcher.matches()) {
      return Integer.parseInt(matcher.group(1));
    } else {
      throw new IllegalArgumentException();
    }
  }

  static long parseByteMultiplier(String diskSpaceLimit) {
    Matcher matcher = DISK_SPACE_LIMIT_PATTERN.matcher(diskSpaceLimit);
    if (!matcher.matches()) {
      throw new IllegalArgumentException();
    }
    switch (matcher.group(2).toLowerCase()) {
      case "k":
        return KILOBYTE;
      case "t":
        return TERABYTE;
      case "g":
        return GIGABYTE;
      case "m":
      default:
        return MEGABYTE;
    }
  }

  static final long KILOBYTE = 1024L;
  static final long MEGABYTE = KILOBYTE * 1024;
  static final long GIGABYTE = MEGABYTE * 1024;
  static final long TERABYTE = GIGABYTE * 1024;
}
