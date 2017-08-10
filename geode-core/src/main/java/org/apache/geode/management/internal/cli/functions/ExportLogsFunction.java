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
 *
 */
package org.apache.geode.management.internal.cli.functions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogLevel;
import org.apache.geode.management.internal.cli.commands.ExportLogsCommand;
import org.apache.geode.management.internal.cli.util.ExportLogsCacheWriter;
import org.apache.geode.management.internal.cli.util.LogExporter;
import org.apache.geode.management.internal.cli.util.LogFilter;
import org.apache.geode.management.internal.configuration.domain.Configuration;

/**
 * this function extracts the logs using a LogExporter which creates a zip file, and then writes the
 * zip file bytes into a replicated region, this in effect, "stream" the zip file bytes to the
 * locator
 *
 * The function only extracts .log and .gfs files under server's working directory
 */
public class ExportLogsFunction implements Function, InternalEntity {
  private static final Logger logger = LogService.getLogger();

  public static final String EXPORT_LOGS_REGION = "__exportLogsRegion";

  private static final long serialVersionUID = 1L;
  private static final int BUFFER_SIZE = 1024;

  @Override
  public void execute(final FunctionContext context) {
    try {
      InternalCache cache = (InternalCache) context.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

      String memberId = cache.getDistributedSystem().getMemberId();
      logger.info("ExportLogsFunction started for member {}", memberId);

      Region exportLogsRegion = createOrGetExistingExportLogsRegion(false, cache);

      Args args = (Args) context.getArguments();
      File baseLogFile = null;
      File baseStatsFile = null;

      if (args.isIncludeLogs() && !config.getLogFile().toString().isEmpty()) {
        baseLogFile = config.getLogFile().getAbsoluteFile();
      }
      if (args.isIncludeStats() && !config.getStatisticArchiveFile().toString().isEmpty()) {
        baseStatsFile = config.getStatisticArchiveFile().getAbsoluteFile();
      }

      LogFilter logFilter = new LogFilter(args.getLogLevel(), args.isThisLogLevelOnly(),
          args.getStartTime(), args.getEndTime());

      Path exportedZipFile = new LogExporter(logFilter, baseLogFile, baseStatsFile).export();

      // nothing to return back
      if (exportedZipFile == null) {
        context.getResultSender().lastResult(null);
        return;
      }

      logger.info("Streaming zipped file: " + exportedZipFile.toString());
      try (FileInputStream inputStream = new FileInputStream(exportedZipFile.toFile())) {
        byte[] buffer = new byte[BUFFER_SIZE];

        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) > 0) {
          if (bytesRead == BUFFER_SIZE) {
            exportLogsRegion.put(memberId, buffer);
          } else {
            exportLogsRegion.put(memberId, Arrays.copyOfRange(buffer, 0, bytesRead));
          }
        }
      }

      context.getResultSender().lastResult(null);

    } catch (Exception e) {
      e.printStackTrace();
      logger.error(e);
      context.getResultSender().sendException(e);
    }
  }

  public static Region createOrGetExistingExportLogsRegion(boolean isInitiatingMember,
      InternalCache cache) throws IOException, ClassNotFoundException {

    Region exportLogsRegion = cache.getRegion(EXPORT_LOGS_REGION);
    if (exportLogsRegion == null) {
      AttributesFactory<String, Configuration> regionAttrsFactory = new AttributesFactory<>();
      regionAttrsFactory.setDataPolicy(DataPolicy.EMPTY);
      regionAttrsFactory.setScope(Scope.DISTRIBUTED_ACK);

      if (isInitiatingMember) {
        regionAttrsFactory.setCacheWriter(new ExportLogsCacheWriter());
      }
      InternalRegionArguments internalArgs = new InternalRegionArguments();
      internalArgs.setIsUsedForMetaRegion(true);
      exportLogsRegion =
          cache.createVMRegion(EXPORT_LOGS_REGION, regionAttrsFactory.create(), internalArgs);
    }

    return exportLogsRegion;
  }

  public static void destroyExportLogsRegion(InternalCache cache) {
    Region exportLogsRegion = cache.getRegion(EXPORT_LOGS_REGION);
    if (exportLogsRegion == null) {
      return;
    }
    exportLogsRegion.destroyRegion();
  }

  @Override
  public boolean isHA() {
    return false;
  }

  public static class Args implements Serializable {
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Level logLevel;
    private boolean thisLogLevelOnly;
    private boolean includeLogs;
    private boolean includeStats;

    public Args(String startTime, String endTime, String logLevel, boolean logLevelOnly,
        boolean logsOnly, boolean statsOnly) {
      this.startTime = parseTime(startTime);
      this.endTime = parseTime(endTime);

      if (StringUtils.isBlank(logLevel)) {
        this.logLevel = LogLevel.getLevel(ExportLogsCommand.DEFAULT_EXPORT_LOG_LEVEL);
      } else {
        this.logLevel = LogLevel.getLevel(logLevel);
      }
      this.thisLogLevelOnly = logLevelOnly;

      this.includeLogs = !statsOnly;
      this.includeStats = !logsOnly;
    }

    public LocalDateTime getStartTime() {
      return startTime;
    }

    public LocalDateTime getEndTime() {
      return endTime;
    }

    public Level getLogLevel() {
      return logLevel;
    }

    public boolean isThisLogLevelOnly() {
      return thisLogLevelOnly;
    }

    public boolean isIncludeLogs() {
      return includeLogs;
    }

    public boolean isIncludeStats() {
      return includeStats;
    }
  }

  public static LocalDateTime parseTime(String dateString) {
    if (dateString == null) {
      return null;
    }

    try {
      SimpleDateFormat df = new SimpleDateFormat(ExportLogsCommand.FORMAT);
      return df.parse(dateString).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    } catch (ParseException e) {
      try {
        SimpleDateFormat df = new SimpleDateFormat(ExportLogsCommand.ONLY_DATE_FORMAT);
        return df.parse(dateString).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
      } catch (ParseException e1) {
        return null;
      }
    }
  }
}
