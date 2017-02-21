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

import static java.util.stream.Collectors.toSet;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.InternalEntity;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.management.internal.cli.commands.MiscellaneousCommands;
import org.apache.geode.management.internal.cli.util.ExportLogsCacheWriter;
import org.apache.geode.management.internal.cli.util.LogExporter;
import org.apache.geode.management.internal.cli.util.LogFilter;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

public class ExportLogsFunction implements Function, InternalEntity {
  public static final String EXPORT_LOGS_REGION = "__exportLogsRegion";
  private static final Logger LOGGER = LogService.getLogger();
  private static final long serialVersionUID = 1L;
  private static final int BUFFER_SIZE = 1024;


  @Override
  public void execute(final FunctionContext context) {
    try {
      // TODO: change this to get cache from FunctionContext when it becomes available
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

      String memberId = cache.getDistributedSystem().getMemberId();
      LOGGER.info("ExportLogsFunction started for member {}", memberId);

      Region exportLogsRegion = createOrGetExistingExportLogsRegion(false);

      Args args = (Args) context.getArguments();
      LogFilter logFilter =
          new LogFilter(args.getPermittedLogLevels(), args.getStartTime(), args.getEndTime());
      Path workingDir = Paths.get(System.getProperty("user.dir"));

      Path exportedZipFile = new LogExporter(logFilter).export(workingDir);

      LOGGER.info("Streaming zipped file: " + exportedZipFile.toString());
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
      LOGGER.error(e);
      context.getResultSender().sendException(e);
    }
  }

  public static Region createOrGetExistingExportLogsRegion(boolean isInitiatingMember)
      throws IOException, ClassNotFoundException {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

    Region exportLogsRegion = cache.getRegion(EXPORT_LOGS_REGION);
    if (exportLogsRegion == null) {
      AttributesFactory<String, Configuration> regionAttrsFactory =
          new AttributesFactory<String, Configuration>();
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

  public static void destroyExportLogsRegion() {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

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
    private String startTime;
    private String endTime;
    private String logLevel;
    private boolean logLevelOnly;

    public Args(String startTime, String endTime, String logLevel, boolean logLevelOnly) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.logLevel = logLevel;
      this.logLevelOnly = logLevelOnly;
    }

    public LocalDateTime getStartTime() {
      return parseTime(startTime);
    }

    public LocalDateTime getEndTime() {
      return parseTime(endTime);
    }

    public Set<String> getPermittedLogLevels() {
      if (logLevel == null || StringUtils.isBlank(logLevel)) {
        return LogFilter.allLogLevels();
      }

      if (logLevelOnly) {
        return Stream.of(logLevel).collect(toSet());
      }

      // Return all log levels lower than or equal to the specified logLevel
      return Arrays.stream(InternalLogWriter.levelNames).filter((String level) -> {
        int logLevelCode = LogWriterImpl.levelNameToCode(level);
        int logLevelCodeThreshold = LogWriterImpl.levelNameToCode(logLevel);

        return logLevelCode >= logLevelCodeThreshold;
      }).collect(toSet());
    }

    private static LocalDateTime parseTime(String dateString) {
      if (dateString == null) {
        return null;
      }

      try {
        SimpleDateFormat df = new SimpleDateFormat(MiscellaneousCommands.FORMAT);
        return df.parse(dateString).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
      } catch (ParseException e) {
        try {
          SimpleDateFormat df = new SimpleDateFormat(MiscellaneousCommands.ONLY_DATE_FORMAT);
          return df.parse(dateString).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        } catch (ParseException e1) {
          return null;
        }
      }
    }
  }
}
