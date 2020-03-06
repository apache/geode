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
package org.apache.geode.management.internal.cli.functions;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.internal.cli.util.BytesToString;
import org.apache.geode.management.internal.cli.util.LogExporter;
import org.apache.geode.management.internal.cli.util.LogFilter;

public class SizeExportLogsFunction extends ExportLogsFunction
    implements InternalFunction<ExportLogsFunction.Args> {
  private static final Logger LOGGER = LogService.getLogger();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(final FunctionContext<Args> context) {
    try {
      InternalCache cache = (InternalCache) context.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();
      Args args = context.getArguments();
      long diskAvailable = getDiskAvailable(config);
      long estimatedSize = estimateLogFileSize(cache.getMyId(), config.getLogFile(),
          config.getStatisticArchiveFile(), args);

      BytesToString bytesToString = new BytesToString();
      if (estimatedSize == 0 || estimatedSize < diskAvailable) {
        context.getResultSender().lastResult(estimatedSize);
      } else {
        String sb = "Estimated disk space required (" +
            bytesToString.of(estimatedSize) + ") to consolidate logs on member " +
            cache.getName() + " will exceed available disk space (" +
            bytesToString.of(diskAvailable) + ")";
        context.getResultSender().sendException(new ManagementException(sb)); // FileTooBigException
      }

    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error(e.getMessage());
      context.getResultSender().sendException(e);
    }
  }

  long getDiskAvailable(DistributionConfig config) {
    return config.getLogFile().getUsableSpace();
  }

  long estimateLogFileSize(final DistributedMember member, final File logFile,
      final File statArchive, final Args args) throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("SizeExportLogsFunction started for member {}", member);
    }

    File baseLogFile = null;
    File baseStatsFile = null;

    if (args.isIncludeLogs() && !logFile.toString().isEmpty()) {
      baseLogFile = logFile.getAbsoluteFile();
    }
    if (args.isIncludeStats() && !statArchive.toString().isEmpty()) {
      baseStatsFile = statArchive.getAbsoluteFile();
    }

    LogFilter logFilter = new LogFilter(args.getLogLevel(), args.isThisLogLevelOnly(),
        args.getStartTime(), args.getEndTime());

    long estimatedSize =
        new LogExporter(logFilter, baseLogFile, baseStatsFile).estimateFilteredSize();

    LOGGER.info("Estimated log file size: " + estimatedSize);

    return estimatedSize;
  }
}
