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
package org.apache.geode.internal.statistics;

import java.io.File;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * SimpleStatSampler is a functional implementation of HostStatSampler that samples statistics
 * stored in local java memory and does not require any native code or additional GemFire features.
 * <p>
 * The StatisticsManager may be implemented by LocalStatisticsFactory and does not require a GemFire
 * connection.
 *
 */
public class SimpleStatSampler extends HostStatSampler {

  private static final Logger logger = LogService.getLogger();

  public static final String ARCHIVE_FILE_NAME_PROPERTY = "stats.archive-file";
  public static final String FILE_SIZE_LIMIT_PROPERTY = "stats.file-size-limit";
  public static final String DISK_SPACE_LIMIT_PROPERTY = "stats.disk-space-limit";
  public static final String SAMPLE_RATE_PROPERTY = "stats.sample-rate";

  public static final String DEFAULT_ARCHIVE_FILE_NAME = "stats.gfs";
  public static final long DEFAULT_FILE_SIZE_LIMIT = 0;
  public static final long DEFAULT_DISK_SPACE_LIMIT = 0;
  public static final int DEFAULT_SAMPLE_RATE = 1000;

  private final File archiveFileName =
      new File(System.getProperty(ARCHIVE_FILE_NAME_PROPERTY, DEFAULT_ARCHIVE_FILE_NAME));
  private final long archiveFileSizeLimit =
      Long.getLong(FILE_SIZE_LIMIT_PROPERTY, DEFAULT_FILE_SIZE_LIMIT).longValue() * (1024 * 1024);
  private final long archiveDiskSpaceLimit =
      Long.getLong(DISK_SPACE_LIMIT_PROPERTY, DEFAULT_DISK_SPACE_LIMIT).longValue() * (1024 * 1024);
  private final int sampleRate =
      Integer.getInteger(SAMPLE_RATE_PROPERTY, DEFAULT_SAMPLE_RATE).intValue();

  private final StatisticsManager sm;

  public SimpleStatSampler(CancelCriterion stopper, StatisticsManager sm) {
    this(stopper, sm, new NanoTimer());
  }

  public SimpleStatSampler(CancelCriterion stopper, StatisticsManager sm, NanoTimer timer) {
    super(stopper, new StatSamplerStats(sm, sm.getId()), timer);
    this.sm = sm;
    logger.info(LogMarker.STATISTICS_MARKER, LocalizedMessage
        .create(LocalizedStrings.SimpleStatSampler_STATSSAMPLERATE_0, getSampleRate()));
  }

  @Override
  protected void checkListeners() {
    // do nothing
  }

  @Override
  public File getArchiveFileName() {
    return this.archiveFileName;
  }

  @Override
  public long getArchiveFileSizeLimit() {
    if (fileSizeLimitInKB()) {
      return this.archiveFileSizeLimit / 1024;
    } else {
      return this.archiveFileSizeLimit;
    }
  }

  @Override
  public long getArchiveDiskSpaceLimit() {
    if (fileSizeLimitInKB()) {
      return this.archiveDiskSpaceLimit / 1024;
    } else {
      return this.archiveDiskSpaceLimit;
    }
  }

  @Override
  public String getProductDescription() {
    return "Unknown product";
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return this.sm;
  }

  @Override
  protected int getSampleRate() {
    return this.sampleRate;
  }

  @Override
  public boolean isSamplingEnabled() {
    return true;
  }
}
