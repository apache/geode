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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireIOException;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogWriterAppenders;

/**
 * Provides an implementation of {@code DistributionConfig} that is used at runtime by an
 * {@link InternalDistributedSystem}. It allows for dynamic reconfiguration of the app that owns it.
 *
 * The attribute setter methods in this class all assume that they are being called at runtime. If
 * they are called from some other ConfigSource then those calls should come through
 * setAttributeObject and it will set the attSourceMap to the correct source after these methods
 * return.
 *
 * @since GemFire 3.0
 */
public class RuntimeDistributionConfigImpl extends DistributionConfigImpl {

  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = -805637520096606113L;

  private final transient InternalDistributedSystem system;

  /**
   * Create a new {@code RuntimeDistributionConfigImpl} from the contents of another
   * {@code DistributionConfig}.
   */
  public RuntimeDistributionConfigImpl(InternalDistributedSystem system) {
    super(system.getOriginalConfig());
    this.system = system;
    modifiable = false;
  }

  @Override
  public void setLogLevel(int value) {
    logLevel = (Integer) checkAttribute(LOG_LEVEL, value);
    getAttSourceMap().put(LOG_LEVEL, ConfigSource.runtime());
    system.getInternalLogWriter().setLogWriterLevel(value);
    LogWriterAppenders.configChanged(LogWriterAppenders.Identifier.MAIN);
  }

  @Override
  public void setStatisticSamplingEnabled(boolean newValue) {
    statisticSamplingEnabled = (Boolean) checkAttribute(STATISTIC_SAMPLING_ENABLED, newValue);
    getAttSourceMap().put(STATISTIC_SAMPLING_ENABLED, ConfigSource.runtime());
  }

  @Override
  public void setStatisticSampleRate(int value) {
    value = (Integer) checkAttribute(STATISTIC_SAMPLE_RATE, value);
    if (value < DEFAULT_STATISTIC_SAMPLE_RATE) {
      // fix 48228
      logger.info(
          "Setting statistic-sample-rate to {} instead of the requested {} because VSD does not work with sub-second sampling.",
          DEFAULT_STATISTIC_SAMPLE_RATE, value);
      value = DEFAULT_STATISTIC_SAMPLE_RATE;
    }
    statisticSampleRate = value;
  }

  @Override
  public void setStatisticArchiveFile(File value) {
    value = (File) checkAttribute(STATISTIC_ARCHIVE_FILE, value);
    if (value == null) {
      value = new File("");
    }
    try {
      system.getStatSampler().changeArchive(value);
    } catch (GemFireIOException ex) {
      throw new IllegalArgumentException(ex.getMessage());
    }
    statisticArchiveFile = value;
    getAttSourceMap().put(STATISTIC_ARCHIVE_FILE, ConfigSource.runtime());
  }

  @Override
  public void setArchiveDiskSpaceLimit(int value) {
    archiveDiskSpaceLimit = (Integer) checkAttribute(ARCHIVE_DISK_SPACE_LIMIT, value);
    getAttSourceMap().put(ARCHIVE_DISK_SPACE_LIMIT, ConfigSource.runtime());
  }

  @Override
  public void setArchiveFileSizeLimit(int value) {
    archiveFileSizeLimit = (Integer) checkAttribute(ARCHIVE_FILE_SIZE_LIMIT, value);
    getAttSourceMap().put(ARCHIVE_FILE_SIZE_LIMIT, ConfigSource.runtime());
  }

  @Override
  public void setLogDiskSpaceLimit(int value) {
    logDiskSpaceLimit = (Integer) checkAttribute(LOG_DISK_SPACE_LIMIT, value);
    getAttSourceMap().put(LOG_DISK_SPACE_LIMIT, ConfigSource.runtime());
    LogWriterAppenders.configChanged(LogWriterAppenders.Identifier.MAIN);
  }

  @Override
  public void setLogFileSizeLimit(int value) {
    logFileSizeLimit = (Integer) checkAttribute(LOG_FILE_SIZE_LIMIT, value);
    getAttSourceMap().put(LOG_FILE_SIZE_LIMIT, ConfigSource.runtime());
    LogWriterAppenders.configChanged(LogWriterAppenders.Identifier.MAIN);
  }

  @Override
  public List<String> getModifiableAttributes() {
    String[] modifiables = {HTTP_SERVICE_PORT, JMX_MANAGER_HTTP_PORT, ARCHIVE_DISK_SPACE_LIMIT,
        ARCHIVE_FILE_SIZE_LIMIT, LOG_DISK_SPACE_LIMIT, LOG_FILE_SIZE_LIMIT, LOG_LEVEL,
        STATISTIC_ARCHIVE_FILE, STATISTIC_SAMPLE_RATE, STATISTIC_SAMPLING_ENABLED};
    return Arrays.asList(modifiables);
  }

  public DistributionConfig takeSnapshot() {
    return new DistributionConfigSnapshot(this);
  }
}
