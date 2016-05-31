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
   
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterAppenders;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Provides an implementation of <code>DistributionConfig</code> that
 * is used at runtime by a {@link InternalDistributedSystem}. It allows
 * for dynamic reconfig of the app the owns it.
 * 
 * The attribute settor methods in this class all assume that they are
 * being called at runtime. If they are called from some other ConfigSource
 * then those calls should come through setAttributeObject and it will
 * set the attSourceMap to the correct source after these methods return.
 *
 *
 * @since GemFire 3.0
 */
public final class RuntimeDistributionConfigImpl
  extends DistributionConfigImpl  {

  private static final long serialVersionUID = -805637520096606113L;
  transient private final InternalDistributedSystem ds;

  //////////////////////  Constructors  //////////////////////

  /**
   * Create a new <code>RuntimeDistributionConfigImpl</code> from the
   * contents of another <code>DistributionConfig</code>.
   */
  public RuntimeDistributionConfigImpl(InternalDistributedSystem ds) {
    super(ds.getOriginalConfig());
    this.ds = ds;
    this.modifiable = false;
  }

  ////////////////////  Configuration Methods  ////////////////////
  @Override
  public void setLogLevel(int value) {
    this.logLevel = (Integer)checkAttribute(LOG_LEVEL, value);
    getAttSourceMap().put(LOG_LEVEL, ConfigSource.runtime());
    this.ds.getInternalLogWriter().setLogWriterLevel(value);
    LogWriterAppenders.configChanged(LogWriterAppenders.Identifier.MAIN);
  }
  
  @Override
  public void setStatisticSamplingEnabled(boolean value) {
    this.statisticSamplingEnabled = (Boolean)checkAttribute(STATISTIC_SAMPLING_ENABLED, value);
    getAttSourceMap().put(STATISTIC_SAMPLING_ENABLED, ConfigSource.runtime());
  }

  @Override
  public void setStatisticSampleRate(int value) {
    value = (Integer)checkAttribute(STATISTIC_SAMPLE_RATE, value);
    if (value < DEFAULT_STATISTIC_SAMPLE_RATE) {
      // fix 48228
      this.ds.getLogWriter().info("Setting statistic-sample-rate to " + DEFAULT_STATISTIC_SAMPLE_RATE + " instead of the requested " + value + " because VSD does not work with sub-second sampling.");
      value = DEFAULT_STATISTIC_SAMPLE_RATE;
    }
    this.statisticSampleRate = value;
  }

  @Override
  public void setStatisticArchiveFile(File value) {
    value = (File)checkAttribute(STATISTIC_ARCHIVE_FILE, value);
    if (value == null) {
      value = new File("");
    }
    try {
      this.ds.getStatSampler().changeArchive(value);
    } catch (GemFireIOException ex) {
      throw new IllegalArgumentException(ex.getMessage());
    }
    this.statisticArchiveFile = value;
    getAttSourceMap().put(STATISTIC_ARCHIVE_FILE, ConfigSource.runtime());
  }


  @Override
  public void setArchiveDiskSpaceLimit(int value) {
    this.archiveDiskSpaceLimit = (Integer)checkAttribute(ARCHIVE_DISK_SPACE_LIMIT, value);
    getAttSourceMap().put(ARCHIVE_DISK_SPACE_LIMIT, ConfigSource.runtime());
  }

  @Override
  public void setArchiveFileSizeLimit(int value) {
    this.archiveFileSizeLimit = (Integer)checkAttribute(ARCHIVE_FILE_SIZE_LIMIT, value);
    getAttSourceMap().put(ARCHIVE_FILE_SIZE_LIMIT, ConfigSource.runtime());
  }

  @Override
  public void setLogDiskSpaceLimit(int value) {
    this.logDiskSpaceLimit = (Integer)checkAttribute(LOG_DISK_SPACE_LIMIT, value);
    getAttSourceMap().put(LOG_DISK_SPACE_LIMIT, ConfigSource.runtime());
    LogWriterAppenders.configChanged(LogWriterAppenders.Identifier.MAIN);
  }

  @Override
  public void setLogFileSizeLimit(int value) {
    this.logFileSizeLimit = (Integer)checkAttribute(LOG_FILE_SIZE_LIMIT, value);
    getAttSourceMap().put(LOG_FILE_SIZE_LIMIT, ConfigSource.runtime());
    LogWriterAppenders.configChanged(LogWriterAppenders.Identifier.MAIN);
  }

  public DistributionConfig takeSnapshot() {
    return new DistributionConfigSnapshot(this);
  }

  public List<String> getModifiableAttributes(){
    String[] modifiables = {HTTP_SERVICE_PORT,JMX_MANAGER_HTTP_PORT, ARCHIVE_DISK_SPACE_LIMIT,
            ARCHIVE_FILE_SIZE_LIMIT, LOG_DISK_SPACE_LIMIT, LOG_FILE_SIZE_LIMIT,
            LOG_LEVEL, STATISTIC_ARCHIVE_FILE, STATISTIC_SAMPLE_RATE, STATISTIC_SAMPLING_ENABLED};
    return Arrays.asList(modifiables);
  };
}
