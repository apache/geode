/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
package com.gemstone.gemfire.distributed.internal;

import java.io.File;

import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterAppenders;

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
 * @author Darrel Schneider
 *
 * @since 3.0
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
    checkLogLevel(value);
    this.logLevel = value;
    getAttSourceMap().put(LOG_LEVEL_NAME, ConfigSource.runtime());
    this.ds.getInternalLogWriter().setLogWriterLevel(value);
    LogWriterAppenders.configChanged(LogWriterAppenders.Identifier.MAIN);
  }
  @Override
  public boolean isLogLevelModifiable() {
    return true;
  }
  
  @Override
  public void setStatisticSamplingEnabled(boolean value) {
    checkStatisticSamplingEnabled(value);
    this.statisticSamplingEnabled = value;
    getAttSourceMap().put(STATISTIC_SAMPLING_ENABLED_NAME, ConfigSource.runtime());
  }
  @Override
  public boolean isStatisticSamplingEnabledModifiable() {
    return true;
  }
  @Override
  public void setStatisticSampleRate(int value) {
    checkStatisticSampleRate(value);
    if (value < DEFAULT_STATISTIC_SAMPLE_RATE) {
      // fix 48228
      this.ds.getLogWriter().info("Setting statistic-sample-rate to " + DEFAULT_STATISTIC_SAMPLE_RATE + " instead of the requested " + value + " because VSD does not work with sub-second sampling.");
      value = DEFAULT_STATISTIC_SAMPLE_RATE;
    }
    this.statisticSampleRate = value;
  }
  @Override
  public boolean isStatisticSampleRateModifiable() {
    return true;
  }
  @Override
  public void setStatisticArchiveFile(File value) {
    checkStatisticArchiveFile(value);
    if (value == null) {
      value = new File("");
    }
    try {
      this.ds.getStatSampler().changeArchive(value);
    } catch (GemFireIOException ex) {
      throw new IllegalArgumentException(ex.getMessage());
    }
    this.statisticArchiveFile = value;
    getAttSourceMap().put(STATISTIC_ARCHIVE_FILE_NAME, ConfigSource.runtime());
  }
  @Override
  public boolean isStatisticArchiveFileModifiable() {
    return true;
  }

  @Override
  public void setArchiveDiskSpaceLimit(int value) {
    checkArchiveDiskSpaceLimit(value);
    this.archiveDiskSpaceLimit = value;
    getAttSourceMap().put(ARCHIVE_DISK_SPACE_LIMIT_NAME, ConfigSource.runtime());
  }
  @Override
  public boolean isArchiveDiskSpaceLimitModifiable() {
    return true;
  }
  @Override
  public void setArchiveFileSizeLimit(int value) {
    checkArchiveFileSizeLimit(value);
    this.archiveFileSizeLimit = value;
    getAttSourceMap().put(ARCHIVE_FILE_SIZE_LIMIT_NAME, ConfigSource.runtime());
  }
  @Override
  public boolean isArchiveFileSizeLimitModifiable() {
    return true;
  }
  @Override
  public void setLogDiskSpaceLimit(int value) {
    checkLogDiskSpaceLimit(value);
    this.logDiskSpaceLimit = value;
    getAttSourceMap().put(LOG_DISK_SPACE_LIMIT_NAME, ConfigSource.runtime());
    LogWriterAppenders.configChanged(LogWriterAppenders.Identifier.MAIN);
  }
  @Override
  public boolean isLogDiskSpaceLimitModifiable() {
    return true;
  }
  @Override
  public void setLogFileSizeLimit(int value) {
    checkLogFileSizeLimit(value);
    this.logFileSizeLimit = value;
    getAttSourceMap().put(this.LOG_FILE_SIZE_LIMIT_NAME, ConfigSource.runtime());
    LogWriterAppenders.configChanged(LogWriterAppenders.Identifier.MAIN);
  }
  @Override
  public boolean isLogFileSizeLimitModifiable() {
    return true;
  }

  public DistributionConfig takeSnapshot() {
    return new DistributionConfigSnapshot(this);
  }
}
