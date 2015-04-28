package com.gemstone.gemfire.internal.logging;

import java.io.File;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * LogConfig implementation for Security logging configuration that delegates 
 * to a DistributionConfig.
 * 
 * @author Kirk Lund
 */
public class SecurityLogConfig implements LogConfig {

  private final DistributionConfig config;

  public SecurityLogConfig(final DistributionConfig config) {
    this.config = config;
  }

  @Override
  public int getLogLevel() {
    return this.config.getSecurityLogLevel();
  }

  @Override
  public File getLogFile() {
    return this.config.getSecurityLogFile();
  }

  @Override
  public int getLogFileSizeLimit() {
    return this.config.getLogFileSizeLimit();
  }

  @Override
  public int getLogDiskSpaceLimit() {
    return this.config.getLogDiskSpaceLimit();
  }

  @Override
  public String toLoggerString() {
    return this.config.toLoggerString();
  }

  @Override
  public String getName() {
    return this.config.getName();
  }
}
