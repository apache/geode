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
package org.apache.geode.internal.logging;

import java.io.File;

import org.apache.geode.logging.internal.spi.LogConfig;

/**
 * Wraps a {@link LogConfig} and overrides configuration for Security.
 */
public class SecurityLogConfig implements LogConfig {

  private final LogConfig config;

  SecurityLogConfig(final LogConfig config) {
    this.config = config;
  }

  @Override
  public int getLogLevel() {
    return config.getSecurityLogLevel();
  }

  @Override
  public File getLogFile() {
    return config.getSecurityLogFile();
  }

  @Override
  public File getSecurityLogFile() {
    return config.getSecurityLogFile();
  }

  @Override
  public int getSecurityLogLevel() {
    return config.getSecurityLogLevel();
  }

  @Override
  public int getLogFileSizeLimit() {
    return config.getLogFileSizeLimit();
  }

  @Override
  public int getLogDiskSpaceLimit() {
    return config.getLogDiskSpaceLimit();
  }

  @Override
  public String getName() {
    return config.getName();
  }

  @Override
  public String toLoggerString() {
    return config.toLoggerString();
  }

  @Override
  public boolean isLoner() {
    return config.isLoner();
  }
}
