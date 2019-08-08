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
package org.apache.geode.logging.internal.spi;

import java.io.File;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogWriterImpl;

/**
 * Configuration for logging.
 */
public interface LogConfig {

  /**
   * Returns true if the {@code LogConfig} has a non-null and non-default
   * {@link ConfigurationProperties#SECURITY_LOG_FILE}.
   */
  static boolean hasSecurityLogFile(final LogConfig logConfig) {
    return logConfig.getSecurityLogFile() != null
        && !logConfig.getSecurityLogFile().equals(new File(""));
  }

  /**
   * Returns the value of the {@link ConfigurationProperties#LOG_LEVEL} property
   *
   * @see LogWriterImpl
   */
  int getLogLevel();

  /**
   * Returns the value of the {@link ConfigurationProperties#LOG_FILE} property
   *
   * @return {@code null} if logging information goes to standard out
   */
  File getLogFile();

  /**
   * Returns the value of the {@link ConfigurationProperties#SECURITY_LOG_FILE} property
   *
   * @return {@code null} if logging information goes to standard out
   */
  File getSecurityLogFile();

  /**
   * Get the current log-level for {@link ConfigurationProperties#SECURITY_LOG_LEVEL}.
   *
   * @return the current security log-level
   */
  int getSecurityLogLevel();

  /**
   * Returns the value of the {@link ConfigurationProperties#LOG_FILE_SIZE_LIMIT} property
   */
  int getLogFileSizeLimit();

  /**
   * Returns the value of the {@link ConfigurationProperties#LOG_DISK_SPACE_LIMIT} property
   */
  int getLogDiskSpaceLimit();

  /**
   * Returns the value of the {@link ConfigurationProperties#NAME} property Gets the member's name.
   * A name is optional and by default empty. If set it must be unique in the ds. When set its used
   * by tools to help identify the member.
   * <p>
   * The default value is: {@link DistributionConfig#DEFAULT_NAME}.
   *
   * @return the system's name.
   */
  String getName();

  /**
   * Returns string representation of {@code LogConfig} for logging the banner.
   */
  String toLoggerString();

  /**
   * Returns true if locators and mcast-port are not configured.
   */
  boolean isLoner();
}
