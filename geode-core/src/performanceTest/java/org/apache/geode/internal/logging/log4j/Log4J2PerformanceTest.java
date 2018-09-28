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
package org.apache.geode.internal.logging.log4j;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.After;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LoggingPerformanceTestCase;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.PerformanceTest;

@Category({PerformanceTest.class, LoggingTest.class})
@Ignore("TODO: repackage as jmh benchmark")
public class Log4J2PerformanceTest extends LoggingPerformanceTestCase {

  private static final int DEFAULT_LOG_FILE_SIZE_LIMIT = Integer.MAX_VALUE;
  private static final int DEFAULT_LOG_FILE_COUNT_LIMIT = 20;

  private static final String SYS_LOG_FILE = "gemfire-log-file";
  private static final String SYS_LOG_FILE_SIZE_LIMIT = "gemfire-log-file-size-limit";
  private static final String SYS_LOG_FILE_COUNT_LIMIT = "gemfire-log-file-count-limit";

  private File config;

  @After
  public void tearDownLog4J2PerformanceTest() {
    config = null; // leave this file in place for now
  }

  private void setPropertySubstitutionValues(final String logFile, final int logFileSizeLimitMB,
      final int logFileCountLimit) {
    if (logFileSizeLimitMB < 0) {
      throw new IllegalArgumentException("logFileSizeLimitMB must be zero or positive integer");
    }
    if (logFileCountLimit < 0) {
      throw new IllegalArgumentException("logFileCountLimit must be zero or positive integer");
    }

    // flip \ to / if any exist
    final String logFileValue = logFile.replace("\\", "/");
    // append MB
    final String logFileSizeLimitMBValue =
        new StringBuilder(String.valueOf(logFileSizeLimitMB)).append(" MB").toString();
    final String logFileCountLimitValue =
        new StringBuilder(String.valueOf(logFileCountLimit)).toString();

    System.setProperty(SYS_LOG_FILE, logFileValue);
    System.setProperty(SYS_LOG_FILE_SIZE_LIMIT, logFileSizeLimitMBValue);
    System.setProperty(SYS_LOG_FILE_COUNT_LIMIT, logFileCountLimitValue);
  }

  Logger createLogger() throws IOException {
    // create configuration with log-file and log-level
    configDirectory = new File(getUniqueName());
    configDirectory.mkdir();
    assertTrue(configDirectory.isDirectory() && configDirectory.canWrite());

    // copy the log4j2-test.xml to the configDirectory
    // final URL srcURL =
    // getClass().getResource("/org/apache/geode/internal/logging/log4j/log4j2-test.xml");
    final URL srcURL = getClass().getResource("log4j2-test.xml");
    final File src = new File(srcURL.getFile());
    FileUtils.copyFileToDirectory(src, configDirectory);
    config = new File(configDirectory, "log4j2-test.xml");
    assertTrue(config.exists());

    logFile = new File(configDirectory, DistributionConfig.GEMFIRE_PREFIX + "log");
    final String logFilePath = IOUtils.tryGetCanonicalPathElseGetAbsolutePath(logFile);
    final String logFileName =
        logFilePath.substring(0, logFilePath.lastIndexOf(File.separatorChar));
    setPropertySubstitutionValues(logFileName, DEFAULT_LOG_FILE_SIZE_LIMIT,
        DEFAULT_LOG_FILE_COUNT_LIMIT);

    final String configPath =
        "file://" + IOUtils.tryGetCanonicalPathElseGetAbsolutePath(config);
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, configPath);

    final Logger logger = LogManager.getLogger();
    return logger;
  }

  @Override
  protected PerformanceLogger createPerformanceLogger() throws IOException {
    final Logger logger = createLogger();

    final PerformanceLogger perfLogger = new PerformanceLogger() {
      @Override
      public void log(String message) {
        logger.info(message);
      }

      @Override
      public boolean isEnabled() {
        return logger.isEnabled(Level.INFO);
      }
    };

    return perfLogger;
  }
}
