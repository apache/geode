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
package org.apache.geode.logging.log4j.internal.impl;

import static org.apache.geode.logging.internal.spi.LogWriterLevel.CONFIG;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;

import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.SessionContext;
import org.apache.geode.internal.statistics.StatisticsConfig;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link LogWriterAppender} with
 * {@link ConfigurationProperties#LOG_FILE_SIZE_LIMIT} and
 * {@link ConfigurationProperties#LOG_DISK_SPACE_LIMIT}.
 */
@Category(LoggingTest.class)
public class LogWriterAppenderWithLimitsIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "LogWriterAppenderWithLimitsIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "LOGWRITER";
  private static final int MAX_LOG_DISK_SPACE_LIMIT = 1_000_000;
  private static final int MAX_LOG_FILE_SIZE_LIMIT = 1_000_000;

  private static String configFilePath;

  private LogWriterAppender logWriterAppender;
  private LogConfig config;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpLogConfigFile() {
    URL resource = getResource(CONFIG_FILE_NAME);
    configFilePath = createFileFromResource(resource, temporaryFolder.getRoot(), CONFIG_FILE_NAME)
        .getAbsolutePath();
  }

  @Before
  public void setUp() throws Exception {
    logWriterAppender = loggerContextRule.getAppender(APPENDER_NAME,
        LogWriterAppender.class);

    String name = testName.getMethodName();
    File logFile = temporaryFolder.newFile(name + ".log");

    config = mock(LogConfig.class);
    when(config.getName()).thenReturn(name);
    when(config.getLogFile()).thenReturn(logFile);
    when(config.getLogLevel()).thenReturn(CONFIG.intLevel());
    when(config.getLogDiskSpaceLimit()).thenReturn(MAX_LOG_DISK_SPACE_LIMIT);
    when(config.getLogFileSizeLimit()).thenReturn(MAX_LOG_FILE_SIZE_LIMIT);

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(config);
    when(logConfigSupplier.getStatisticsConfig()).thenReturn(mock(StatisticsConfig.class));

    SessionContext sessionContext = mock(SessionContext.class);
    when(sessionContext.getLogConfigSupplier()).thenReturn(logConfigSupplier);

    logWriterAppender.createSession(sessionContext);
  }

  @After
  public void tearDown() {
    logWriterAppender.clearLogEvents();
  }

  @Test
  public void logDiskSpaceLimit() {
    // log-disk-space-limit starts out as MAX_LOG_DISK_SPACE_LIMIT
    LogConfig logConfig = logWriterAppender.getLogWriter().getConfig();
    assertThat(logConfig.getLogDiskSpaceLimit()).isEqualTo(MAX_LOG_DISK_SPACE_LIMIT);

    // log-disk-space-limit changes to 1_000
    when(config.getLogDiskSpaceLimit()).thenReturn(1_000);
    logWriterAppender.configChanged();

    assertThat(logConfig.getLogDiskSpaceLimit()).isEqualTo(1_000);

    // log-disk-space-limit changes back to MAX_LOG_DISK_SPACE_LIMIT
    when(config.getLogDiskSpaceLimit()).thenReturn(MAX_LOG_DISK_SPACE_LIMIT);
    logWriterAppender.configChanged();

    assertThat(logConfig.getLogDiskSpaceLimit()).isEqualTo(MAX_LOG_DISK_SPACE_LIMIT);
  }

  @Test
  public void logFileSizeLimit() {
    // log-file-size-limit starts out as MAX_LOG_FILE_SIZE_LIMIT
    LogConfig logConfig = logWriterAppender.getLogWriter().getConfig();
    assertThat(logConfig.getLogFileSizeLimit()).isEqualTo(MAX_LOG_FILE_SIZE_LIMIT);

    // log-file-size-limit changes to 1_000
    when(config.getLogFileSizeLimit()).thenReturn(1_000);
    logWriterAppender.configChanged();

    assertThat(logConfig.getLogFileSizeLimit()).isEqualTo(1_000);

    // log-disk-space-limit changes back to MAX_LOG_FILE_SIZE_LIMIT
    when(config.getLogFileSizeLimit()).thenReturn(MAX_LOG_FILE_SIZE_LIMIT);
    logWriterAppender.configChanged();

    assertThat(logConfig.getLogFileSizeLimit()).isEqualTo(MAX_LOG_FILE_SIZE_LIMIT);
  }
}
