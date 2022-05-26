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

import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
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

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.SessionContext;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link LogWriterAppender}.
 */
@Category(LoggingTest.class)
public class LogWriterAppenderShutdownIntegrationTest {

  private static final String CONFIG_FILE_NAME = "LogWriterAppenderIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "LOGWRITER";

  private static String configFilePath;

  private File logFile;

  private String logMessage;

  private Logger logger;

  private LogWriterAppender logWriterAppender;

  private SessionContext sessionContext;

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
  public void setUp() {
    String name = testName.getMethodName();
    logFile = new File(temporaryFolder.getRoot(), name + ".log");

    LogConfig config = mock(LogConfig.class);
    when(config.getName()).thenReturn(name);
    when(config.getLogFile()).thenReturn(logFile);

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(config);

    sessionContext = mock(SessionContext.class);
    when(sessionContext.getLogConfigSupplier()).thenReturn(logConfigSupplier);

    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();

    logWriterAppender =
        loggerContextRule.getAppender(APPENDER_NAME, LogWriterAppender.class);
  }

  @After
  public void tearDown() {
    logWriterAppender.stopSession();
    logWriterAppender.clearLogEvents();
  }

  @Test
  public void stopActiveSession() {
    logWriterAppender.clearLogEvents();

    logWriterAppender.createSession(sessionContext);
    logWriterAppender.startSession();
    assertThat(logWriterAppender.getLogWriter()).isNotNull();
    assertThat(logWriterAppender.isStarted()).isTrue();

    logger.info(logMessage);

    assertThat(logWriterAppender.getLogEvents()).hasSize(1);
    LogEvent event = logWriterAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(getClass().getName());
    assertThat(event.getLevel()).isEqualTo(Level.INFO);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);

    logWriterAppender.stopSession();
    assertThat(logWriterAppender.getLogWriter()).isNull();
    assertThat(logWriterAppender.isPaused()).isTrue();
    logWriterAppender.clearLogEvents();

    logger.info(logMessage);

    assertThat(logWriterAppender.getLogEvents()).hasSize(0);
  }
}
