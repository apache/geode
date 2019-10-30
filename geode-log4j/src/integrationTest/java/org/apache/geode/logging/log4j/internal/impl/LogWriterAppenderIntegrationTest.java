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

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.commons.io.FileUtils.readLines;
import static org.apache.geode.logging.log4j.internal.impl.NonBlankStrings.nonBlankStrings;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.FileUtils;
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

import org.apache.geode.logging.internal.LogMessageRegex;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.SessionContext;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link LogWriterAppender}.
 */
@Category(LoggingTest.class)
public class LogWriterAppenderIntegrationTest {

  private static final String CONFIG_FILE_NAME = "LogWriterAppenderIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "LOGWRITER";

  private static String configFilePath;

  private File logFile;
  private Logger logger;
  private String logMessage;
  private LogWriterAppender logWriterAppender;

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

    SessionContext sessionContext = mock(SessionContext.class);
    when(sessionContext.getLogConfigSupplier()).thenReturn(logConfigSupplier);

    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();

    logWriterAppender =
        loggerContextRule.getAppender(APPENDER_NAME, LogWriterAppender.class);
    logWriterAppender.createSession(sessionContext);
    logWriterAppender.startSession();
  }

  @After
  public void tearDown() {
    logWriterAppender.stopSession();
    logWriterAppender.clearLogEvents();
  }

  @Test
  public void getLogEventsReturnsLoggedEvents() {
    logWriterAppender.clearLogEvents();

    logger.info(logMessage);

    assertThat(logWriterAppender.getLogEvents()).hasSize(1);
    LogEvent event = logWriterAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(getClass().getName());
    assertThat(event.getLevel()).isEqualTo(Level.INFO);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);
  }

  @Test
  public void pausedDoesNotLog() {
    logWriterAppender.pause();
    logWriterAppender.clearLogEvents();

    logger.info(logMessage);

    assertThat(logWriterAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void resumeAfterPausedLogs() {
    logWriterAppender.pause();
    logWriterAppender.clearLogEvents();
    logWriterAppender.resume();

    logger.info(logMessage);

    assertThat(logWriterAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void resumeWithoutPauseStillLogs() {
    logWriterAppender.clearLogEvents();
    logWriterAppender.resume();

    logger.info(logMessage);

    assertThat(logWriterAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void isPausedReturnsTrueAfterPause() {
    logWriterAppender.pause();
    assertThat(logWriterAppender.isPaused()).isTrue();
  }

  @Test
  public void isPausedReturnsFalseAfterResume() {
    logWriterAppender.pause();
    logWriterAppender.resume();

    assertThat(logWriterAppender.isPaused()).isFalse();
  }

  @Test
  public void resumeWithoutPauseDoesNothing() {
    logWriterAppender.resume();

    assertThat(logWriterAppender.isPaused()).isFalse();
  }

  @Test
  public void isPausedReturnsFalseByDefault() {
    assertThat(logWriterAppender.isPaused()).isFalse();
  }

  @Test
  public void logsToFile() throws Exception {
    logger.info(logMessage);

    assertThat(logFile).exists();
    String content = FileUtils.readFileToString(logFile, defaultCharset()).trim();
    assertThat(content).contains(logMessage);
  }

  @Test
  public void linesInFileShouldMatchPatternLayout() throws Exception {
    logger.info(logMessage);

    assertThat(logFile).exists();

    List<String> lines = nonBlankStrings(readLines(logFile, defaultCharset()));
    assertThat(lines).hasSize(1);

    for (String line : lines) {
      assertThat(line).matches(LogMessageRegex.getRegex());
    }
  }
}
