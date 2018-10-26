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

import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
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

import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogConfigSupplier;
import org.apache.geode.internal.logging.LogMessageRegex;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link PausableLogWriterAppender}.
 */
@Category(LoggingTest.class)
public class PausableLogWriterAppenderIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "PausableLogWriterAppenderIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "LOGWRITER";

  private static String configFilePath;

  private PausableLogWriterAppender pausableLogWriterAppender;
  private File logFile;
  private Logger logger;
  private String logMessage;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpLogConfigFile() throws Exception {
    URL resource = getResource(CONFIG_FILE_NAME);
    configFilePath = createFileFromResource(resource, temporaryFolder.getRoot(), CONFIG_FILE_NAME)
        .getAbsolutePath();
  }

  @Before
  public void setUp() throws Exception {
    pausableLogWriterAppender =
        loggerContextRule.getAppender(APPENDER_NAME, PausableLogWriterAppender.class);

    String name = testName.getMethodName();
    logFile = new File(temporaryFolder.getRoot(), name + ".log");

    LogConfig config = mock(LogConfig.class);
    when(config.getName()).thenReturn(name);
    when(config.getLogFile()).thenReturn(logFile);

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(config);

    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();

    pausableLogWriterAppender.createSession(logConfigSupplier);
    pausableLogWriterAppender.startSession();
  }

  @After
  public void tearDown() {
    pausableLogWriterAppender.stopSession();
    pausableLogWriterAppender.clearLogEvents();
  }

  @Test
  public void getLogEventsReturnsLoggedEvents() {
    pausableLogWriterAppender.clearLogEvents();

    logger.info(logMessage);

    assertThat(pausableLogWriterAppender.getLogEvents()).hasSize(1);
    LogEvent event = pausableLogWriterAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(getClass().getName());
    assertThat(event.getLevel()).isEqualTo(Level.INFO);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);
  }

  @Test
  public void pausedDoesNotLog() {
    pausableLogWriterAppender.pause();
    pausableLogWriterAppender.clearLogEvents();

    logger.info(logMessage);

    assertThat(pausableLogWriterAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void resumeAfterPausedLogs() {
    pausableLogWriterAppender.pause();
    pausableLogWriterAppender.clearLogEvents();
    pausableLogWriterAppender.resume();

    logger.info(logMessage);

    assertThat(pausableLogWriterAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void resumeWithoutPauseStillLogs() {
    pausableLogWriterAppender.clearLogEvents();
    pausableLogWriterAppender.resume();

    logger.info(logMessage);

    assertThat(pausableLogWriterAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void isPausedReturnsTrueAfterPause() {
    pausableLogWriterAppender.pause();
    assertThat(pausableLogWriterAppender.isPaused()).isTrue();
  }

  @Test
  public void isPausedReturnsFalseAfterResume() {
    pausableLogWriterAppender.pause();
    pausableLogWriterAppender.resume();

    assertThat(pausableLogWriterAppender.isPaused()).isFalse();
  }

  @Test
  public void resumeWithoutPauseDoesNothing() {
    pausableLogWriterAppender.resume();

    assertThat(pausableLogWriterAppender.isPaused()).isFalse();
  }

  @Test
  public void isPausedReturnsFalseByDefault() {
    assertThat(pausableLogWriterAppender.isPaused()).isFalse();
  }

  @Test
  public void logsToFile() throws Exception {
    logger.info(logMessage);

    assertThat(logFile).exists();
    String content = FileUtils.readFileToString(logFile, Charset.defaultCharset()).trim();
    assertThat(content).contains(logMessage);
  }

  @Test
  public void linesInFileShouldMatchPatternLayout() throws Exception {
    logger.info(logMessage);

    assertThat(logFile).exists();

    List<String> lines = FileUtils.readLines(logFile, Charset.defaultCharset());
    int logLines = 0;
    for (String line : lines) {
      if (!line.isEmpty()) {
        assertThat(line).matches(LogMessageRegex.getRegex());
        logLines++;
      }
    }

    // make sure there was at least 1 line in the log file
    assertThat(logLines).isGreaterThanOrEqualTo(1);
  }
}
