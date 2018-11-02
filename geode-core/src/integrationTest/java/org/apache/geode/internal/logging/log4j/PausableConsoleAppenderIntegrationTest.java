/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging.log4j;

import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LifeCycle;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.DefaultErrorHandler;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
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

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link PausableConsoleAppender}.
 */
@Category(LoggingTest.class)
public class PausableConsoleAppenderIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "PausableConsoleAppenderIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "STDOUT";
  private static final String OUTPUT_STREAM_MANAGER_NAME = "null.SYSTEM_OUT.false.false";
  private static final String DELEGATE_APPENDER_NAME = "STDOUT_DELEGATE";
  private static final String DELEGATE_OUTPUT_STREAM_MANAGER_NAME = "SYSTEM_OUT.false.false";

  private static String configFilePath;

  private PausableConsoleAppender pausableConsoleAppender;
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
    pausableConsoleAppender =
        loggerContextRule.getAppender("STDOUT", PausableConsoleAppender.class);
    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() throws Exception {
    pausableConsoleAppender.clearLogEvents();
  }

  @Test
  public void getLogEventsIsEmptyByDefault() {
    assertThat(pausableConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void getLogEventsReturnsLoggedEvents() {
    logger.info(logMessage);

    assertThat(pausableConsoleAppender.getLogEvents()).hasSize(1);

    LogEvent event = pausableConsoleAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(getClass().getName());
    assertThat(event.getLevel()).isEqualTo(Level.INFO);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);
  }

  @Test
  public void pausedDoesNotLog() {
    pausableConsoleAppender.pause();

    logger.info(logMessage);

    assertThat(pausableConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void resumeAfterPausedLogs() {
    pausableConsoleAppender.pause();
    pausableConsoleAppender.resume();

    logger.info(logMessage);

    assertThat(pausableConsoleAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void resumeWithoutPauseStillLogs() {
    pausableConsoleAppender.resume();

    logger.info(logMessage);

    assertThat(pausableConsoleAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void isPausedReturnsTrueAfterPause() {
    pausableConsoleAppender.pause();

    assertThat(pausableConsoleAppender.isPaused()).isTrue();
  }

  @Test
  public void isPausedReturnsFalseAfterResume() {
    pausableConsoleAppender.pause();
    pausableConsoleAppender.resume();

    assertThat(pausableConsoleAppender.isPaused()).isFalse();
  }

  @Test
  public void resumeWithoutPauseDoesNothing() {
    pausableConsoleAppender.resume();

    assertThat(pausableConsoleAppender.isPaused()).isFalse();
  }

  @Test
  public void isPausedReturnsFalseByDefault() {
    assertThat(pausableConsoleAppender.isPaused()).isFalse();
  }

  @Test
  public void pausableConsoleAppenderIsConfigured() {
    assertThat(pausableConsoleAppender).isNotNull();

    assertThat(pausableConsoleAppender.getFilter()).isNull();
    assertThat(pausableConsoleAppender.getHandler()).isInstanceOf(DefaultErrorHandler.class);
    assertThat(pausableConsoleAppender.getImmediateFlush()).isTrue();
    assertThat(pausableConsoleAppender.getLayout()).isNotNull();
    assertThat(pausableConsoleAppender.getManager()).isInstanceOf(OutputStreamManager.class);
    assertThat(pausableConsoleAppender.getName()).isEqualTo(APPENDER_NAME);
    assertThat(pausableConsoleAppender.getState()).isSameAs(LifeCycle.State.STARTED);
    // assertThat(pausableConsoleAppender.getTarget()).isSameAs(ConsoleAppender.Target.SYSTEM_OUT);

    OutputStreamManager outputStreamManager = pausableConsoleAppender.getManager();
    assertThat(outputStreamManager.isOpen()).isTrue();
    assertThat(outputStreamManager.getByteBuffer()).isInstanceOf(ByteBuffer.class);
    assertThat(outputStreamManager.hasOutputStream()).isTrue();
    assertThat(outputStreamManager.getContentFormat()).isEmpty();
    assertThat(outputStreamManager.getLoggerContext()).isNull();
    assertThat(outputStreamManager.getName()).isEqualTo(OUTPUT_STREAM_MANAGER_NAME);
  }

  @Test
  public void delegateConsoleAppenderIsConfigured() {
    ConsoleAppender consoleAppender = pausableConsoleAppender.getDelegate();
    assertThat(consoleAppender).isNotNull();

    assertThat(consoleAppender.getFilter()).isNull();
    assertThat(consoleAppender.getHandler()).isInstanceOf(DefaultErrorHandler.class);
    assertThat(consoleAppender.getImmediateFlush()).isTrue();
    assertThat(consoleAppender.getLayout()).isNotNull();
    assertThat(consoleAppender.getManager()).isInstanceOf(OutputStreamManager.class);
    assertThat(consoleAppender.getName()).isEqualTo(DELEGATE_APPENDER_NAME);
    assertThat(consoleAppender.getState()).isSameAs(LifeCycle.State.STARTED);
    assertThat(consoleAppender.getTarget()).isSameAs(ConsoleAppender.Target.SYSTEM_OUT);

    OutputStreamManager outputStreamManager = consoleAppender.getManager();
    assertThat(outputStreamManager.isOpen()).isTrue();
    assertThat(outputStreamManager.getByteBuffer()).isInstanceOf(ByteBuffer.class);
    assertThat(outputStreamManager.hasOutputStream()).isTrue();
    assertThat(outputStreamManager.getContentFormat()).isEmpty();
    assertThat(outputStreamManager.getLoggerContext()).isNull();
    assertThat(outputStreamManager.getName()).isEqualTo(DELEGATE_OUTPUT_STREAM_MANAGER_NAME);
  }
}
