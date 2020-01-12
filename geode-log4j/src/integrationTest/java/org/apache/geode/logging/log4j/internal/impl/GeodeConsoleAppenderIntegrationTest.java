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

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/** Integration tests for {@link GeodeConsoleAppender}. */
@Category(LoggingTest.class)
public class GeodeConsoleAppenderIntegrationTest {

  private static final String CONFIG_FILE_NAME = "GeodeConsoleAppenderIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "STDOUT";
  private static final String OUTPUT_STREAM_MANAGER_NAME = "null.SYSTEM_OUT.false.false";
  private static final String DELEGATE_APPENDER_NAME = "STDOUT_DELEGATE";
  private static final String DELEGATE_OUTPUT_STREAM_MANAGER_NAME = "SYSTEM_OUT.false.false";

  private static String configFilePath;

  private GeodeConsoleAppender geodeConsoleAppender;
  private Logger logger;
  private String logMessage;

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
    geodeConsoleAppender =
        loggerContextRule.getAppender("STDOUT", GeodeConsoleAppender.class);
    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() {
    geodeConsoleAppender.clearLogEvents();
  }

  @Test
  public void getLogEventsIsEmptyByDefault() {
    assertThat(geodeConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void getLogEventsReturnsLoggedEvents() {
    logger.info(logMessage);

    assertThat(geodeConsoleAppender.getLogEvents()).hasSize(1);

    LogEvent event = geodeConsoleAppender.getLogEvents().get(0);
    assertThat(event.getLoggerName()).isEqualTo(getClass().getName());
    assertThat(event.getLevel()).isEqualTo(Level.INFO);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);
  }

  @Test
  public void pausedDoesNotLog() {
    geodeConsoleAppender.pause();

    logger.info(logMessage);

    assertThat(geodeConsoleAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void resumeAfterPausedLogs() {
    geodeConsoleAppender.pause();
    geodeConsoleAppender.resume();

    logger.info(logMessage);

    assertThat(geodeConsoleAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void resumeWithoutPauseStillLogs() {
    geodeConsoleAppender.resume();

    logger.info(logMessage);

    assertThat(geodeConsoleAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void isPausedReturnsTrueAfterPause() {
    geodeConsoleAppender.pause();

    assertThat(geodeConsoleAppender.isPaused()).isTrue();
  }

  @Test
  public void isPausedReturnsFalseAfterResume() {
    geodeConsoleAppender.pause();
    geodeConsoleAppender.resume();

    assertThat(geodeConsoleAppender.isPaused()).isFalse();
  }

  @Test
  public void resumeWithoutPauseDoesNothing() {
    geodeConsoleAppender.resume();

    assertThat(geodeConsoleAppender.isPaused()).isFalse();
  }

  @Test
  public void isPausedReturnsFalseByDefault() {
    assertThat(geodeConsoleAppender.isPaused()).isFalse();
  }

  @Test
  public void geodeConsoleAppenderIsConfigured() {
    assertThat(geodeConsoleAppender).isNotNull();

    assertThat(geodeConsoleAppender.getFilter()).isNull();
    assertThat(geodeConsoleAppender.getHandler()).isInstanceOf(DefaultErrorHandler.class);
    assertThat(geodeConsoleAppender.getImmediateFlush()).isTrue();
    assertThat(geodeConsoleAppender.getLayout()).isNotNull();
    assertThat(geodeConsoleAppender.getManager()).isInstanceOf(OutputStreamManager.class);
    assertThat(geodeConsoleAppender.getName()).isEqualTo(APPENDER_NAME);
    assertThat(geodeConsoleAppender.getState()).isSameAs(LifeCycle.State.STARTED);
    // assertThat(geodeConsoleAppender.getTarget()).isSameAs(ConsoleAppender.Target.SYSTEM_OUT);

    OutputStreamManager outputStreamManager = geodeConsoleAppender.getManager();
    assertThat(outputStreamManager.isOpen()).isTrue();
    assertThat(outputStreamManager.getByteBuffer()).isInstanceOf(ByteBuffer.class);
    assertThat(outputStreamManager.hasOutputStream()).isTrue();
    assertThat(outputStreamManager.getContentFormat()).isEmpty();
    assertThat(outputStreamManager.getLoggerContext()).isNull();
    assertThat(outputStreamManager.getName()).isEqualTo(OUTPUT_STREAM_MANAGER_NAME);
  }

  @Test
  public void delegateConsoleAppenderIsConfigured() {
    ConsoleAppender consoleAppender = geodeConsoleAppender.getDelegate();
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
