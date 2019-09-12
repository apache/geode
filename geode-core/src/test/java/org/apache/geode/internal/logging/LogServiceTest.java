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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.MessageFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.LogWriter;
import org.apache.geode.internal.logging.log4j.FastLogger;
import org.apache.geode.internal.logging.log4j.LogWriterLogger;
import org.apache.geode.internal.logging.log4j.message.GemFireParameterizedMessageFactory;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link LogService}.
 */
@Category(LoggingTest.class)
public class LogServiceTest {

  private static final String APPLICATION_LOGGER_NAME = "com.application";

  @Rule
  public TestName testName = new TestName();

  @Test
  public void getLoggerReturnsFastLogger() {
    assertThat(LogService.getLogger()).isInstanceOf(FastLogger.class);
  }

  @Test
  public void getLoggerReturnsLoggerWithCallerClassName() {
    assertThat(LogService.getLogger().getName()).isEqualTo(getClass().getName());
  }

  @Test
  public void getLoggerReturnsLoggerWithGeodeMessageFactory() {
    Logger logger = LogService.getLogger();

    MessageFactory messageFactory = logger.getMessageFactory();
    assertThat(messageFactory).isInstanceOf(GemFireParameterizedMessageFactory.class);
  }

  @Test
  public void getLoggerNameReturnsLoggerWithSpecifiedName() {
    assertThat(LogService.getLogger(APPLICATION_LOGGER_NAME).getName())
        .isEqualTo(APPLICATION_LOGGER_NAME);
  }

  @Test
  public void getLoggerNameReturnsLoggerWithGeodeMessageFactory() {
    Logger logger = LogService.getLogger(APPLICATION_LOGGER_NAME);

    MessageFactory messageFactory = logger.getMessageFactory();
    assertThat(messageFactory).isInstanceOf(GemFireParameterizedMessageFactory.class);
  }

  @Test
  public void createLogWriterLoggerReturnsFastLogger() {
    LogWriterLogger logWriterLogger =
        DeprecatedLogService
            .createLogWriterLogger(getClass().getName(), testName.getMethodName(), false);

    assertThat(logWriterLogger).isInstanceOf(FastLogger.class);
  }

  @Test
  public void createLogWriterLoggerReturnsLogWriter() {
    LogWriterLogger logWriterLogger =
        DeprecatedLogService
            .createLogWriterLogger(getClass().getName(), testName.getMethodName(), false);

    assertThat(logWriterLogger).isInstanceOf(LogWriter.class);
  }

  @Test
  public void createLogWriterLoggerReturnsLoggerWithSpecifiedName() {
    LogWriterLogger logWriterLogger =
        DeprecatedLogService
            .createLogWriterLogger(getClass().getName(), testName.getMethodName(), false);

    assertThat(logWriterLogger.getName()).isEqualTo(getClass().getName());
  }

  @Test
  public void createLogWriterLoggerReturnsLoggerWithSpecifiedConnectionName() {
    LogWriterLogger logWriterLogger =
        DeprecatedLogService
            .createLogWriterLogger(getClass().getName(), testName.getMethodName(), false);

    assertThat(logWriterLogger.getConnectionName()).isEqualTo(testName.getMethodName());
  }

  @Test
  public void createLogWriterLoggerReturnsLoggerWithGeodeMessageFactory() {
    LogWriterLogger logWriterLogger =
        DeprecatedLogService
            .createLogWriterLogger(getClass().getName(), testName.getMethodName(), false);

    MessageFactory messageFactory = logWriterLogger.getMessageFactory();
    assertThat(messageFactory).isInstanceOf(GemFireParameterizedMessageFactory.class);
  }

  @Test
  public void createLogWriterLoggerWithSecureFalseReturnsSecureLogWriter() {
    LogWriterLogger logWriterLogger =
        DeprecatedLogService
            .createLogWriterLogger(getClass().getName(), testName.getMethodName(), false);

    assertThat(logWriterLogger.isSecure()).isFalse();
  }

  @Test
  public void createLogWriterLoggerWithSecureTrueReturnsSecureLogWriter() {
    LogWriterLogger logWriterLogger =
        DeprecatedLogService
            .createLogWriterLogger(getClass().getName(), testName.getMethodName(), true);

    assertThat(logWriterLogger.isSecure()).isTrue();
  }
}
