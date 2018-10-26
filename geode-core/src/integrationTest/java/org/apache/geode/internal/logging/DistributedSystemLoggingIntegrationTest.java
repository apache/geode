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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.internal.logging.LogWriterLevel.CONFIG;
import static org.apache.geode.internal.logging.LogWriterLevel.ERROR;
import static org.apache.geode.internal.logging.LogWriterLevel.FINE;
import static org.apache.geode.internal.logging.LogWriterLevel.FINER;
import static org.apache.geode.internal.logging.LogWriterLevel.FINEST;
import static org.apache.geode.internal.logging.LogWriterLevel.INFO;
import static org.apache.geode.internal.logging.LogWriterLevel.SEVERE;
import static org.apache.geode.internal.logging.LogWriterLevel.WARNING;
import static org.apache.geode.internal.logging.log4j.Log4jAgent.getLoggerConfig;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.log4j.FastLogger;
import org.apache.geode.internal.logging.log4j.Log4jAgent;
import org.apache.geode.internal.logging.log4j.LogWriterLogger;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for logging with {@link InternalDistributedSystem} lifecycle.
 */
@Category(LoggingTest.class)
public class DistributedSystemLoggingIntegrationTest {

  private static final String APPLICATION_LOGGER_NAME = "com.application";
  private static final AtomicInteger COUNTER = new AtomicInteger();

  private String currentWorkingDirPath;
  private File logFile;
  private String logFilePath;
  private File securityLogFile;
  private String securityLogFilePath;
  private InternalDistributedSystem system;
  private String prefix;

  private Logger applicationLogger;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    File currentWorkingDir = new File("");
    currentWorkingDirPath = currentWorkingDir.getAbsolutePath();

    logFile = new File(temporaryFolder.getRoot(), testName.getMethodName() + "-main.log");
    logFilePath = logFile.getAbsolutePath();

    securityLogFile =
        new File(temporaryFolder.getRoot(), testName.getMethodName() + "-security.log");
    securityLogFilePath = securityLogFile.getAbsolutePath();

    prefix = "ExpectedStrings: " + testName.getMethodName() + " message logged at ";

    applicationLogger = LogManager.getLogger(APPLICATION_LOGGER_NAME);

    Log4jAgent.updateLogLevel(Level.INFO, getLoggerConfig(applicationLogger));
  }

  @After
  public void tearDown() throws Exception {
    if (system != null) {
      system.disconnect();
    }
  }

  @Test
  public void applicationLoggerLevelIsInfo() {
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);
  }

  @Test
  public void defaultConfigForLogging() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    LogConfig logConfig = system.getLogConfig();

    assertThat(logConfig.getName()).isEqualTo("");
    assertThat(logConfig.getLogFile().getAbsolutePath()).isEqualTo(currentWorkingDirPath);
    assertThat(logConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(logConfig.getLogDiskSpaceLimit()).isEqualTo(0);
    assertThat(logConfig.getLogFileSizeLimit()).isEqualTo(0);
    assertThat(logConfig.getSecurityLogFile().getAbsolutePath())
        .isEqualTo(currentWorkingDirPath);
    assertThat(logConfig.getSecurityLogLevel()).isEqualTo(CONFIG.intLevel());
  }

  @Test
  public void bothLogFilesConfigured() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(SECURITY_LOG_FILE, securityLogFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> {
      system.getLogWriter().info("log another line");

      assertThat(logFile).exists();
      assertThat(securityLogFile).exists();

      // assertThat logFile is not empty
      try (FileInputStream fis = new FileInputStream(logFile)) {
        assertThat(fis.available()).isGreaterThan(0);
      }
    });

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(CONFIG.intLevel());

    assertThat(distributionConfig.getLogFile().getAbsolutePath()).isEqualTo(logFilePath);
    assertThat(distributionConfig.getSecurityLogFile().getAbsolutePath())
        .isEqualTo(securityLogFilePath);

    assertThat(system.getLogWriter()).isInstanceOf(LogWriterLogger.class);
    assertThat(system.getSecurityLogWriter()).isInstanceOf(LogWriterLogger.class);

    LogWriterLogger logWriterLogger = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriterLogger = (LogWriterLogger) system.getSecurityLogWriter();

    assertThat(logWriterLogger.getLogWriterLevel()).isEqualTo(INFO.intLevel());
    assertThat(securityLogWriterLogger.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    securityLogWriterLogger.info("test: security log file created at info");

    // assertThat securityLogFile is not empty
    try (FileInputStream fis = new FileInputStream(securityLogFile)) {
      assertThat(fis.available()).isGreaterThan(0);
    }

    LogWriter logWriter = logWriterLogger;
    assertThat(logWriter.finestEnabled()).isFalse();
    assertThat(logWriter.finerEnabled()).isFalse();
    assertThat(logWriter.fineEnabled()).isFalse();
    assertThat(logWriter.configEnabled()).isTrue();
    assertThat(logWriter.infoEnabled()).isTrue();
    assertThat(logWriter.warningEnabled()).isTrue();
    assertThat(logWriter.errorEnabled()).isTrue();
    assertThat(logWriter.severeEnabled()).isTrue();

    FastLogger logWriterFastLogger = logWriterLogger;
    // TODO: assertThat(logWriterFastLogger.isDelegating()).isTrue();
    assertThat(logWriterFastLogger.isTraceEnabled()).isFalse();
    assertThat(logWriterFastLogger.isDebugEnabled()).isFalse();
    assertThat(logWriterFastLogger.isInfoEnabled()).isTrue();
    assertThat(logWriterFastLogger.isWarnEnabled()).isTrue();
    assertThat(logWriterFastLogger.isErrorEnabled()).isTrue();
    assertThat(logWriterFastLogger.isFatalEnabled()).isTrue();

    LogWriter securityLogWriter = securityLogWriterLogger;
    assertThat(securityLogWriter.finestEnabled()).isFalse();
    assertThat(securityLogWriter.finerEnabled()).isFalse();
    assertThat(securityLogWriter.fineEnabled()).isFalse();
    assertThat(securityLogWriter.configEnabled()).isTrue();
    assertThat(securityLogWriter.infoEnabled()).isTrue();
    assertThat(securityLogWriter.warningEnabled()).isTrue();
    assertThat(securityLogWriter.errorEnabled()).isTrue();
    assertThat(securityLogWriter.severeEnabled()).isTrue();

    FastLogger securityLogWriterFastLogger = logWriterLogger;
    // TODO: assertThat(securityLogWriterFastLogger.isDelegating()).isFalse();
    assertThat(securityLogWriterFastLogger.isTraceEnabled()).isFalse();
    assertThat(securityLogWriterFastLogger.isDebugEnabled()).isFalse();
    assertThat(securityLogWriterFastLogger.isInfoEnabled()).isTrue();
    assertThat(securityLogWriterFastLogger.isWarnEnabled()).isTrue();
    assertThat(securityLogWriterFastLogger.isErrorEnabled()).isTrue();
    assertThat(securityLogWriterFastLogger.isFatalEnabled()).isTrue();
  }

  @Test
  public void mainLogWriterLogsToMainLogFile() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    // CONFIG level

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    logWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

    // FINE level

    distributionConfig.setLogLevel(FINE.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    message = createMessage(FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    logWriter.fine(message);
    assertThatFileContains(logFile, message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(ERROR.intLevel());

    message = createMessage(FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    logWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(INFO);
    logWriter.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void securityLogWriterLogsToMainLogFile() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    // CONFIG level

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(logFile, message);

    // FINE level

    distributionConfig.setLogLevel(FINE.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    message = createMessage(FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(logFile, message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    message = createMessage(FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void geodeLoggerLogsToMainLogFile() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    Logger geodeLogger = LogService.getLogger();

    // CONFIG level

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.INFO);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileContains(logFile, message);

    // FINE level

    distributionConfig.setLogLevel(FINE.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileContains(logFile, message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.ERROR);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void applicationLoggerLogsToMainLogFile() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    // CONFIG level

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);

    String message = createMessage(Level.TRACE);
    applicationLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    applicationLogger.debug(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.INFO);
    applicationLogger.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    applicationLogger.warn(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    applicationLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    applicationLogger.fatal(message);
    assertThatFileContains(logFile, message);

    // FINE level

    distributionConfig.setLogLevel(FINE.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);

    message = createMessage(Level.TRACE);
    applicationLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    applicationLogger.debug(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.INFO);
    applicationLogger.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    applicationLogger.warn(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    applicationLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    applicationLogger.fatal(message);
    assertThatFileContains(logFile, message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);

    message = createMessage(Level.TRACE);
    applicationLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    applicationLogger.debug(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.INFO);
    applicationLogger.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    applicationLogger.warn(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    applicationLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    applicationLogger.fatal(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void mainLogWriterLogsToMainLogFileWithMainLogLevelFine() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "fine");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    DistributionConfig distributionConfig = system.getConfig();

    // FINE level

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    logWriter.fine(message);
    assertThatFileContains(logFile, message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(ERROR.intLevel());

    message = createMessage(FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    logWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(INFO);
    logWriter.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void securityLogWriterLogsToMainLogFileWithMainLogLevelFine() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "fine");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void geodeLoggerLogsToMainLogFileWithMainLogLevelFine() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "fine");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    // FINE level

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileContains(logFile, message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.ERROR);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void mainLogWriterLogsToMainLogFileWithMainLogLevelDebug() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "debug");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    DistributionConfig distributionConfig = system.getConfig();

    // DEBUG LEVEL

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    logWriter.fine(message);
    assertThatFileContains(logFile, message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

    // ERROR LEVEL

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(ERROR.intLevel());

    message = createMessage(FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    logWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(INFO);
    logWriter.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void securityLogWriterLogsToMainLogFileWithMainLogLevelDebug() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "debug");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void geodeLoggerLogsToMainLogFileWithMainLogLevelDebug() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "debug");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    // DEBUG LEVEL

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileContains(logFile, message);

    // ERROR LEVEL

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.ERROR);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void logsToDifferentLogFilesWithMainLogLevelFine() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "fine");
    config.setProperty(SECURITY_LOG_FILE, securityLogFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> {
      assertThat(logFile).exists();
      assertThat(securityLogFile).exists();
    });

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();
    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(CONFIG.intLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    geodeLogger.debug(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void logsToDifferentLogFilesWithBothLogLevelsFine() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "fine");
    config.setProperty(SECURITY_LOG_FILE, securityLogFilePath);
    config.setProperty(SECURITY_LOG_LEVEL, "fine");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> {
      assertThat(logFile).exists();
      assertThat(securityLogFile).exists();
    });

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();
    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(FINE.intLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileContains(logFile, message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a less granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void mainLogWriterLogsToMainLogFileWithHigherSecurityLogLevel() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "fine");
    config.setProperty(SECURITY_LOG_LEVEL, "info");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    logWriter.fine(message);
    assertThatFileContains(logFile, message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a less granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void securityLogWriterLogsToMainLogFileWithHigherSecurityLogLevel() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "fine");
    config.setProperty(SECURITY_LOG_LEVEL, "info");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());

    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a less granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void geodeLoggerLogsToMainLogFileWithHigherSecurityLogLevel() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "fine");
    config.setProperty(SECURITY_LOG_LEVEL, "info");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileContains(logFile, message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a more granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void mainLogWriterLogsToMainLogFileWithLowerSecurityLogLevel() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "info");
    config.setProperty(SECURITY_LOG_LEVEL, "fine");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(INFO.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(FINE.intLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    logWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a more granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void securityLogWriterLogsToMainLogFileWithLowerSecurityLogLevel() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "info");
    config.setProperty(SECURITY_LOG_LEVEL, "fine");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(INFO.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(FINE.intLevel());

    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    assertThatFileContains(logFile, message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(logFile, message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a more granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void geodeLoggerLogsToMainLogFileWithLowerSecurityLogLevel() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFilePath);
    config.setProperty(LOG_LEVEL, "info");
    config.setProperty(SECURITY_LOG_LEVEL, "fine");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().atMost(5, MINUTES).untilAsserted(() -> assertThat(logFile).exists());

    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(INFO.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(FINE.intLevel());

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.INFO);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    assertThatFileContains(logFile, message);
  }

  private String createMessage(LogWriterLevel logLevel) {
    return prefix + logLevel.name() + " [" + COUNTER.incrementAndGet() + "]";
  }

  private String createMessage(Level level) {
    return prefix + level.name() + " [" + COUNTER.incrementAndGet() + "]";
  }

  private void assertThatFileContains(final File file, final String string)
      throws IOException {
    try (Scanner scanner = new Scanner(file)) {
      while (scanner.hasNextLine()) {
        if (scanner.nextLine().trim().contains(string)) {
          return;
        }
      }
    }

    List<String> lines = Files.readAllLines(file.toPath());
    fail("Expected file " + file.getAbsolutePath() + " to contain " + string + LINE_SEPARATOR
        + "Actual: " + lines);
  }

  private void assertThatFileDoesNotContain(final File file, final String string)
      throws IOException {
    boolean fail = false;
    try (Scanner scanner = new Scanner(file)) {
      while (scanner.hasNextLine()) {
        if (scanner.nextLine().trim().contains(string)) {
          fail = true;
          break;
        }
      }
    }
    if (fail) {
      List<String> lines = Files.readAllLines(file.toPath());
      fail("Expected file " + file.getAbsolutePath() + " to not contain " + string + LINE_SEPARATOR
          + "Actual: " + lines);
    }
  }
}
