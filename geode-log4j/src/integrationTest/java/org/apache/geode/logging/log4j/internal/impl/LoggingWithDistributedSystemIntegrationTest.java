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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.CONFIG;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.ERROR;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINER;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINEST;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.INFO;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.SEVERE;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.WARNING;
import static org.apache.geode.logging.log4j.internal.impl.Log4jLoggingProvider.getLoggerConfig;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
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

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.logging.internal.log4j.LogWriterLogger;
import org.apache.geode.logging.internal.log4j.api.FastLogger;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogWriterLevel;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for logging with {@link InternalDistributedSystem} lifecycle.
 */
@Category(LoggingTest.class)
@SuppressWarnings("deprecation")
public class LoggingWithDistributedSystemIntegrationTest {

  private static final String APPLICATION_LOGGER_NAME = "com.application";
  private static final AtomicInteger COUNTER = new AtomicInteger();

  private String currentWorkingDirPath;
  private File mainLogFile;
  private String mainLogFilePath;
  private File securityLogFile;
  private String securityLogFilePath;
  private InternalDistributedSystem system;
  private String prefix;

  private Logger geodeLogger;
  private Logger applicationLogger;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    File currentWorkingDir = new File("");
    currentWorkingDirPath = currentWorkingDir.getAbsolutePath();

    String name = testName.getMethodName();
    mainLogFile = new File(temporaryFolder.getRoot(), name + "-main.log");
    mainLogFilePath = mainLogFile.getAbsolutePath();
    securityLogFile = new File(temporaryFolder.getRoot(), name + "-security.log");
    securityLogFilePath = securityLogFile.getAbsolutePath();

    prefix = "ExpectedStrings: " + testName.getMethodName() + " message logged at level ";

    geodeLogger = LogService.getLogger();
    applicationLogger = LogManager.getLogger(APPLICATION_LOGGER_NAME);

    Log4jLoggingProvider.updateLogLevel(Level.INFO, getLoggerConfig(applicationLogger));
  }

  @After
  public void tearDown() {
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
    assertThat(logConfig.getSecurityLogFile().getAbsolutePath()).isEqualTo(currentWorkingDirPath);
    assertThat(logConfig.getSecurityLogLevel()).isEqualTo(CONFIG.intLevel());
  }

  @Test
  public void bothLogFilesConfigured() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(SECURITY_LOG_FILE, securityLogFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger logWriterLogger = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriterLogger = (LogWriterLogger) system.getSecurityLogWriter();

    await().untilAsserted(() -> {
      system.getLogWriter().info("log another line");

      assertThat(mainLogFile).exists();
      assertThat(securityLogFile).exists();

      // assertThat mainLogFile is not empty
      try (FileInputStream fis = new FileInputStream(mainLogFile)) {
        assertThat(fis.available()).isGreaterThan(0);
      }
    });

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(CONFIG.intLevel());

    assertThat(distributionConfig.getLogFile().getAbsolutePath()).isEqualTo(mainLogFilePath);
    assertThat(distributionConfig.getSecurityLogFile().getAbsolutePath())
        .isEqualTo(securityLogFilePath);

    assertThat(system.getLogWriter()).isInstanceOf(LogWriterLogger.class);
    assertThat(system.getSecurityLogWriter()).isInstanceOf(LogWriterLogger.class);

    assertThat(logWriterLogger.getLogWriterLevel()).isEqualTo(INFO.intLevel());
    assertThat(securityLogWriterLogger.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    securityLogWriterLogger.info("test: security log file created at info");

    // assertThat securityLogFile is not empty
    try (FileInputStream fis = new FileInputStream(securityLogFile)) {
      assertThat(fis.available()).isGreaterThan(0);
    }

    org.apache.geode.LogWriter logWriter = logWriterLogger;
    assertThat(logWriter.finestEnabled()).isFalse();
    assertThat(logWriter.finerEnabled()).isFalse();
    assertThat(logWriter.fineEnabled()).isFalse();
    assertThat(logWriter.configEnabled()).isTrue();
    assertThat(logWriter.infoEnabled()).isTrue();
    assertThat(logWriter.warningEnabled()).isTrue();
    assertThat(logWriter.errorEnabled()).isTrue();
    assertThat(logWriter.severeEnabled()).isTrue();

    FastLogger logWriterFastLogger = (FastLogger) logWriterLogger.getExtendedLogger();
    assertThat(logWriterFastLogger.isDelegating()).isFalse();
    assertThat(logWriterFastLogger.isTraceEnabled()).isFalse();
    assertThat(logWriterFastLogger.isDebugEnabled()).isFalse();
    assertThat(logWriterFastLogger.isInfoEnabled()).isTrue();
    assertThat(logWriterFastLogger.isWarnEnabled()).isTrue();
    assertThat(logWriterFastLogger.isErrorEnabled()).isTrue();
    assertThat(logWriterFastLogger.isFatalEnabled()).isTrue();

    org.apache.geode.LogWriter securityLogWriter = securityLogWriterLogger;
    assertThat(securityLogWriter.finestEnabled()).isFalse();
    assertThat(securityLogWriter.finerEnabled()).isFalse();
    assertThat(securityLogWriter.fineEnabled()).isFalse();
    assertThat(securityLogWriter.configEnabled()).isTrue();
    assertThat(securityLogWriter.infoEnabled()).isTrue();
    assertThat(securityLogWriter.warningEnabled()).isTrue();
    assertThat(securityLogWriter.errorEnabled()).isTrue();
    assertThat(securityLogWriter.severeEnabled()).isTrue();

    FastLogger securityLogWriterFastLogger = (FastLogger) logWriterLogger.getExtendedLogger();
    assertThat(securityLogWriterFastLogger.isDelegating()).isFalse();
    assertThat(securityLogWriterFastLogger.isTraceEnabled()).isFalse();
    assertThat(securityLogWriterFastLogger.isDebugEnabled()).isFalse();
    assertThat(securityLogWriterFastLogger.isInfoEnabled()).isTrue();
    assertThat(securityLogWriterFastLogger.isWarnEnabled()).isTrue();
    assertThat(securityLogWriterFastLogger.isErrorEnabled()).isTrue();
    assertThat(securityLogWriterFastLogger.isFatalEnabled()).isTrue();
  }

  @Test
  public void mainLogWriterLogsToMainLogFile() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    // CONFIG level

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    logWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    logWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    logWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    logWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // FINE level

    distributionConfig.setLogLevel(FINE.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    message = createMessage(FINEST);
    logWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    logWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    logWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    logWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    logWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(ERROR.intLevel());

    message = createMessage(FINEST);
    logWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    logWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    logWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(INFO);
    logWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(ERROR);
    logWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void securityLogWriterLogsToMainLogFile() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    // CONFIG level

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // FINE level

    distributionConfig.setLogLevel(FINE.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    message = createMessage(FINEST);
    securityLogWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    message = createMessage(FINEST);
    securityLogWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void geodeLoggerLogsToMainLogFile() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    // CONFIG level

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.INFO);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // FINE level

    distributionConfig.setLogLevel(FINE.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.ERROR);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void applicationLoggerLogsToMainLogFile() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    // CONFIG level

    assertThat(distributionConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);

    String message = createMessage(Level.TRACE);
    applicationLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    applicationLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.INFO);
    applicationLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    applicationLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    applicationLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    applicationLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // FINE level

    distributionConfig.setLogLevel(FINE.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);

    message = createMessage(Level.TRACE);
    applicationLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    applicationLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.INFO);
    applicationLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    applicationLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    applicationLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    applicationLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);

    message = createMessage(Level.TRACE);
    applicationLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    applicationLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.INFO);
    applicationLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    applicationLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    applicationLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    applicationLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void mainLogWriterLogsToMainLogFileWithMainLogLevelFine() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, FINE.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    // FINE level

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    logWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    logWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    logWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    logWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(ERROR.intLevel());

    message = createMessage(FINEST);
    logWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    logWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    logWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(INFO);
    logWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(ERROR);
    logWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void securityLogWriterLogsToMainLogFileWithMainLogLevelFine() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, FINE.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void geodeLoggerLogsToMainLogFileWithMainLogLevelFine() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, FINE.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    // FINE level

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // ERROR level

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.ERROR);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void mainLogWriterLogsToMainLogFileWithMainLogLevelDebug() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, Level.DEBUG.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    // DEBUG LEVEL

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    logWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    logWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    logWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    logWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // ERROR LEVEL

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(ERROR.intLevel());

    message = createMessage(FINEST);
    logWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    logWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    logWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(INFO);
    logWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(ERROR);
    logWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void securityLogWriterLogsToMainLogFileWithMainLogLevelDebug() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, Level.DEBUG.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void geodeLoggerLogsToMainLogFileWithMainLogLevelDebug() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, Level.DEBUG.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    // DEBUG LEVEL

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    // ERROR LEVEL

    distributionConfig.setLogLevel(ERROR.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(ERROR.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.ERROR);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void logsToDifferentLogFilesWithMainLogLevelFine() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, FINE.name());
    config.setProperty(SECURITY_LOG_FILE, securityLogFilePath);

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    await().untilAsserted(() -> {
      assertThat(mainLogFile).exists();
      assertThat(securityLogFile).exists();
    });

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(CONFIG.intLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    LogFileAssert.assertThat(securityLogFile).contains(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  @Test
  public void logsToDifferentLogFilesWithBothLogLevelsFine() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, FINE.name());
    config.setProperty(SECURITY_LOG_FILE, securityLogFilePath);
    config.setProperty(SECURITY_LOG_LEVEL, FINE.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    await().untilAsserted(() -> {
      assertThat(mainLogFile).exists();
      assertThat(securityLogFile).exists();
    });

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(FINE.intLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    LogFileAssert.assertThat(securityLogFile).contains(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    LogFileAssert.assertThat(securityLogFile).contains(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    LogFileAssert.assertThat(securityLogFile).contains(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    LogFileAssert.assertThat(securityLogFile).contains(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    LogFileAssert.assertThat(securityLogFile).contains(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    LogFileAssert.assertThat(securityLogFile).contains(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(securityLogFile).doesNotContain(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a less granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void mainLogWriterLogsToMainLogFileWithHigherSecurityLogLevel() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, FINE.name());
    config.setProperty(SECURITY_LOG_LEVEL, INFO.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    logWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    logWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    logWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    logWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a less granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void securityLogWriterLogsToMainLogFileWithHigherSecurityLogLevel() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, FINE.name());
    config.setProperty(SECURITY_LOG_LEVEL, INFO.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());

    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a less granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void geodeLoggerLogsToMainLogFileWithHigherSecurityLogLevel() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, FINE.name());
    config.setProperty(SECURITY_LOG_LEVEL, INFO.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(FINE.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a more granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void mainLogWriterLogsToMainLogFileWithLowerSecurityLogLevel() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, INFO.name());
    config.setProperty(SECURITY_LOG_LEVEL, FINE.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(INFO.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(FINE.intLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    String message = createMessage(FINEST);
    logWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    logWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    logWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(CONFIG);
    logWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    logWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    logWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    logWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    logWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a more granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void securityLogWriterLogsToMainLogFileWithLowerSecurityLogLevel() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, INFO.name());
    config.setProperty(SECURITY_LOG_LEVEL, FINE.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(INFO.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(FINE.intLevel());

    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(FINE.intLevel());

    String message = createMessage(FINEST);
    securityLogWriter.finest(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINER);
    securityLogWriter.finer(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(FINE);
    securityLogWriter.fine(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(CONFIG);
    securityLogWriter.config(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(INFO);
    securityLogWriter.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(WARNING);
    securityLogWriter.warning(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(ERROR);
    securityLogWriter.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(SEVERE);
    securityLogWriter.severe(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a more granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void geodeLoggerLogsToMainLogFileWithLowerSecurityLogLevel() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFilePath);
    config.setProperty(LOG_LEVEL, INFO.name());
    config.setProperty(SECURITY_LOG_LEVEL, FINE.name());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    DistributionConfig distributionConfig = system.getConfig();

    await().untilAsserted(() -> assertThat(mainLogFile).exists());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(INFO.intLevel());
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(FINE.intLevel());

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.INFO);

    String message = createMessage(Level.TRACE);
    geodeLogger.trace(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.DEBUG);
    geodeLogger.debug(message);
    LogFileAssert.assertThat(mainLogFile).doesNotContain(message);

    message = createMessage(Level.INFO);
    geodeLogger.info(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.WARN);
    geodeLogger.warn(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.ERROR);
    geodeLogger.error(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);

    message = createMessage(Level.FATAL);
    geodeLogger.fatal(message);
    LogFileAssert.assertThat(mainLogFile).contains(message);
  }

  private String createMessage(LogWriterLevel logLevel) {
    return prefix + logLevel.name() + " [" + COUNTER.incrementAndGet() + "]";
  }

  private String createMessage(Level level) {
    return prefix + level.name() + " [" + COUNTER.incrementAndGet() + "]";
  }
}
