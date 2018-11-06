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

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
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
import org.apache.logging.log4j.core.LoggerContext;
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
import org.apache.geode.internal.logging.log4j.LogWriterLogger;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Connects DistributedSystem and tests logging behavior at a high level.
 */
@Category(LoggingTest.class)
public class DistributedSystemLogFileIntegrationTest {

  private static final AtomicInteger COUNTER = new AtomicInteger();

  private File logFile;
  private String logFileName;
  private File securityLogFile;
  private String securityLogFileName;

  private InternalDistributedSystem system;

  private String prefix;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    logFile = new File(temporaryFolder.getRoot(),
        testName.getMethodName() + "-system-" + System.currentTimeMillis() + ".log");
    logFileName = logFile.getAbsolutePath();

    securityLogFile = new File(temporaryFolder.getRoot(),
        "security-" + testName.getMethodName() + "-system-" + System.currentTimeMillis() + ".log");
    securityLogFileName = securityLogFile.getAbsolutePath();

    prefix = "ExpectedStrings: " + testName.getMethodName() + " message logged at ";
  }

  @After
  public void tearDown() throws Exception {
    if (system != null) {
      system.disconnect();
    }
    // We will want to remove this at some point but right now the log context
    // does not clear out the security logconfig between tests
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    context.stop();
  }

  @Test
  public void testDistributedSystemLogWritersWithFilesDetails() throws Exception {
    Properties config = new Properties();
    config.put(LOG_FILE, logFileName);
    config.put(SECURITY_LOG_FILE, securityLogFileName);
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> {
      assertThat(logFile).exists();
      assertThat(securityLogFile).exists();
    });

    // assertThat logFile is not empty
    try (FileInputStream fis = new FileInputStream(logFile)) {
      assertThat(fis.available()).isGreaterThan(0);
    }

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel())
        .isEqualTo(LogWriterLevel.CONFIG.getLogWriterLevel());
    assertThat(distributionConfig.getSecurityLogLevel())
        .isEqualTo(LogWriterLevel.CONFIG.getLogWriterLevel());

    assertThat(distributionConfig.getLogFile().getAbsolutePath()).isEqualTo(logFileName);
    assertThat(distributionConfig.getSecurityLogFile().getAbsolutePath())
        .isEqualTo(securityLogFileName);

    assertThat(system.getLogWriter()).isInstanceOf(LogWriterLogger.class);
    assertThat(system.getSecurityLogWriter()).isInstanceOf(LogWriterLogger.class);

    LogWriterLogger logWriterLogger = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriterLogger = (LogWriterLogger) system.getSecurityLogWriter();

    assertThat(logWriterLogger.getLogWriterLevel())
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(securityLogWriterLogger.getLogWriterLevel())
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());

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
    // assertThat(logWriterFastLogger.isDelegating()).isTrue();
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
    // assertThat(securityLogWriterFastLogger.isDelegating()).isFalse();
    assertThat(securityLogWriterFastLogger.isTraceEnabled()).isFalse();
    assertThat(securityLogWriterFastLogger.isDebugEnabled()).isFalse();
    assertThat(securityLogWriterFastLogger.isInfoEnabled()).isTrue();
    assertThat(securityLogWriterFastLogger.isWarnEnabled()).isTrue();
    assertThat(securityLogWriterFastLogger.isErrorEnabled()).isTrue();
    assertThat(securityLogWriterFastLogger.isFatalEnabled()).isTrue();
  }

  @Test
  public void testDistributedSystemCreatesLogFile() throws Exception {
    Properties config = new Properties();
    config.put(LOG_FILE, logFileName);
    config.put(LOG_LEVEL, "config");
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    Logger geodeLogger = LogService.getLogger();
    Logger applicationLogger = LogManager.getLogger("net.customer");

    // -------------------------------------------------------------------------------------------
    // CONFIG level

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel())
        .isEqualTo(LogWriterLevel.CONFIG.getLogWriterLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.INFO);
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);

    String message = createMessage(LogWriterLevel.FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    logWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.TRACE);
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

    // -------------------------------------------------------------------------------------------
    // FINE level

    distributionConfig.setLogLevel(LogWriterLevel.FINE.getLogWriterLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.DEBUG);

    message = createMessage(LogWriterLevel.FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    logWriter.fine(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

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

    message = createMessage(Level.TRACE);
    applicationLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    applicationLogger.debug(message);
    assertThatFileContains(logFile, message);

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

    // -------------------------------------------------------------------------------------------
    // ERROR level

    distributionConfig.setLogLevel(LogWriterLevel.ERROR.getLogWriterLevel());

    assertThat(distributionConfig.getLogLevel())
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.ERROR);
    assertThat(applicationLogger.getLevel()).isEqualTo(Level.ERROR);

    message = createMessage(LogWriterLevel.FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    logWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    logWriter.config(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    logWriter.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    logWriter.warning(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

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

    message = createMessage(Level.TRACE);
    applicationLogger.trace(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.DEBUG);
    applicationLogger.debug(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.INFO);
    applicationLogger.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.WARN);
    applicationLogger.warn(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(Level.ERROR);
    applicationLogger.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.FATAL);
    applicationLogger.fatal(message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void testDistributedSystemWithFineLogLevel() throws Exception {
    Properties config = new Properties();
    config.put(LOG_FILE, logFileName);
    config.put(LOG_LEVEL, "fine");
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    // -------------------------------------------------------------------------------------------
    // FINE level

    assertThat(distributionConfig.getLogLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(LogWriterLevel.FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    logWriter.fine(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

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

    // -------------------------------------------------------------------------------------------
    // ERROR level

    distributionConfig.setLogLevel(LogWriterLevel.ERROR.getLogWriterLevel());

    assertThat(distributionConfig.getLogLevel())
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.ERROR);

    message = createMessage(LogWriterLevel.FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    logWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    logWriter.config(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    logWriter.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    logWriter.warning(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

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
  public void testDistributedSystemWithDebugLogLevel() throws Exception {
    Properties config = new Properties();
    config.put(LOG_FILE, logFileName);
    config.put(LOG_LEVEL, "debug");
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    // -------------------------------------------------------------------------------------------
    // DEBUG LEVEL

    assertThat(distributionConfig.getLogLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(LogWriterLevel.FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    logWriter.fine(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    logWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    logWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    logWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

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

    // -------------------------------------------------------------------------------------------
    // ERROR LEVEL

    distributionConfig.setLogLevel(LogWriterLevel.ERROR.getLogWriterLevel());

    assertThat(distributionConfig.getLogLevel())
        .isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.ERROR.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.ERROR);

    message = createMessage(LogWriterLevel.FINEST);
    logWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    logWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    logWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    logWriter.config(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    logWriter.info(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    logWriter.warning(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    logWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
    logWriter.severe(message);
    assertThatFileContains(logFile, message);

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
  public void testDistributedSystemWithSecurityLogDefaultLevel() throws Exception {
    Properties config = new Properties();
    config.put(LOG_FILE, logFileName);
    config.put(LOG_LEVEL, "fine");
    config.put(SECURITY_LOG_FILE, securityLogFileName);
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> {
      assertThat(logFile).exists();
      assertThat(securityLogFile).exists();
    });

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();
    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(distributionConfig.getSecurityLogLevel())
        .isEqualTo(LogWriterLevel.CONFIG.getLogWriterLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(securityLogWriter.getLogWriterLevel())
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(LogWriterLevel.FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    securityLogWriter.fine(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    securityLogWriter.info(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    geodeLogger.debug(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileContains(logFile, message);
  }

  @Test
  public void testDistributedSystemWithSecurityLogFineLevel() throws Exception {
    Properties config = new Properties();
    config.put(LOG_FILE, logFileName);
    config.put(LOG_LEVEL, "fine");
    config.put(SECURITY_LOG_FILE, securityLogFileName);
    config.put(SECURITY_LOG_LEVEL, "fine");
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> {
      assertThat(logFile).exists();
      assertThat(securityLogFile).exists();
    });

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();
    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(distributionConfig.getSecurityLogLevel())
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(securityLogWriter.getLogWriterLevel())
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(LogWriterLevel.FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    securityLogWriter.fine(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    securityLogWriter.info(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(securityLogFile, message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
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
  public void testDistributedSystemWithSecurityInfoLevelAndLogAtFineLevelButNoSecurityLog()
      throws Exception {
    Properties CONFIG = new Properties();
    CONFIG.put(LOG_FILE, logFileName);
    CONFIG.put(LOG_LEVEL, "fine");
    CONFIG.put(SECURITY_LOG_LEVEL, "info");
    CONFIG.put(MCAST_PORT, "0");
    CONFIG.put(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(CONFIG);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();
    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(distributionConfig.getSecurityLogLevel())
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(securityLogWriter.getLogWriterLevel())
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.DEBUG);

    String message = createMessage(LogWriterLevel.FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    securityLogWriter.fine(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    securityLogWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(logFile, message);

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
  }

  /**
   * tests scenario where security log has not been set but a level has been set to a more granular
   * level than that of the regular log. Verifies that the correct logs for security show up in the
   * regular log as expected
   */
  @Test
  public void testDistributedSystemWithSecurityFineLevelAndLogAtInfoLevelButNoSecurityLog()
      throws Exception {
    Properties config = new Properties();
    config.put(LOG_FILE, logFileName);
    config.put(LOG_LEVEL, "info");
    config.put(SECURITY_LOG_LEVEL, "fine");
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    LogWriterLogger logWriter = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriter = (LogWriterLogger) system.getSecurityLogWriter();
    Logger geodeLogger = LogService.getLogger();

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogLevel()).isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(distributionConfig.getSecurityLogLevel())
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());

    assertThat(logWriter.getLogWriterLevel()).isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(securityLogWriter.getLogWriterLevel())
        .isEqualTo(LogWriterLevel.FINE.getLogWriterLevel());
    assertThat(geodeLogger.getLevel()).isEqualTo(Level.INFO);

    String message = createMessage(LogWriterLevel.FINEST);
    securityLogWriter.finest(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINER);
    securityLogWriter.finer(message);
    assertThatFileDoesNotContain(logFile, message);

    message = createMessage(LogWriterLevel.FINE);
    securityLogWriter.fine(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.CONFIG);
    securityLogWriter.config(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.INFO);
    securityLogWriter.info(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.WARNING);
    securityLogWriter.warning(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.ERROR);
    securityLogWriter.error(message);
    assertThatFileContains(logFile, message);

    message = createMessage(LogWriterLevel.SEVERE);
    securityLogWriter.severe(message);
    assertThatFileContains(logFile, message);

    message = createMessage(Level.TRACE);
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
