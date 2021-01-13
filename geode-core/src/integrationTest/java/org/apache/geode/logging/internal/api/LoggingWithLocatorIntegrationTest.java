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
package org.apache.geode.logging.internal.api;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.CONFIG;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.INFO;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.logging.internal.log4j.LogWriterLogger;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for logging with {@link InternalLocator} lifecycle.
 */
@Category(LoggingTest.class)
public class LoggingWithLocatorIntegrationTest {

  private String name;
  private int port;
  private String currentWorkingDirPath;
  private File logFile;
  private File securityLogFile;

  private InternalLocator locator;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    name = testName.getMethodName();

    File currentWorkingDir = new File("");
    currentWorkingDirPath = currentWorkingDir.getAbsolutePath();
    logFile = new File(temporaryFolder.getRoot(),
        testName.getMethodName() + "-system-" + System.currentTimeMillis() + ".log");
    securityLogFile = new File("");

    port = getRandomAvailableTCPPort();
  }

  @After
  public void tearDown() {
    if (locator != null) {
      locator.stop();
    }
  }

  @Test
  public void startLocatorDefaultLoggingConfig() throws Exception {
    Properties config = new Properties();

    locator = InternalLocator.startLocator(port, null, null, null, null, false, config, null,
        temporaryFolder.getRoot().toPath());

    LogConfig logConfig = locator.getLogConfig();

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
  public void startLocatorDefaultLoggingConfigWithLogFile() throws Exception {
    Properties config = new Properties();

    locator = InternalLocator.startLocator(port, logFile, null, null, null, false, config, null,
        temporaryFolder.getRoot().toPath());

    LogConfig logConfig = locator.getLogConfig();

    assertThat(logConfig.getName()).isEqualTo("");
    assertThat(logConfig.getLogFile().getAbsolutePath()).isEqualTo(logFile.getAbsolutePath());
    assertThat(logConfig.getLogLevel()).isEqualTo(CONFIG.intLevel());
    assertThat(logConfig.getLogDiskSpaceLimit()).isEqualTo(0);
    assertThat(logConfig.getLogFileSizeLimit()).isEqualTo(0);
    assertThat(logConfig.getSecurityLogFile().getAbsolutePath())
        .isEqualTo(currentWorkingDirPath);
    assertThat(logConfig.getSecurityLogLevel()).isEqualTo(CONFIG.intLevel());
  }

  @Test
  public void startLocatorAndDSCreatesLogFile() throws Exception {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "localhost[" + port + "]");
    config.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    locator = (InternalLocator) Locator.startLocatorAndDS(port, logFile, config);

    await().untilAsserted(() -> assertThat(logFile).exists());

    // assertThat logFile is not empty
    try (FileInputStream fis = new FileInputStream(logFile)) {
      assertThat(fis.available()).isGreaterThan(0);
    }

    InternalDistributedSystem system = (InternalDistributedSystem) locator.getDistributedSystem();

    assertThat(system.getLogWriter()).isInstanceOf(LogWriterLogger.class);
    assertThat(system.getSecurityLogWriter()).isInstanceOf(LogWriterLogger.class);

    LogWriterLogger logWriterLogger = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriterLogger = (LogWriterLogger) system.getSecurityLogWriter();

    assertThat(logWriterLogger.getLogWriterLevel()).isEqualTo(INFO.intLevel());
    assertThat(securityLogWriterLogger.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    DistributionConfig distributionConfig = system.getConfig();

    assertThat(distributionConfig.getLogFile().getAbsolutePath())
        .isEqualTo(logFile.getAbsolutePath());
    assertThat(distributionConfig.getSecurityLogFile().getAbsolutePath())
        .isEqualTo(currentWorkingDirPath);

    assertThat(securityLogFile).doesNotExist();
  }

  @Test
  public void locatorWithNoDS() throws Exception {
    Properties config = new Properties();
    config.setProperty(NAME, name);
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());
    config.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    locator = InternalLocator.startLocator(port, null, null, null, null, false, config, null,
        temporaryFolder.getRoot().toPath());
    Logger logger = LogService.getLogger();

    // assert that logging goes to logFile
    String logMessageBeforeClose = "Logging before locator is stopped in " + name;
    logger.info(logMessageBeforeClose);

    LogFileAssert.assertThat(logFile).contains(logMessageBeforeClose);

    locator.stop();

    // assert that logging stops going to logFile
    String logMessageAfterClose = "Logging after locator is stopped in " + name;
    logger.info(logMessageAfterClose);
    LogFileAssert.assertThat(logFile).doesNotContain(logMessageAfterClose);
  }

  @Test
  public void locatorWithDS() throws Exception {
    Properties config = new Properties();
    config.setProperty(NAME, name);
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());
    config.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    locator = (InternalLocator) InternalLocator.startLocatorAndDS(port, null, config);
    Logger logger = LogService.getLogger();

    // assert that logging goes to logFile
    String logMessageBeforeClose = "Logging before locator is stopped in " + name;
    logger.info(logMessageBeforeClose);

    LogFileAssert.assertThat(logFile).contains(logMessageBeforeClose);

    locator.stop();

    // assert that logging stops going to logFile
    String logMessageAfterClose = "Logging after locator is stopped in " + name;
    logger.info(logMessageAfterClose);
    LogFileAssert.assertThat(logFile).doesNotContain(logMessageAfterClose);
  }
}
