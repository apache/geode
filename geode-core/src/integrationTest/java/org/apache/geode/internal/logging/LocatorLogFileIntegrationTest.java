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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

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
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.logging.log4j.LogWriterLogger;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Creates Locator and tests logging behavior at a high level.
 */
@Category(LoggingTest.class)
public class LocatorLogFileIntegrationTest {

  private File logFile;
  private String logFileName;
  private File securityLogFile;

  private int port;
  private Locator locator;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    logFile = new File(temporaryFolder.getRoot(),
        testName.getMethodName() + "-system-" + System.currentTimeMillis() + ".log");
    logFileName = logFile.getAbsolutePath();

    securityLogFile = new File("");

    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
  }

  @After
  public void tearDown() throws Exception {
    if (locator != null) {
      locator.stop();
    }
  }

  @Test
  public void testLocatorCreatesLogFile() throws Exception {
    Properties config = new Properties();
    config.put(LOG_LEVEL, "config");
    config.put(MCAST_PORT, "0");
    config.put(LOCATORS, "localhost[" + port + "]");
    config.put(ENABLE_CLUSTER_CONFIGURATION, "false");

    locator = Locator.startLocatorAndDS(port, logFile, config);

    InternalDistributedSystem system = (InternalDistributedSystem) locator.getDistributedSystem();

    await().untilAsserted(() -> assertThat(logFile).exists());

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
        .isEqualTo(securityLogFile.getAbsolutePath());

    assertThat(system.getLogWriter()).isInstanceOf(LogWriterLogger.class);
    assertThat(system.getSecurityLogWriter()).isInstanceOf(LogWriterLogger.class);

    LogWriterLogger logWriterLogger = (LogWriterLogger) system.getLogWriter();
    LogWriterLogger securityLogWriterLogger = (LogWriterLogger) system.getSecurityLogWriter();

    assertThat(logWriterLogger.getLogWriterLevel())
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());
    assertThat(securityLogWriterLogger.getLogWriterLevel())
        .isEqualTo(LogWriterLevel.INFO.getLogWriterLevel());

    assertThat(securityLogFile).doesNotExist();
  }
}
