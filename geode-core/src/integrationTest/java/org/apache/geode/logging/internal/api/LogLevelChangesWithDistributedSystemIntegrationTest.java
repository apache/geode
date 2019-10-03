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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.INFO;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.WARNING;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.logging.internal.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for log level control using {@link DistributionConfig} with lifecycle of
 * {@link DistributedSystem}.
 */
@Category(LoggingTest.class)
public class LogLevelChangesWithDistributedSystemIntegrationTest {

  private static final String APPLICATION_LOGGER_NAME = "com.application";

  private InternalDistributedSystem system;
  private Logger geodeLogger;
  private Logger applicationLogger;
  private InternalLogWriter mainLogWriter;
  private InternalLogWriter securityLogWriter;
  private DistributionConfig distributionConfig;
  private LogConfig logConfig;

  @Before
  public void setUp() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_LEVEL, "INFO");
    config.setProperty(SECURITY_LOG_LEVEL, "INFO");

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    distributionConfig = system.getConfig();
    logConfig = system.getLogConfig();

    mainLogWriter = (InternalLogWriter) system.getLogWriter();
    securityLogWriter = (InternalLogWriter) system.getSecurityLogWriter();

    geodeLogger = LogService.getLogger();
    applicationLogger = LogManager.getLogger(APPLICATION_LOGGER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    if (system != null) {
      system.disconnect();
    }
  }

  @Test
  public void distributionConfigLogLevelIsInfo() {
    assertThat(distributionConfig.getLogLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void distributionConfigSecurityLogLevelIsInfo() {
    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void logConfigLogLevelIsInfo() {
    assertThat(logConfig.getLogLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void logConfigSecurityLogLevelIsInfo() {
    assertThat(logConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void mainLogWriterLevelIsInfo() {
    assertThat(mainLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void securityLogWriterLevelIsInfo() {
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void changesMainLogWriterLevel() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    assertThat(mainLogWriter.getLogWriterLevel()).isEqualTo(WARNING.intLevel());
  }

  @Test
  public void changesDistributionConfigLogLevel() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    assertThat(distributionConfig.getLogLevel()).isEqualTo(WARNING.intLevel());
  }

  @Test
  public void changesLogConfigLogLevel() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    assertThat(logConfig.getLogLevel()).isEqualTo(WARNING.intLevel());
  }

  @Test
  public void doesNotChangeSecurityLogWriterLogLevel() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void doesNotChangeDistributionConfigSecurityLogLevel() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    assertThat(distributionConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void doesNotChangeLogConfigSecurityLogLevel() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    assertThat(logConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void changesGeodeLoggerLogLevel() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.WARN);
  }

  @Test
  public void doesNotChangeApplicationLoggerLogLevel() {
    distributionConfig.setLogLevel(WARNING.intLevel());

    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);
  }
}
