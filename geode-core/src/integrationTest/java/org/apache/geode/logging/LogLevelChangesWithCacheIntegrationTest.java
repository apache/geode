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
package org.apache.geode.logging;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.logging.spi.LogWriterLevel.INFO;
import static org.apache.geode.logging.spi.LogWriterLevel.WARNING;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.logging.spi.LogConfig;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for log level control using {@link DistributionConfig} with lifecycle of
 * {@link Cache}.
 */
@Category(LoggingTest.class)
public class LogLevelChangesWithCacheIntegrationTest {

  private static final String APPLICATION_LOGGER_NAME = "com.application";

  private InternalCache cache;
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

    cache = (InternalCache) new CacheFactory(config).create();
    InternalDistributedSystem system = cache.getInternalDistributedSystem();

    distributionConfig = system.getConfig();
    logConfig = system.getLogConfig();

    mainLogWriter = (InternalLogWriter) cache.getLogger();
    securityLogWriter = (InternalLogWriter) cache.getSecurityLogger();

    geodeLogger = LogService.getLogger();
    applicationLogger = LogManager.getLogger(APPLICATION_LOGGER_NAME);
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void mainLogWriterLogLevelIsInfo() {
    assertThat(mainLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void securityLogWriterLogLevelIsInfo() {
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
