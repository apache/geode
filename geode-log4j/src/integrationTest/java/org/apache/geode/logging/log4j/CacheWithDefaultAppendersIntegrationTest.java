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
package org.apache.geode.logging.log4j;

import static org.apache.geode.alerting.log4j.Log4jAlertingProvider.GEODE_ALERT_APPENDER_NAME;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.logging.log4j.Log4jLoggingProvider.GEODE_CONSOLE_APPENDER_NAME;
import static org.apache.geode.logging.log4j.Log4jLoggingProvider.LOGWRITER_APPENDER_NAME;
import static org.apache.geode.logging.log4j.Log4jLoggingProvider.SECURITY_LOGWRITER_APPENDER_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.alerting.log4j.AlertAppender;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Validates the default appenders that should exist when a {@code Cache} is created.
 */
@Category(LoggingTest.class)
public class CacheWithDefaultAppendersIntegrationTest {

  private InternalCache cache;
  private Configuration configuration;

  @Before
  public void setUp() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");

    cache = (InternalCache) new CacheFactory(config).create();

    Logger coreLogger = (Logger) LogManager.getRootLogger();
    LoggerContext context = coreLogger.getContext();

    configuration = context.getConfiguration();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void hasGeodeConsoleAppenderNamed_GEODE_CONSOLE_APPENDER_NAME() {
    Appender appender = configuration.getAppender(GEODE_CONSOLE_APPENDER_NAME);

    assertThat(appender).isNotNull().isInstanceOf(GeodeConsoleAppender.class);
  }

  @Test
  public void hasAlertAppenderNamed_ALERT_APPENDER_NAME() {
    Appender appender = configuration.getAppender(GEODE_ALERT_APPENDER_NAME);

    assertThat(appender).isNotNull().isInstanceOf(AlertAppender.class);
  }

  @Test
  public void hasLogWriterAppenderNamed_LOGWRITER_APPENDER_NAME() {
    Appender appender = configuration.getAppender(LOGWRITER_APPENDER_NAME);

    assertThat(appender).isNotNull().isInstanceOf(LogWriterAppender.class);
  }

  @Test
  public void hasLogWriterAppenderNamed_SECURITY_LOGWRITER_APPENDER_NAME() {
    Appender appender = configuration.getAppender(SECURITY_LOGWRITER_APPENDER_NAME);

    assertThat(appender).isNotNull().isInstanceOf(LogWriterAppender.class);
  }
}
