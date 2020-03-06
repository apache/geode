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
package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.INFO;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.WARNING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Integration tests for {@link ChangeLogLevelFunction}.
 */
@Category(LoggingTest.class)
public class ChangeLogLevelFunctionIntegrationTest {

  private static final String APPLICATION_LOGGER_NAME = "com.application";

  private InternalCache cache;
  private Logger geodeLogger;
  private Logger applicationLogger;
  @SuppressWarnings("deprecation")
  private org.apache.geode.internal.logging.InternalLogWriter mainLogWriter;
  @SuppressWarnings("deprecation")
  private org.apache.geode.internal.logging.InternalLogWriter securityLogWriter;
  private LogConfig logConfig;
  private FunctionContext<Object[]> functionContext;

  private ChangeLogLevelFunction changeLogLevelFunction;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_LEVEL, Level.INFO.name());
    config.setProperty(SECURITY_LOG_LEVEL, Level.INFO.name());

    cache = (InternalCache) new CacheFactory(config).create();
    InternalDistributedSystem system = cache.getInternalDistributedSystem();

    setupInternalLogWriter();

    geodeLogger = LogService.getLogger();
    applicationLogger = LogManager.getLogger(APPLICATION_LOGGER_NAME);

    logConfig = system.getLogConfig();
    assertThat(logConfig.getLogLevel()).isEqualTo(INFO.intLevel());
    assertThat(logConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());

    functionContext = mock(FunctionContext.class);
    when(functionContext.getCache()).thenReturn(cache);
    when(functionContext.getArguments()).thenReturn(new Object[] {Level.WARN.name()});
    when(functionContext.getResultSender()).thenReturn(mock(ResultSender.class));

    changeLogLevelFunction = new ChangeLogLevelFunction();
  }

  @SuppressWarnings("deprecation")
  private void setupInternalLogWriter() {
    mainLogWriter = (org.apache.geode.internal.logging.InternalLogWriter) cache.getLogger();
    assertThat(mainLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());

    securityLogWriter =
        (org.apache.geode.internal.logging.InternalLogWriter) cache.getSecurityLogger();
    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void changesMainLogWriterLevel() {
    changeLogLevelFunction.execute(functionContext);

    assertThat(mainLogWriter.getLogWriterLevel()).isEqualTo(WARNING.intLevel());
  }

  @Test
  public void changesLogConfigLogLevel() {
    changeLogLevelFunction.execute(functionContext);

    assertThat(logConfig.getLogLevel()).isEqualTo(WARNING.intLevel());
  }

  @Test
  public void doesNotChangeSecurityLogWriterLogLevel() {
    changeLogLevelFunction.execute(functionContext);

    assertThat(securityLogWriter.getLogWriterLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void doesNotChangeLogConfigSecurityLogLevel() {
    changeLogLevelFunction.execute(functionContext);

    assertThat(logConfig.getSecurityLogLevel()).isEqualTo(INFO.intLevel());
  }

  @Test
  public void changesGeodeLoggerLogLevel() {
    changeLogLevelFunction.execute(functionContext);

    assertThat(geodeLogger.getLevel()).isEqualTo(Level.WARN);
  }

  @Test
  public void doesNotChangeApplicationLoggerLogLevel() {
    changeLogLevelFunction.execute(functionContext);

    assertThat(applicationLogger.getLevel()).isEqualTo(Level.INFO);
  }

  @Test
  public void changesGemFireLogLevelSystemProperty() {
    changeLogLevelFunction.execute(functionContext);

    assertThat(System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + LOG_LEVEL))
        .isEqualTo(Level.WARN.name());
  }

  @Test
  public void doesNotChangeGemFireSecurityLogLevelSystemProperty() {
    changeLogLevelFunction.execute(functionContext);

    assertThat(System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + SECURITY_LOG_LEVEL)).isNull();
  }
}
