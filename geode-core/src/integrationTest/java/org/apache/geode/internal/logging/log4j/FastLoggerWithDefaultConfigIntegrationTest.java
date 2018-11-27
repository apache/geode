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
package org.apache.geode.internal.logging.log4j;

import static org.apache.geode.internal.logging.LogWriterLevel.INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.Configuration;
import org.apache.geode.internal.logging.Configuration.LogLevelUpdateOccurs;
import org.apache.geode.internal.logging.Configuration.LogLevelUpdateScope;
import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogConfigSupplier;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link FastLogger} when using the default {@code log4j2.xml} for Geode.
 */
@Category(LoggingTest.class)
public class FastLoggerWithDefaultConfigIntegrationTest {

  private Logger logger;

  @Before
  public void setUp() throws Exception {
    LogConfig logConfig = mock(LogConfig.class);
    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);

    when(logConfig.getLogLevel()).thenReturn(INFO.intLevel());
    when(logConfig.getSecurityLogLevel()).thenReturn(INFO.intLevel());
    when(logConfigSupplier.getLogConfig()).thenReturn(logConfig);

    Configuration configuration =
        Configuration.create(LogLevelUpdateOccurs.ALWAYS, LogLevelUpdateScope.GEODE_LOGGERS);
    configuration.initialize(logConfigSupplier);
  }

  /**
   * LogService isUsingGemFireDefaultConfig should be true
   */
  @Test
  public void isUsingGemFireDefaultConfig() {
    assertThat(Log4jAgent.isUsingGemFireDefaultConfig()).isTrue();
  }

  /**
   * LogService getLogger should return loggers wrapped in FastLogger
   */
  @Test
  public void logServiceReturnsFastLoggers() {
    logger = LogService.getLogger();

    assertThat(logger).isInstanceOf(FastLogger.class);
  }

  /**
   * FastLogger isDelegating should be false
   */
  @Test
  public void isDelegatingShouldBeFalse() {
    logger = LogService.getLogger();

    assertThat(((FastLogger) logger).isDelegating()).isFalse();
  }
}
