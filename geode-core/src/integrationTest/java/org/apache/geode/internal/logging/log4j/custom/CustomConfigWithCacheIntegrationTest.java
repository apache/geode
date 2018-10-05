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
package org.apache.geode.internal.logging.log4j.custom;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.status.StatusLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.Configurator;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests with custom log4j2 configuration.
 */
@Category(LoggingTest.class)
public class CustomConfigWithCacheIntegrationTest {

  private static final String CONFIG_LAYOUT_PREFIX = "CUSTOM";
  private static final String CUSTOM_REGEX_STRING =
      "CUSTOM: level=[A-Z]+ time=\\d{4}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} [^ ]{3} message=.*[\\n]+throwable=.*$";
  private static final Pattern CUSTOM_REGEX = Pattern.compile(CUSTOM_REGEX_STRING, Pattern.DOTALL);

  private String beforeConfigFileProp;
  private Level beforeLevel;

  private Cache cache;

  @Rule
  public SystemErrRule systemErrRule = new SystemErrRule().enableLog();

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {

    Configurator.shutdown();
    BasicAppender.clearInstance();

    beforeConfigFileProp =
        System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    beforeLevel = StatusLogger.getLogger().getLevel();

    String configFileName = getClass().getSimpleName() + "_log4j2.xml";
    File customConfigFile = createFileFromResource(getResource(configFileName),
        temporaryFolder.getRoot(), configFileName);

    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY,
        customConfigFile.getAbsolutePath());

    Properties gemfireProperties = new Properties();
    gemfireProperties.put(LOCATORS, "");
    gemfireProperties.put(MCAST_PORT, "0");
    gemfireProperties.put(LOG_LEVEL, "info");
    cache = new CacheFactory(gemfireProperties).create();
  }

  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.getDistributedSystem().disconnect();
    }

    Configurator.shutdown();

    System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    if (beforeConfigFileProp != null) {
      System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY,
          beforeConfigFileProp);
    }
    StatusLogger.getLogger().setLevel(beforeLevel);

    LogService.reconfigure();
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
        .isTrue();

    BasicAppender.clearInstance();

    assertThat(systemErrRule.getLog()).isEmpty();
  }

  @Test
  public void cacheLogWriterMessageShouldMatchCustomConfig() {
    String logLogger = LogService.MAIN_LOGGER_NAME;
    Level logLevel = Level.INFO;
    String logMessage = "this is a log statement from " + testName.getMethodName();

    LogWriter logger = cache.getLogger();
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
        .isFalse();

    BasicAppender.clearEvents();

    logger.info(logMessage);

    BasicAppender appender = BasicAppender.getInstance();
    assertThat(appender).isNotNull();
    assertThat(appender.events().size()).isGreaterThan(0);

    LogEvent event = null;
    for (LogEvent logEvent : appender.events()) {
      if (logEvent.getMessage().getFormattedMessage().contains(logMessage)) {
        event = logEvent;
        break;
      }
    }

    assertThat(event).isNotNull();

    assertThat(event.getLoggerName()).isEqualTo(logLogger);
    assertThat(event.getLevel()).isEqualTo(logLevel);
    assertThat(event.getMessage().getFormattedMessage()).isEqualTo(logMessage);

    assertThat(systemOutRule.getLog()).contains(logLevel.name());
    assertThat(systemOutRule.getLog()).contains(logMessage);
    assertThat(systemOutRule.getLog()).contains(CONFIG_LAYOUT_PREFIX);
    assertThat(CUSTOM_REGEX.matcher(systemOutRule.getLog()).matches()).isTrue();
  }
}
