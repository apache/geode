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

import static org.apache.geode.logging.log4j.internal.impl.Log4jLoggingProvider.getConfigurationInfoString;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link LogService} with custom logging configuration.
 */
@Category(LoggingTest.class)
public class LogServiceWithCustomLogConfigIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "LogServiceWithCustomLogConfigIntegrationTest_log4j2.xml";
  private static final String CONFIG_LAYOUT_PREFIX = "CUSTOM";
  private static final String CUSTOM_REGEX_STRING =
      "CUSTOM: level=[A-Z]+ time=\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3} [^ ]{3} message=.*[\\n]+throwable=.*$";
  private static final Pattern CUSTOM_REGEX = Pattern.compile(CUSTOM_REGEX_STRING, Pattern.DOTALL);

  private static String configFilePath;

  private Logger logger;
  private String logMessage;
  private ListAppender listAppender;

  @ClassRule
  public static SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @ClassRule
  public static SystemErrRule systemErrRule = new SystemErrRule().enableLog();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpLogConfigFile() {
    URL resource = getResource(CONFIG_FILE_NAME);
    configFilePath = createFileFromResource(resource, temporaryFolder.getRoot(), CONFIG_FILE_NAME)
        .getAbsolutePath();
  }

  @Before
  public void setUp() {
    systemOutRule.clearLog();
    systemErrRule.clearLog();

    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
    listAppender = loggerContextRule.getListAppender("CUSTOM");
  }

  @Test
  public void isUsingGemFireDefaultConfigIsFalse() {
    assertThat(Log4jLoggingProvider.isUsingGemFireDefaultConfig()).as(getConfigurationInfoString())
        .isFalse();
  }

  @Test
  public void logEventShouldMatchCustomConfig() {
    logger.info(logMessage);

    LogEvent logEvent = listAppender.getEvents().get(0);
    assertThat(logEvent.getLoggerName()).isEqualTo(logger.getName());
    assertThat(logEvent.getLevel()).isEqualTo(Level.INFO);
    assertThat(logEvent.getMessage().getFormattedMessage()).isEqualTo(logMessage);

    assertThat(systemOutRule.getLog()).contains(Level.INFO.name());
    assertThat(systemOutRule.getLog()).contains(logMessage);
    assertThat(systemOutRule.getLog()).contains(CONFIG_LAYOUT_PREFIX);
    assertThat(CUSTOM_REGEX.matcher(systemOutRule.getLog()).matches()).isTrue();
  }
}
