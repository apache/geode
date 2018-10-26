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

import static org.apache.geode.internal.logging.Configuration.STARTUP_CONFIGURATION;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.internal.Banner;
import org.apache.geode.internal.logging.Configuration;
import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogConfigSupplier;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link PausableLogWriterAppender} with {@link Banner} and startup
 * configuration. Startup configuration consists of {@link Configuration#STARTUP_CONFIGURATION} and
 * {@link LogConfig#toLoggerString()}.
 */
@Category(LoggingTest.class)
public class PausableLogWriterAppenderWithBannerIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "PausableLogWriterAppenderWithBannerIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "LOGWRITER";

  private static final int BANNER_LOG_EVENT_SIZE = 1;
  private static final int BANNER_LOG_EVENT_INDEX = 0;
  private static final int STARTUP_CONFIG_LOG_EVENT_SIZE = 1;
  private static final int STARTUP_CONFIG_LOG_EVENT_INDEX = 1;
  private static final int LOG_MESSAGE_LOG_EVENT_SIZE = 1;

  private static String configFilePath;

  private PausableLogWriterAppender pausableLogWriterAppender;
  private File logFile;
  private String banner;
  private Logger logger;
  private String logMessage;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @Rule
  public TestName testName = new TestName();

  @BeforeClass
  public static void setUpLogConfigFile() throws Exception {
    URL resource = getResource(CONFIG_FILE_NAME);
    configFilePath = createFileFromResource(resource, temporaryFolder.getRoot(), CONFIG_FILE_NAME)
        .getAbsolutePath();
  }

  @Before
  public void setUp() throws Exception {
    pausableLogWriterAppender = loggerContextRule.getAppender(APPENDER_NAME,
        PausableLogWriterAppender.class);

    String name = testName.getMethodName();
    logFile = new File(temporaryFolder.getRoot(), name + ".log");

    LogConfig config = mock(LogConfig.class);
    when(config.getName()).thenReturn("");
    when(config.getLogFile()).thenReturn(logFile);
    when(config.toLoggerString()).thenReturn("toLoggerString");

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(config);

    pausableLogWriterAppender.createSession(logConfigSupplier);
    pausableLogWriterAppender.startSession();

    banner = Banner.getString(null);

    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() throws Exception {
    pausableLogWriterAppender.clearLogEvents();
  }

  @Test
  public void getLogEventsHasBannerByDefault() {
    assertThat(pausableLogWriterAppender.getLogEvents())
        .hasSize(BANNER_LOG_EVENT_SIZE + STARTUP_CONFIG_LOG_EVENT_SIZE);
    LogEvent logEvent = pausableLogWriterAppender.getLogEvents().get(BANNER_LOG_EVENT_INDEX);
    assertThat(logEvent.getMessage().getFormattedMessage()).contains(banner);
  }

  @Test
  public void getLogEventsHasStartupConfigurationByDefault() {
    assertThat(pausableLogWriterAppender.getLogEvents())
        .hasSize(BANNER_LOG_EVENT_SIZE + STARTUP_CONFIG_LOG_EVENT_SIZE);
    LogEvent logEvent =
        pausableLogWriterAppender.getLogEvents().get(STARTUP_CONFIG_LOG_EVENT_INDEX);
    assertThat(logEvent.getMessage().getFormattedMessage()).contains(STARTUP_CONFIGURATION);
  }

  @Test
  public void startupConfigurationIsLoggedToFile() throws Exception {
    assertThat(logFile).exists();
    String content = FileUtils.readFileToString(logFile, Charset.defaultCharset()).trim();
    assertThat(content).contains(STARTUP_CONFIGURATION);
  }

  @Test
  public void startupConfigurationIsLoggedBeforeLogMessages() throws Exception {
    logger.info(logMessage);

    assertThat(pausableLogWriterAppender.getLogEvents()).hasSize(
        BANNER_LOG_EVENT_SIZE + STARTUP_CONFIG_LOG_EVENT_SIZE + LOG_MESSAGE_LOG_EVENT_SIZE);
    assertThat(logFile).exists();

    List<String> lines = FileUtils.readLines(logFile, Charset.defaultCharset());
    boolean foundStartupConfiguration = false;
    boolean foundLogMessage = false;
    for (String line : lines) {
      if (line.contains(STARTUP_CONFIGURATION)) {
        foundStartupConfiguration = true;
      }
      if (line.contains(logMessage)) {
        assertThat(foundStartupConfiguration)
            .as("Log message should be after startup configuration: " + lines).isTrue();
        foundLogMessage = true;
      }
    }

    assertThat(foundStartupConfiguration).as("Startup configuration not found in: " + lines)
        .isTrue();
    assertThat(foundLogMessage).as("Log message not found in: " + lines).isTrue();
  }
}
