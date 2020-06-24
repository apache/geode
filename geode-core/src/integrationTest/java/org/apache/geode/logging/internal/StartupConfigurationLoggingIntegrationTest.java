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
package org.apache.geode.logging.internal;

import static java.lang.System.lineSeparator;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.logging.internal.Configuration.STARTUP_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.Banner;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for logging of the startup configuration. Startup configuration consists of
 * {@link Configuration#STARTUP_CONFIGURATION} and {@link DistributionConfig#toLoggerString()}.
 */
@Category(LoggingTest.class)
public class StartupConfigurationLoggingIntegrationTest {

  private File mainLogFile;
  private InternalDistributedSystem system;
  private String banner;
  private String[] startupConfiguration;
  private Logger geodeLogger;
  private String logMessage;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    String name = testName.getMethodName();
    mainLogFile = new File(temporaryFolder.getRoot(), name + "-main.log");

    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFile.getAbsolutePath());
    config.setProperty(NAME, getClass().getSimpleName() + "_" + testName.getMethodName());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    banner = new Banner(new ServiceLoaderModuleService(LogService.getLogger())).getString();

    DistributionConfig distributionConfig = system.getConfig();
    startupConfiguration = StringUtils
        .split(STARTUP_CONFIGURATION + lineSeparator() + distributionConfig.toLoggerString(),
            lineSeparator());

    geodeLogger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() throws Exception {
    if (system != null) {
      system.disconnect();
    }
  }

  @Test
  public void startupConfigurationIsLoggedToFile() {
    LogFileAssert.assertThat(mainLogFile).contains(startupConfiguration);
  }

  @Test
  public void startupConfigurationIsLoggedToFileOnlyOnce() {
    LogFileAssert.assertThat(mainLogFile).containsOnlyOnce(startupConfiguration);
  }

  @Test
  public void startupConfigurationIsLoggedToFileAfterBanner() throws Exception {
    List<String> bannerLines = Arrays.asList(StringUtils.split(banner, lineSeparator()));
    List<String> startupConfigLines = Arrays.asList(startupConfiguration);
    List<String> logLines = FileUtils.readLines(mainLogFile, Charset.defaultCharset());

    boolean foundBanner = false;
    boolean foundStartupConfig = false;

    for (String line : logLines) {
      if (bannerLines.contains(line)) {
        assertThat(foundStartupConfig)
            .as("Banner should be logged before startup configuration: " + logLines).isFalse();
        foundBanner = true;
      }
      if (startupConfigLines.contains(line)) {
        assertThat(foundBanner)
            .as("Startup configuration should be logged before banner: " + logLines).isTrue();
        foundStartupConfig = true;
      }
    }

    assertThat(foundBanner).as("Banner not found in: " + logLines).isTrue();
    assertThat(foundStartupConfig).as("Startup configuration not found in: " + logLines).isTrue();
  }

  @Test
  public void startupConfigurationIsLoggedToFileBeforeLogMessage() throws Exception {
    geodeLogger.info(logMessage);

    List<String> startupConfigLines = Arrays.asList(startupConfiguration);
    List<String> logLines = FileUtils.readLines(mainLogFile, Charset.defaultCharset());

    boolean foundStartupConfig = false;
    boolean foundLogMessage = false;

    for (String line : logLines) {
      if (startupConfigLines.contains(line)) {
        assertThat(foundLogMessage)
            .as("Startup configuration should be before log message: " + logLines).isFalse();
        foundStartupConfig = true;
      }
      if (line.contains(logMessage)) {
        assertThat(foundStartupConfig)
            .as("Log message should be after startup configuration: " + logLines).isTrue();
        foundLogMessage = true;
      }
    }

    assertThat(foundStartupConfig).as("Startup configuration not found in: " + logLines).isTrue();
    assertThat(foundLogMessage).as("Log message not found in: " + logLines).isTrue();
  }
}
