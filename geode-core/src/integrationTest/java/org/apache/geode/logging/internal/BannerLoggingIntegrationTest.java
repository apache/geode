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

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.commons.io.FileUtils.readLines;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.internal.logging.Banner.BannerHeader.displayValues;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.logging.Banner;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.logging.LogFileAssert;

/**
 * Integration tests for logging of the {@link Banner}.
 */
@Category(LoggingTest.class)
public class BannerLoggingIntegrationTest {

  private File mainLogFile;
  private InternalDistributedSystem system;
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

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

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
  public void bannerIsLoggedToFile() {
    LogFileAssert.assertThat(mainLogFile).contains(displayValues());
  }

  @Test
  public void bannerIsLoggedToFileOnlyOnce() {
    LogFileAssert.assertThat(mainLogFile).containsOnlyOnce(displayValues());
  }

  /**
   * Verifies that the banner is logged before any log messages.
   */
  @Test
  public void bannerIsLoggedToFileBeforeLogMessage() throws Exception {
    geodeLogger.info(logMessage);

    List<String> lines = readLines(mainLogFile, defaultCharset());

    boolean foundBanner = false;
    boolean foundLogMessage = false;

    for (String line : lines) {
      if (containsAny(line, displayValues())) {
        assertThat(foundLogMessage).as("Banner should be logged before log message: " + lines)
            .isFalse();
        foundBanner = true;
      }
      if (line.contains(logMessage)) {
        assertThat(foundBanner).as("Log message should be logged after banner: " + lines)
            .isTrue();
        foundLogMessage = true;
      }
    }

    assertThat(foundBanner).as("Banner not found in: " + lines).isTrue();
    assertThat(foundLogMessage).as("Log message not found in: " + lines).isTrue();
  }

  private boolean containsAny(String string, String... values) {
    for (String value : values) {
      if (string.contains(value)) {
        return true;
      }
    }
    return false;
  }
}
