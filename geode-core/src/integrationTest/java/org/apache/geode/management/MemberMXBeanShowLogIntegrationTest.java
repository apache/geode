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
package org.apache.geode.management;

import static java.lang.System.lineSeparator;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.management.internal.ManagementConstants.DEFAULT_SHOW_LOG_LINES;
import static org.apache.geode.management.internal.ManagementConstants.MAX_SHOW_LOG_LINES;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.ManagementTest;

/**
 * Integration tests for {@link MemberMXBean} with just a {@link Cache}.
 */
@Category({ManagementTest.class, LoggingTest.class})
public class MemberMXBeanShowLogIntegrationTest {

  private File logFile;
  private InternalCache cache;
  private SystemManagementService managementService;
  private Logger logger;
  private String logMessage;

  private MemberMXBean memberMXBean;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    logFile = new File(temporaryFolder.getRoot(), testName.getMethodName() + ".log");
    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void showLogWithoutLogFile() {
    createCacheWithoutLogFile();

    assertThat(memberMXBean.showLog(0)).isEqualTo(
        "No log file was specified in the configuration, messages will be directed to stdout.");
  }

  @Test
  public void showLogIsNeverEmpty() {
    createCacheWithLogFile();

    assertThat(memberMXBean.showLog(-20)).isNotEmpty();
    assertThat(memberMXBean.showLog(0)).isNotEmpty();
    assertThat(memberMXBean.showLog(DEFAULT_SHOW_LOG_LINES)).isNotEmpty();
    assertThat(memberMXBean.showLog(MAX_SHOW_LOG_LINES)).isNotEmpty();
  }

  @Test
  public void showLogZeroUsesDefault() {
    createCacheWithLogFile();

    String log = memberMXBean.showLog(0);

    // splitting on lineSeparator() results in a length near 30
    assertThat(log.split(lineSeparator()).length).as("Log: " + log).isGreaterThan(25)
        .isLessThan(35);
  }

  @Test
  public void showLogNegativeUsesDefault() {
    createCacheWithLogFile();

    String log = memberMXBean.showLog(-20);

    // splitting on lineSeparator() results in a length near 30
    assertThat(log.split(lineSeparator()).length).as("Log: " + log).isGreaterThan(25)
        .isLessThan(35);
  }

  @Test
  public void showLogDefault() {
    createCacheWithLogFile();

    String log = memberMXBean.showLog(DEFAULT_SHOW_LOG_LINES);

    // splitting on lineSeparator() results in a length near 30
    assertThat(log.split(lineSeparator()).length).as("Log: " + log).isGreaterThan(25)
        .isLessThan(35);
  }

  @Test
  public void showLogMaxLinesCount() {
    createCacheWithLogFile();

    String log = memberMXBean.showLog(MAX_SHOW_LOG_LINES);

    // splitting on lineSeparator() results in a length near 100
    assertThat(log.split(lineSeparator()).length).as("Log: " + log).isGreaterThan(90)
        .isLessThan(110);
  }

  @Test
  public void showLogGreaterThanMaxUsesMax() {
    createCacheWithLogFile();

    String log = memberMXBean.showLog(MAX_SHOW_LOG_LINES * 10);

    // splitting on lineSeparator() results in a length near 100
    assertThat(log.split(lineSeparator()).length).as("Log: " + log).isGreaterThan(90)
        .isLessThan(110);
  }

  @Test
  public void showLogContainsMostRecentlyLoggedMessage() {
    createCacheWithLogFile();
    logger.info(logMessage);

    String log = memberMXBean.showLog(0);

    assertThat(log).contains(logMessage);
  }

  private void createCacheWithLogFile() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, logFile.getAbsolutePath());

    cache = (InternalCache) new CacheFactory(config).create();

    managementService =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    await().until(() -> managementService.getMemberMXBean() != null);
    memberMXBean = managementService.getMemberMXBean();
    assertThat(memberMXBean).isNotNull();
  }

  private void createCacheWithoutLogFile() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");

    cache = (InternalCache) new CacheFactory(config).create();

    managementService =
        (SystemManagementService) ManagementService.getExistingManagementService(cache);
    await().until(() -> managementService.getMemberMXBean() != null);
    memberMXBean = managementService.getMemberMXBean();
    assertThat(memberMXBean).isNotNull();
  }
}
