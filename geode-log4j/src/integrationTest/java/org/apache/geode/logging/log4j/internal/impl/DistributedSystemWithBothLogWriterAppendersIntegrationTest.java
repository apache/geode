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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_FILE;
import static org.apache.geode.logging.internal.spi.LoggingProvider.SECURITY_LOGGER_NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;

import java.io.File;
import java.net.URL;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
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

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for logging of main and security {@code Logger}s and
 * {@link org.apache.geode.LogWriter}s to
 * {@link ConfigurationProperties#LOG_FILE} and {@link ConfigurationProperties#SECURITY_LOG_FILE}
 * with {@link DistributedSystem}.
 */
@Category(LoggingTest.class)
@SuppressWarnings("deprecation")
public class DistributedSystemWithBothLogWriterAppendersIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "DistributedSystemWithBothLogWriterAppendersIntegrationTest_log4j2.xml";

  private static String configFilePath;

  private File mainLogFile;
  private File securityLogFile;
  private InternalDistributedSystem system;
  private Logger mainLogger;
  private Logger securityLogger;
  private org.apache.geode.LogWriter mainLogWriter;
  private org.apache.geode.LogWriter securityLogWriter;
  private String logMessage;

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
    String name = testName.getMethodName();
    mainLogFile = new File(temporaryFolder.getRoot(), name + "-main.log");
    securityLogFile = new File(temporaryFolder.getRoot(), name + "-security.log");

    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(LOG_FILE, mainLogFile.getAbsolutePath());
    config.setProperty(SECURITY_LOG_FILE, securityLogFile.getAbsolutePath());

    system = (InternalDistributedSystem) DistributedSystem.connect(config);

    mainLogger = LogService.getLogger();
    securityLogger = LogService.getLogger(SECURITY_LOGGER_NAME);

    mainLogWriter = system.getLogWriter();
    securityLogWriter = system.getSecurityLogWriter();

    mainLogger.info("Starting {}", getClass().getName());
    securityLogger.info("Starting {}", getClass().getName());

    await().until(() -> mainLogFile.exists());
    await().until(() -> securityLogFile.exists());

    logMessage = "Logging " + testName.getMethodName();
  }

  @After
  public void tearDown() {
    system.disconnect();
  }

  @Test
  public void mainLogger_debug_notLoggedByDefault() {
    mainLogger.debug(logMessage);

    LogFileAssert.assertThat(mainLogFile).doesNotContain(logMessage);
  }

  @Test
  public void mainLogger_logsTo_mainLogFile() {
    mainLogger.info(logMessage);

    LogFileAssert.assertThat(mainLogFile).contains(logMessage);
  }

  @Test
  public void mainLogger_doesNotLogTo_securityLogFile() {
    mainLogger.info(logMessage);

    LogFileAssert.assertThat(securityLogFile).doesNotContain(logMessage);
  }

  @Test
  public void securityLogger_debug_notLoggedByDefault() {
    securityLogger.debug(logMessage);

    LogFileAssert.assertThat(securityLogFile).doesNotContain(logMessage);
  }

  @Test
  public void securityLogger_logsTo_securityLogFile() {
    securityLogger.info(logMessage);

    LogFileAssert.assertThat(securityLogFile).contains(logMessage);
  }

  @Test
  public void securityLogger_doesNotLogTo_mainLogFile() {
    securityLogger.info(logMessage);

    LogFileAssert.assertThat(mainLogFile).doesNotContain(logMessage);
  }

  @Test
  public void mainLogWriter_fine_notLoggedByDefault() {
    mainLogWriter.fine(logMessage);

    LogFileAssert.assertThat(mainLogFile).doesNotContain(logMessage);
  }

  @Test
  public void mainLogWriter_logsTo_mainLogFile() {
    mainLogWriter.info(logMessage);

    LogFileAssert.assertThat(mainLogFile).contains(logMessage);
  }

  @Test
  public void mainLogWriter_doesNotLogTo_securityLogFile() {
    mainLogWriter.info(logMessage);

    LogFileAssert.assertThat(securityLogFile).doesNotContain(logMessage);
  }

  @Test
  public void securityLogWriter_fine_notLoggedByDefault() {
    securityLogWriter.fine(logMessage);

    LogFileAssert.assertThat(securityLogFile).doesNotContain(logMessage);
  }

  @Test
  public void securityLogWriter_logsTo_securityLogFile() {
    securityLogWriter.info(logMessage);

    LogFileAssert.assertThat(securityLogFile).contains(logMessage);
  }

  @Test
  public void securityLogWriter_doesNotLogTo_mainLogFile() {
    securityLogWriter.info(logMessage);

    LogFileAssert.assertThat(mainLogFile).doesNotContain(logMessage);
  }
}
