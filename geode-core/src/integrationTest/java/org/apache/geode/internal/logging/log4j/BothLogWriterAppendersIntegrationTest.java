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

import static org.apache.geode.internal.logging.Configuration.MAIN_LOGGER_NAME;
import static org.apache.geode.internal.logging.Configuration.SECURITY_LOGGER_NAME;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;

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
import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogConfigSupplier;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.statistics.StatisticsConfig;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Integration tests for main and security {@link PausableLogWriterAppender}s, loggers, and
 * {@link ConfigurationProperties#LOG_FILE} and {@link ConfigurationProperties#SECURITY_LOG_FILE}.
 */
@Category({LoggingTest.class, SecurityTest.class})
public class BothLogWriterAppendersIntegrationTest {

  private static final String CONFIG_FILE_NAME = "BothLogWriterAppendersIntegrationTest_log4j2.xml";
  private static final String MAIN_APPENDER_NAME = "LOGWRITER";
  private static final String SECURITY_APPENDER_NAME = "SECURITYLOGWRITER";

  private static String configFilePath;

  private PausableLogWriterAppender mainLogWriterAppender;
  private PausableLogWriterAppender securityLogWriterAppender;
  private Logger mainGeodeLogger;
  private Logger securityGeodeLogger;
  private File securityLogFile;
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
    mainLogWriterAppender = loggerContextRule.getAppender(MAIN_APPENDER_NAME,
        PausableLogWriterAppender.class);
    securityLogWriterAppender = loggerContextRule.getAppender(SECURITY_APPENDER_NAME,
        PausableLogWriterAppender.class);

    String name = testName.getMethodName();
    File mainLogFile = new File(temporaryFolder.getRoot(), name + ".log");
    securityLogFile = new File(temporaryFolder.getRoot(), name + "-security.log");

    LogConfig mainLogConfig = mock(LogConfig.class);
    when(mainLogConfig.getName()).thenReturn(name);
    when(mainLogConfig.getLogFile()).thenReturn(mainLogFile);

    LogConfigSupplier mainLogConfigSupplier = mock(LogConfigSupplier.class);
    when(mainLogConfigSupplier.getLogConfig()).thenReturn(mainLogConfig);
    when(mainLogConfigSupplier.getStatisticsConfig()).thenReturn(mock(StatisticsConfig.class));

    LogConfig securityLogConfig = mock(LogConfig.class);
    when(securityLogConfig.getName()).thenReturn(name);
    when(securityLogConfig.getLogFile()).thenReturn(securityLogFile);

    LogConfigSupplier securityLogConfigSupplier = mock(LogConfigSupplier.class);
    when(securityLogConfigSupplier.getLogConfig()).thenReturn(securityLogConfig);
    when(securityLogConfigSupplier.getStatisticsConfig()).thenReturn(mock(StatisticsConfig.class));

    mainLogWriterAppender.createSession(mainLogConfigSupplier);
    securityLogWriterAppender.createSession(securityLogConfigSupplier);

    mainGeodeLogger = LogService.getLogger(MAIN_LOGGER_NAME);
    securityGeodeLogger = LogService.getLogger(SECURITY_LOGGER_NAME);

    mainLogWriterAppender.createSession(mainLogConfigSupplier);
    mainLogWriterAppender.startSession();

    securityLogWriterAppender.createSession(securityLogConfigSupplier);
    securityLogWriterAppender.startSession();

    logMessage = "Logging in " + testName.getMethodName();

    mainLogWriterAppender.clearLogEvents();
    securityLogWriterAppender.clearLogEvents();
  }

  @After
  public void tearDown() throws Exception {
    mainLogWriterAppender.stopSession();
    securityLogWriterAppender.stopSession();
  }

  @Test
  public void mainLogWriterAppenderLogEventsIsEmptyByDefault() {
    assertThat(mainLogWriterAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void securityLogWriterAppenderLogEventsIsEmptyByDefault() {
    assertThat(securityLogWriterAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void geodeLoggerAppendsToLogWriterAppender() {
    mainGeodeLogger.info(logMessage);

    assertThat(mainLogWriterAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void geodeLoggerDoesNotAppendToSecurityLogWriterAppender() {
    mainGeodeLogger.info(logMessage);

    assertThat(securityLogWriterAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void geodeSecurityLoggerAppendsToSecurityLogWriterAppender() {
    securityGeodeLogger.info(logMessage);

    assertThat(securityLogWriterAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void geodeSecurityLoggerDoesNotAppendToLogWriterAppender() {
    securityGeodeLogger.info(logMessage);

    assertThat(mainLogWriterAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void securityGeodeLoggerLogsToSecurityLogFile() {
    securityGeodeLogger.info(logMessage);

    assertThat(securityLogWriterAppender.getLogEvents()).hasSize(1);

    LogFileAssert.assertThat(securityLogFile).exists().contains(logMessage);
  }

  @Test
  public void securityGeodeLoggerDoesNotLogToMainLogFile() {
    securityGeodeLogger.info(logMessage);

    assertThat(mainLogWriterAppender.getLogEvents()).isEmpty();
  }
}
