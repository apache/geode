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
package org.apache.geode.logging.log4j;

import static org.apache.geode.logging.spi.LoggingProvider.SECURITY_LOGGER_NAME;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.logging.spi.LogConfig;
import org.apache.geode.logging.spi.LogConfigSupplier;
import org.apache.geode.logging.spi.SessionContext;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Integration tests for {@link LogWriterAppender} with security logger and
 * {@link ConfigurationProperties#SECURITY_LOG_FILE}.
 */
@Category({LoggingTest.class, SecurityTest.class})
public class SecurityLogWriterAppenderIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "SecurityLogWriterAppenderIntegrationTest_log4j2.xml";
  private static final String SECURITY_APPENDER_NAME = "SECURITYLOGWRITER";

  private static String configFilePath;

  private LogWriterAppender securityLogWriterAppender;
  private File securityLogFile;
  private SessionContext sessionContext;
  private LogConfigSupplier logConfigSupplier;
  private Logger securityGeodeLogger;
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
    securityLogWriterAppender = loggerContextRule.getAppender(SECURITY_APPENDER_NAME,
        LogWriterAppender.class);

    String name = testName.getMethodName();
    securityLogFile = new File(temporaryFolder.getRoot(), name + "-security.log");

    LogConfig logConfig = mock(LogConfig.class);
    when(logConfig.getName()).thenReturn(name);
    when(logConfig.getSecurityLogFile()).thenReturn(securityLogFile);

    logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(logConfig);

    sessionContext = mock(SessionContext.class);
    when(sessionContext.getLogConfigSupplier()).thenReturn(logConfigSupplier);

    securityGeodeLogger = LogService.getLogger(SECURITY_LOGGER_NAME);
    logMessage = "Logging in " + testName.getMethodName();
  }

  @Test
  public void securityLogWriterAppenderLogEventsIsEmptyByDefault() {
    assertThat(securityLogWriterAppender.getLogEvents()).isEmpty();
  }

  @Test
  public void geodeSecurityLoggerAppendsToSecurityLogWriterAppender() {
    securityLogWriterAppender.createSession(sessionContext);
    securityLogWriterAppender.startSession();

    securityGeodeLogger.info(logMessage);

    assertThat(securityLogWriterAppender.getLogEvents()).hasSize(1);
  }

  @Test
  public void securityLogFileIsEmptyByDefault() {
    securityGeodeLogger.info(logMessage);

    assertThat(securityLogFile).doesNotExist();
  }

  @Test
  public void securityGeodeLoggerLogsToSecurityLogFile() {
    securityLogWriterAppender.createSession(sessionContext);
    securityLogWriterAppender.startSession();

    securityGeodeLogger.info(logMessage);

    LogFileAssert.assertThat(securityLogFile).exists().contains(logMessage);
  }
}
