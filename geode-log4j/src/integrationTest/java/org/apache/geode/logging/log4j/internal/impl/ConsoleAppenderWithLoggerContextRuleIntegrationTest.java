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

import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LifeCycle;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.DefaultErrorHandler;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Verifies behavior of {@code SystemOutRule} with {@code LoggerContextRule} that other Geode
 * logging tests depends on. If this behavior changes, then those tests may also need to change.
 */
@Category(LoggingTest.class)
public class ConsoleAppenderWithLoggerContextRuleIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "ConsoleAppenderWithLoggerContextRuleIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "STDOUT";

  private static String configFilePath;

  private Logger logger;
  private String logMessage;
  private ConsoleAppender consoleAppender;

  @ClassRule
  public static SystemOutRule systemOutRule = new SystemOutRule().enableLog();

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
    logger = LogManager.getLogger();
    logMessage = "Logging in " + testName.getMethodName();
    consoleAppender = loggerContextRule.getAppender(APPENDER_NAME, ConsoleAppender.class);

    systemOutRule.clearLog();
  }

  @Test
  public void consoleAppenderIsConfigured() {
    assertThat(consoleAppender.getFilter()).isNull();
    assertThat(consoleAppender.getHandler()).isInstanceOf(DefaultErrorHandler.class);
    assertThat(consoleAppender.getImmediateFlush()).isTrue();
    assertThat(consoleAppender.getLayout()).isNotNull();
    assertThat(consoleAppender.getManager()).isInstanceOf(OutputStreamManager.class);
    assertThat(consoleAppender.getName()).isEqualTo(APPENDER_NAME);
    assertThat(consoleAppender.getState()).isSameAs(LifeCycle.State.STARTED);
    assertThat(consoleAppender.getTarget()).isSameAs(ConsoleAppender.Target.SYSTEM_OUT);

    OutputStreamManager outputStreamManager = consoleAppender.getManager();
    assertThat(outputStreamManager.isOpen()).isTrue();
    assertThat(outputStreamManager.getByteBuffer()).isInstanceOf(ByteBuffer.class);
    assertThat(outputStreamManager.hasOutputStream()).isTrue();
    assertThat(outputStreamManager.getContentFormat()).isEmpty();
    assertThat(outputStreamManager.getLoggerContext()).isNull();
    assertThat(outputStreamManager.getName()).isEqualTo("SYSTEM_OUT.false.false");
  }

  @Test
  public void staticSystemOutRuleCapturesConsoleAppenderOutput() {
    logger.info(logMessage);

    assertThat(systemOutRule.getLog()).contains(Level.INFO.name().toLowerCase());
    assertThat(systemOutRule.getLog()).contains(logMessage);
  }
}
