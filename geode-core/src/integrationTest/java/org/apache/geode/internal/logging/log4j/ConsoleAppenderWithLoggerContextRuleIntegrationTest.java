/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging.log4j;

import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LifeCycle;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.DefaultErrorHandler;
import org.apache.logging.log4j.core.appender.OutputStreamManager;
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

import org.apache.geode.test.junit.categories.LoggingTest;

@Category(LoggingTest.class)
public class ConsoleAppenderWithLoggerContextRuleIntegrationTest {

  private static String configFilePath;

  private Logger postConfigLogger;
  private String logMessage;

  @ClassRule
  public static SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @ClassRule
  public static SystemErrRule systemErrRule = new SystemErrRule().enableLog();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @BeforeClass
  public static void setUpLogConfigFile() throws Exception {
    String configFileName =
        ConsoleAppenderWithLoggerContextRuleIntegrationTest.class.getSimpleName() + "_log4j2.xml";
    URL resource = getResource(configFileName);
    configFilePath = createFileFromResource(resource, temporaryFolder.getRoot(), configFileName)
        .getAbsolutePath();
  }

  @Before
  public void setUp() throws Exception {
    postConfigLogger = LogManager.getLogger();
    logMessage = "this is a log statement";

    systemOutRule.clearLog();
    systemErrRule.clearLog();
  }

  @Test
  public void delegateConsoleAppenderIsConfigured() {
    assertThat(loggerContextRule.getListAppender("LIST")).isNotNull();

    ListAppender listAppender = findAppender(ListAppender.class);
    assertThat(listAppender).isNotNull();

    ConsoleAppender consoleAppender = findAppender(ConsoleAppender.class);
    assertThat(consoleAppender).isNotNull();

    assertThat(consoleAppender.getFilter()).isNull();
    assertThat(consoleAppender.getHandler()).isInstanceOf(DefaultErrorHandler.class);
    assertThat(consoleAppender.getImmediateFlush()).isTrue();
    assertThat(consoleAppender.getLayout()).isNotNull();
    assertThat(consoleAppender.getManager()).isInstanceOf(OutputStreamManager.class);
    assertThat(consoleAppender.getName()).isEqualTo("STDOUT");
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
  public void staticSystemOutRuleCapturesConsoleAppenderOutputFromPostConfigLogger() {
    postConfigLogger.info(logMessage);

    assertThat(systemOutRule.getLog()).contains(Level.INFO.name().toLowerCase());
    assertThat(systemOutRule.getLog()).contains(logMessage);

    assertThat(systemErrRule.getLog()).isEmpty();
  }

  private <T extends Appender> T findAppender(Class<T> appenderClass) {
    LoggerContext loggerContext =
        ((org.apache.logging.log4j.core.Logger) LogManager.getRootLogger()).getContext();
    Map<String, Appender> appenders = loggerContext.getConfiguration().getAppenders();
    List<Class<? extends Appender>> appenderClasses = new ArrayList<>();
    for (Appender appender : appenders.values()) {
      appenderClasses.add(appender.getClass());
      if (appenderClass.isAssignableFrom(appender.getClass())) {
        return appenderClass.cast(appender);
      }
    }
    assertThat(appenderClasses).contains(appenderClass);
    return null;
  }
}
