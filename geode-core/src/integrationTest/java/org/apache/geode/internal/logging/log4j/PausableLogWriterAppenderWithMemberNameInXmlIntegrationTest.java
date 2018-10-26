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

import static org.apache.geode.internal.logging.LogMessageRegex.getPattern;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.commons.io.FileUtils;
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

import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogConfigSupplier;
import org.apache.geode.internal.logging.LogMessageRegex.Groups;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link PausableLogWriterAppender} with {@code memberName} in
 * {@code log4j2.xml}.
 */
@Category(LoggingTest.class)
public class PausableLogWriterAppenderWithMemberNameInXmlIntegrationTest {

  private static final String CONFIG_FILE_NAME =
      "PausableLogWriterAppenderWithMemberNameInXmlIntegrationTest_log4j2.xml";
  private static final String APPENDER_NAME = "LOGWRITERWITHMEMBERNAME";
  private static final String MEMBER_NAME = "MEMBERNAME";

  private static String configFilePath;

  private PausableLogWriterAppender pausableLogWriterAppender;
  private File logFile;
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

    String logFileName = MEMBER_NAME + ".log";
    logFile = new File(temporaryFolder.newFolder(testName.getMethodName()), logFileName);

    LogConfig config = mock(LogConfig.class);
    when(config.getName()).thenReturn("");
    when(config.getLogFile()).thenReturn(logFile);

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(config);

    pausableLogWriterAppender.createSession(logConfigSupplier);
    pausableLogWriterAppender.startSession();

    logger = LogService.getLogger();
    logMessage = "Logging in " + testName.getMethodName();

    pausableLogWriterAppender.clearLogEvents();
  }

  @Test
  public void logsToSpecifiedFile() throws Exception {
    logger.info(logMessage);

    assertThat(pausableLogWriterAppender.getLogEvents()).hasSize(1);
    assertThat(logFile).exists();
    String content = FileUtils.readFileToString(logFile, Charset.defaultCharset()).trim();
    assertThat(content).contains(logMessage);
  }

  @Test
  public void logLinesInFileShouldContainMemberName() throws Exception {
    logger.info(logMessage);

    assertThat(pausableLogWriterAppender.getLogEvents()).hasSize(1);
    assertThat(logFile).exists();

    // ManagerLogWriter writes some white space lines so we'll need to skip white space lines
    List<String> allLines = FileUtils.readLines(logFile, Charset.defaultCharset());
    int logLineCount = 0;
    for (String line : allLines) {
      if (!line.isEmpty()) {
        Matcher matcher = getPattern().matcher(line);
        assertThat(matcher.matches()).as("Line does not match regex: " + line).isTrue();
        assertThat(matcher.group(Groups.MEMBER_NAME.getName()))
            .as("Line does not match member name regex: " + line).isEqualTo(MEMBER_NAME);

        logLineCount++;
      }
    }

    assertThat(logLineCount).as("Expected one log line: " + allLines).isEqualTo(1);
  }
}
