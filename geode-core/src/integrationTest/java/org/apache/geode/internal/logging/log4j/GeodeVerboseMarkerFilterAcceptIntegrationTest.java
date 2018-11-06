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

import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.apache.logging.log4j.test.appender.ListAppender;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category(LoggingTest.class)
public class GeodeVerboseMarkerFilterAcceptIntegrationTest {

  private static final String APPENDER_NAME = "LIST";

  private static String configFilePath;

  private Logger logger;
  private String logMessage;
  private ListAppender listAppender;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @BeforeClass
  public static void setUpLogConfigFile() throws Exception {
    String configFileName =
        GeodeVerboseMarkerFilterAcceptIntegrationTest.class.getSimpleName() + "_log4j2.xml";
    URL resource = getResource(configFileName);
    configFilePath = createFileFromResource(resource, temporaryFolder.getRoot(), configFileName)
        .getAbsolutePath();
  }

  @Before
  public void setUp() throws Exception {
    logger = LogService.getLogger();
    logMessage = "this is a log statement";

    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
        .isFalse();

    listAppender = loggerContextRule.getListAppender(APPENDER_NAME);
  }

  @Test
  public void geodeVerboseShouldLogIfGeodeVerboseIsAccept() {
    logger.info(LogMarker.GEODE_VERBOSE, logMessage);

    LogEvent logEvent = listAppender.getEvents().get(0);
    assertThat(logEvent.getLoggerName()).isEqualTo(logger.getName());
    assertThat(logEvent.getLevel()).isEqualTo(Level.INFO);
    assertThat(logEvent.getMessage().getFormattedMessage()).isEqualTo(logMessage);
  }

  /**
   * GEMFIRE_VERBOSE is parent of GEODE_VERBOSE so enabling GEODE_VERBOSE does not enable
   * GEMFIRE_VERBOSE.
   */
  @Test
  public void gemfireVerboseShouldNotLogIfGeodeVerboseIsAccept() {
    logger.info(LogMarker.GEMFIRE_VERBOSE, logMessage);

    assertThat(listAppender.getEvents()).isEmpty();
  }
}
