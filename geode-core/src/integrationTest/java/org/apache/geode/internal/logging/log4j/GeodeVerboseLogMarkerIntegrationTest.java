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

import static org.apache.logging.log4j.core.config.ConfigurationFactory.CONFIGURATION_FILE_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.status.StatusLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.custom.BasicAppender;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests with accept and deny of GEODE_VERBOSE and GEMFIRE_VERBOSE.
 */
@Category(LoggingTest.class)
public class GeodeVerboseLogMarkerIntegrationTest {

  private static final String RESOURCE_PACKAGE = "/org/apache/geode/internal/logging/log4j/marker/";
  private static final String FILE_NAME_GEMFIRE_VERBOSE_ACCEPT =
      "log4j2-gemfire_verbose-accept.xml";
  private static final String FILE_NAME_GEMFIRE_VERBOSE_DENY = "log4j2-gemfire_verbose-deny.xml";
  private static final String FILE_NAME_GEODE_VERBOSE_ACCEPT = "log4j2-geode_verbose-accept.xml";
  private static final String FILE_NAME_GEODE_VERBOSE_DENY = "log4j2-geode_verbose-deny.xml";

  private String beforeConfigFileProp;
  private Level beforeLevel;

  private File configFileGemfireVerboseAccept;
  private File configFileGemfireVerboseDeny;
  private File configFileGeodeVerboseAccept;
  private File configFileGeodeVerboseDeny;

  @Rule
  public SystemErrRule systemErrRule = new SystemErrRule().enableLog();
  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  @Before
  public void preAssertions() {
    assertThat(getClass().getResource(RESOURCE_PACKAGE + FILE_NAME_GEMFIRE_VERBOSE_ACCEPT))
        .isNotNull();
    assertThat(getClass().getResource(RESOURCE_PACKAGE + FILE_NAME_GEMFIRE_VERBOSE_DENY))
        .isNotNull();
    assertThat(getClass().getResource(RESOURCE_PACKAGE + FILE_NAME_GEODE_VERBOSE_ACCEPT))
        .isNotNull();
    assertThat(getClass().getResource(RESOURCE_PACKAGE + FILE_NAME_GEODE_VERBOSE_DENY)).isNotNull();
  }

  @Before
  public void setUp() throws Exception {
    Configurator.shutdown();
    BasicAppender.clearInstance();

    beforeConfigFileProp = System.getProperty(CONFIGURATION_FILE_PROPERTY);
    beforeLevel = StatusLogger.getLogger().getLevel();

    configFileGemfireVerboseAccept = createConfigFile(FILE_NAME_GEMFIRE_VERBOSE_ACCEPT);
    configFileGemfireVerboseDeny = createConfigFile(FILE_NAME_GEMFIRE_VERBOSE_DENY);
    configFileGeodeVerboseAccept = createConfigFile(FILE_NAME_GEODE_VERBOSE_ACCEPT);
    configFileGeodeVerboseDeny = createConfigFile(FILE_NAME_GEODE_VERBOSE_DENY);
  }

  @After
  public void tearDown() throws Exception {
    Configurator.shutdown();

    System.clearProperty(CONFIGURATION_FILE_PROPERTY);
    if (beforeConfigFileProp != null) {
      System.setProperty(CONFIGURATION_FILE_PROPERTY, beforeConfigFileProp);
    }
    StatusLogger.getLogger().setLevel(beforeLevel);

    LogService.reconfigure();
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
        .isTrue();

    BasicAppender.clearInstance();

    assertThat(systemErrRule.getLog()).isEmpty();
  }

  @Test
  public void geodeVerboseShouldLogIfGeodeVerboseIsAccept() {
    configureLogging(configFileGeodeVerboseAccept);
    Logger logger = LogService.getLogger();

    String msg = testName.getMethodName();
    logger.info(LogMarker.GEODE_VERBOSE, msg);

    assertThat(systemOutRule.getLog()).contains(msg);
  }

  @Test
  public void geodeVerboseShouldNotLogIfGeodeVerboseIsDeny() {
    configureLogging(configFileGeodeVerboseDeny);
    Logger logger = LogService.getLogger();

    String msg = testName.getMethodName();
    logger.info(LogMarker.GEODE_VERBOSE, msg);

    assertThat(systemOutRule.getLog()).doesNotContain(msg);
  }

  @Test
  public void geodeVerboseShouldLogIfGemfireVerboseIsAccept() {
    configureLogging(configFileGemfireVerboseAccept);
    Logger logger = LogService.getLogger();

    String msg = testName.getMethodName();
    logger.info(LogMarker.GEODE_VERBOSE, msg);

    assertThat(systemOutRule.getLog()).contains(msg);
  }

  @Test
  public void geodeVerboseShouldNotLogIfGemfireVerboseIsDeny() {
    configureLogging(configFileGemfireVerboseDeny);
    Logger logger = LogService.getLogger();

    String msg = testName.getMethodName();
    logger.info(LogMarker.GEODE_VERBOSE, msg);

    assertThat(systemOutRule.getLog()).doesNotContain(msg);
  }

  /**
   * GEMFIRE_VERBOSE is parent of GEODE_VERBOSE so enabling GEODE_VERBOSE does not enable
   * GEMFIRE_VERBOSE.
   */
  @Test
  public void gemfireVerboseShouldNotLogIfGeodeVerboseIsAccept() {
    configureLogging(configFileGeodeVerboseAccept);
    Logger logger = LogService.getLogger();

    String msg = testName.getMethodName();
    logger.info(LogMarker.GEMFIRE_VERBOSE, msg);

    assertThat(systemOutRule.getLog()).doesNotContain(msg);
  }

  /**
   * GEMFIRE_VERBOSE is parent of GEODE_VERBOSE so disabling GEODE_VERBOSE does not disable
   * GEMFIRE_VERBOSE.
   */
  @Test
  public void gemfireVerboseShouldLogIfGeodeVerboseIsDeny() {
    configureLogging(configFileGeodeVerboseDeny);
    Logger logger = LogService.getLogger();

    String msg = testName.getMethodName();
    logger.info(LogMarker.GEMFIRE_VERBOSE, msg);

    assertThat(systemOutRule.getLog()).contains(msg);
  }

  @Test
  public void gemfireVerboseShouldLogIfGemfireVerboseIsAccept() {
    configureLogging(configFileGemfireVerboseAccept);
    Logger logger = LogService.getLogger();

    String msg = testName.getMethodName();
    logger.info(LogMarker.GEMFIRE_VERBOSE, msg);

    assertThat(systemOutRule.getLog()).contains(msg);
  }

  @Test
  public void gemfireVerboseShouldNotLogIfGemfireVerboseIsDeny() {
    configureLogging(configFileGemfireVerboseDeny);
    Logger logger = LogService.getLogger();

    String msg = testName.getMethodName();
    logger.info(LogMarker.GEMFIRE_VERBOSE, msg);

    assertThat(systemOutRule.getLog()).doesNotContain(msg);
  }

  private File createConfigFile(final String name) throws IOException, URISyntaxException {
    assertThat(getClass().getResource(RESOURCE_PACKAGE + name)).isNotNull();
    return new Configuration(getClass().getResource(RESOURCE_PACKAGE + name), name)
        .createConfigFileIn(temporaryFolder.getRoot());
  }

  private void configureLogging(final File configFile) {
    System.setProperty(CONFIGURATION_FILE_PROPERTY, configFile.getAbsolutePath());
    LogService.reconfigure();
  }
}
