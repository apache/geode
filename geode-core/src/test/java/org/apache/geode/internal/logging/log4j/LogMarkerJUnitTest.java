/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging.log4j;


import static org.apache.geode.internal.logging.log4j.custom.CustomConfiguration.*;
import static org.assertj.core.api.Assertions.*;

import java.io.File;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.status.StatusLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.custom.BasicAppender;

/**
 * Integration tests with custom log4j2 configuration.
 */
public class LogMarkerJUnitTest {

  private String beforeConfigFileProp;
  private Level beforeLevel;

  private File customConfigFile;

  @Rule
  public SystemErrRule systemErrRule = new SystemErrRule().enableLog();

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    Configurator.shutdown();
    BasicAppender.clearInstance();

    this.beforeConfigFileProp = System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    this.beforeLevel = StatusLogger.getLogger().getLevel();

    this.customConfigFile = createConfigFileIn(this.temporaryFolder.getRoot());

    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, this.customConfigFile.getAbsolutePath());
    LogService.reconfigure();
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isFalse();
  }

  @After
  public void tearDown() throws Exception {
    Configurator.shutdown();

    System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    if (this.beforeConfigFileProp != null) {
      System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, this.beforeConfigFileProp);
    }
    StatusLogger.getLogger().setLevel(this.beforeLevel);

    LogService.reconfigure();
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isTrue();

    BasicAppender.clearInstance();

    assertThat(this.systemErrRule.getLog()).isEmpty();
  }

  /**
   * Test to see that log messages for GEODE_VERBOSE are filtered, based on the log4j2-custom.xml configuration file
   */
  @Test
  public void testGeodeFilter()
  {
    Logger logger = LogService.getLogger();
    String msg = "verbose geode line";
    logger.error(LogMarker.GEODE_VERBOSE, msg);
    assertThat(systemOutRule.getLog()).contains("");
  }

  /**
   * Test to see that log messages for GEMFIRE_VERBOSE are not filtered, based on the log4j2-custom.xml configuration file
   */
  @Test
  public void testGemfireFilter()
  {
    Logger logger = LogService.getLogger();
    String msg = "verbose gemfire line";
    logger.error(LogMarker.GEMFIRE_VERBOSE, msg);
    assertThat(systemOutRule.getLog()).contains(msg);
  }
}
