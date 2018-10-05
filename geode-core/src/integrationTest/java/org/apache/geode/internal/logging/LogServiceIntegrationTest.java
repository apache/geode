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
package org.apache.geode.internal.logging;

import static org.apache.geode.internal.logging.LogServiceIntegrationTestSupport.writeConfigFile;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URL;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.status.StatusLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.logging.log4j.Configurator;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for LogService and how it configures and uses log4j2
 */
@Category(LoggingTest.class)
public class LogServiceIntegrationTest {

  private static final String DEFAULT_CONFIG_FILE_NAME = "log4j2.xml";
  private static final String CLI_CONFIG_FILE_NAME = "log4j2-cli.xml";

  @Rule
  public SystemErrRule systemErrRule = new SystemErrRule().enableLog();

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExternalResource externalResource = new ExternalResource() {
    @Override
    protected void before() {
      beforeConfigFileProp = System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
      beforeLevel = StatusLogger.getLogger().getLevel();

      System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
      StatusLogger.getLogger().setLevel(Level.OFF);

      Configurator.shutdown();
    }

    @Override
    protected void after() {
      Configurator.shutdown();

      System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
      if (beforeConfigFileProp != null) {
        System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, beforeConfigFileProp);
      }
      StatusLogger.getLogger().setLevel(beforeLevel);

      LogService.reconfigure();
      assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
          .isTrue();
    }
  };

  private String beforeConfigFileProp;
  private Level beforeLevel;

  private URL defaultConfigUrl;
  private URL cliConfigUrl;

  @Before
  public void setUp() {
    defaultConfigUrl = LogService.class.getResource(LogService.DEFAULT_CONFIG);
    cliConfigUrl = LogService.class.getResource(LogService.CLI_CONFIG);
  }

  @After
  public void after() {
    // if either of these fail then log4j2 probably logged a failure to stdout
    assertThat(systemErrRule.getLog()).isEmpty();
    assertThat(systemOutRule.getLog()).isEmpty();
  }

  @Test
  public void shouldPreferConfigurationFilePropertyIfSet() throws Exception {
    File configFile = temporaryFolder.newFile(DEFAULT_CONFIG_FILE_NAME);
    String configFileName = configFile.toURI().toString();
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, configFileName);
    writeConfigFile(configFile, Level.DEBUG);

    LogService.reconfigure();

    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
        .isFalse();
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY))
        .isEqualTo(configFileName);
    assertThat(LogService.getLogger().getName()).isEqualTo(getClass().getName());
  }

  @Test
  public void shouldUseDefaultConfigIfNotConfigured() {
    LogService.reconfigure();

    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
        .isTrue();
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY))
        .isNullOrEmpty();
  }

  @Test
  public void defaultConfigShouldIncludeStdout() {
    LogService.reconfigure();
    Logger rootLogger = (Logger) LogService.getRootLogger();

    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
        .isTrue();
    assertThat(rootLogger.getAppenders().get(LogService.STDOUT)).isNotNull();
  }

  @Test
  public void removeConsoleAppenderShouldRemoveStdout() {
    LogService.reconfigure();
    Logger rootLogger = (Logger) LogService.getRootLogger();

    LogService.removeConsoleAppender();

    assertThat(rootLogger.getAppenders().get(LogService.STDOUT)).isNull();
  }

  @Test
  public void restoreConsoleAppenderShouldRestoreStdout() {
    LogService.reconfigure();
    Logger rootLogger = (Logger) LogService.getRootLogger();

    LogService.removeConsoleAppender();

    assertThat(rootLogger.getAppenders().get(LogService.STDOUT)).isNull();

    LogService.restoreConsoleAppender();

    assertThat(rootLogger.getAppenders().get(LogService.STDOUT)).isNotNull();
  }

  @Test
  public void removeAndRestoreConsoleAppenderShouldAffectRootLogger() {
    LogService.reconfigure();

    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
        .isTrue();

    Logger rootLogger = (Logger) LogService.getRootLogger();

    // assert "Console" is present for ROOT
    Appender appender = rootLogger.getAppenders().get(LogService.STDOUT);
    assertThat(appender).isNotNull();

    LogService.removeConsoleAppender();

    // assert "Console" is not present for ROOT
    appender = rootLogger.getAppenders().get(LogService.STDOUT);
    assertThat(appender).isNull();

    LogService.restoreConsoleAppender();

    // assert "Console" is present for ROOT
    appender = rootLogger.getAppenders().get(LogService.STDOUT);
    assertThat(appender).isNotNull();
  }

  @Test
  public void shouldNotUseDefaultConfigIfCliConfigSpecified() {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY,
        cliConfigUrl.toString());

    LogService.reconfigure();

    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigurationInfo())
        .isFalse();
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY))
        .isEqualTo(cliConfigUrl.toString());
    assertThat(LogService.getLogger().getName()).isEqualTo(getClass().getName());
  }

  @Test
  public void isUsingGemFireDefaultConfigShouldBeTrueIfDefaultConfig() {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY,
        defaultConfigUrl.toString());

    assertThat(LogService.getConfiguration().getConfigurationSource().toString())
        .contains(DEFAULT_CONFIG_FILE_NAME);
    assertThat(LogService.isUsingGemFireDefaultConfig()).isTrue();
  }

  @Test
  public void isUsingGemFireDefaultConfigShouldBeFalseIfCliConfig() {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY,
        cliConfigUrl.toString());

    assertThat(LogService.getConfiguration().getConfigurationSource().toString())
        .doesNotContain(DEFAULT_CONFIG_FILE_NAME);
    assertThat(LogService.isUsingGemFireDefaultConfig()).isFalse();
  }

  @Test
  public void shouldUseCliConfigIfCliConfigIsSpecifiedViaClasspath() {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY,
        "classpath:" + CLI_CONFIG_FILE_NAME);

    assertThat(LogService.getConfiguration().getConfigurationSource().toString())
        .contains(CLI_CONFIG_FILE_NAME);
    assertThat(LogService.isUsingGemFireDefaultConfig()).isFalse();
  }
}
