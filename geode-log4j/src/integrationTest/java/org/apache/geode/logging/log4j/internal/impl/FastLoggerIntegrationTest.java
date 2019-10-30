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

import static org.apache.geode.logging.internal.Configuration.create;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.CONFIG;
import static org.apache.geode.logging.internal.spi.LoggingProvider.MAIN_LOGGER_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.junit.LoggerContextRule;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.logging.internal.Configuration;
import org.apache.geode.logging.internal.log4j.api.FastLogger;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.LogLevelUpdateOccurs;
import org.apache.geode.logging.internal.spi.LogLevelUpdateScope;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Integration tests for {@link FastLogger} with various configurations.
 *
 * <p>
 * For filters see https://logging.apache.org/log4j/2.0/manual/filters.html
 */
@Category(LoggingTest.class)
public class FastLoggerIntegrationTest {

  /**
   * This config file is generated dynamically by the test class.
   */
  private static final String CONFIG_FILE_NAME = "FastLoggerIntegrationTest_log4j2.xml";
  private static final String TEST_LOGGER_NAME = FastLogger.class.getPackage().getName();
  private static final String ENABLED_MARKER_NAME = "ENABLED";
  private static final String UNUSED_MARKER_NAME = "UNUSED";

  private static File configFile;
  private static String configFilePath;

  private Configuration configuration;
  private Logger logger;
  private Marker enabledMarker;
  private Marker unusedMarker;

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LoggerContextRule loggerContextRule = new LoggerContextRule(configFilePath);

  @BeforeClass
  public static void setUpLogConfigFile() throws Exception {
    configFile = new File(temporaryFolder.getRoot(), CONFIG_FILE_NAME);
    configFilePath = configFile.getAbsolutePath();
    writeSimpleConfigFile(configFile, Level.WARN);
  }

  @Before
  public void setUp() {
    enabledMarker = MarkerManager.getMarker(ENABLED_MARKER_NAME);
    unusedMarker = MarkerManager.getMarker(UNUSED_MARKER_NAME);

    logger = LogService.getLogger(TEST_LOGGER_NAME);

    assertThat(LogService.getLogger(MAIN_LOGGER_NAME).getLevel()).isEqualTo(Level.FATAL);
    assertThat(logger).isInstanceOf(FastLogger.class);
    assertThat(logger.getLevel()).isEqualTo(Level.WARN);

    LogConfig logConfig = mock(LogConfig.class);
    when(logConfig.getLogLevel()).thenReturn(CONFIG.intLevel());
    when(logConfig.getSecurityLogLevel()).thenReturn(CONFIG.intLevel());

    LogConfigSupplier logConfigSupplier = mock(LogConfigSupplier.class);
    when(logConfigSupplier.getLogConfig()).thenReturn(logConfig);

    configuration = create(LogLevelUpdateOccurs.NEVER, LogLevelUpdateScope.GEODE_LOGGERS);
    configuration.initialize(logConfigSupplier);
  }

  @After
  public void tearDown() throws Exception {
    configuration.shutdown();

    writeSimpleConfigFile(configFile, Level.WARN);
    loggerContextRule.reconfigure();
  }

  @Test
  public void debugConfigIsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
  }

  @Test
  public void traceConfigIsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.TRACE, expectDelegating(true));
  }

  @Test
  public void infoConfigIsNotDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
  }

  @Test
  public void warnConfigIsNotDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.WARN, expectDelegating(false));
  }

  @Test
  public void errorConfigIsNotDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.ERROR, expectDelegating(false));
  }

  @Test
  public void fatalConfigIsNotDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.FATAL, expectDelegating(false));
  }

  @Test
  public void fromDebugToInfoSetsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
  }

  @Test
  public void fromInfoToDebugUnsetsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
  }

  @Test
  public void fromDebugToContextWideFilterKeepsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
    verifyIsDelegatingForContextWideFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void fromInfoToContextWideFilterSetsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
    verifyIsDelegatingForContextWideFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void fromContextWideFilterToInfoUnsetsDelegating() throws Exception {
    verifyIsDelegatingForContextWideFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
  }

  @Test
  public void fromContextWideFilterToDebugKeepsDelegating() throws Exception {
    verifyIsDelegatingForContextWideFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
  }

  @Test
  public void fromDebugToAppenderFilterKeepsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
    verifyIsDelegatingForAppenderFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void fromInfoToAppenderFilterSetsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
    verifyIsDelegatingForAppenderFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void fromAppenderFilterToInfoUnsetsDelegating() throws Exception {
    verifyIsDelegatingForAppenderFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
  }

  @Test
  public void fromAppenderFilterToDebugKeepsDelegating() throws Exception {
    verifyIsDelegatingForAppenderFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
  }

  @Test
  public void fromDebugToLoggerFilterKeepsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
    verifyIsDelegatingForLoggerFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void fromInfoToLoggerFilterSetsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
    verifyIsDelegatingForLoggerFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void fromLoggerFilterToInfoUnsetsDelegating() throws Exception {
    verifyIsDelegatingForLoggerFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
  }

  @Test
  public void fromLoggerFilterToDebugKeepsDelegating() throws Exception {
    verifyIsDelegatingForLoggerFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
  }

  @Test
  public void fromDebugToAppenderRefFilterKeepsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
    verifyIsDelegatingForAppenderRefFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void fromInfoToAppenderRefFilterSetsDelegating() throws Exception {
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
    verifyIsDelegatingForAppenderRefFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void fromAppenderRefFilterToInfoUnsetsDelegating() throws Exception {
    verifyIsDelegatingForAppenderRefFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForDebugOrLower(Level.INFO, expectDelegating(false));
  }

  @Test
  public void fromAppenderRefFilterToDebugKeepsDelegating() throws Exception {
    verifyIsDelegatingForAppenderRefFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForDebugOrLower(Level.DEBUG, expectDelegating(true));
  }

  @Test
  public void fromContextWideFilterToLoggerFilterKeepsDelegating() throws Exception {
    verifyIsDelegatingForContextWideFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForLoggerFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void fromLoggerFilterToContextWideFilterKeepsDelegating() throws Exception {
    verifyIsDelegatingForLoggerFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForContextWideFilter(Level.INFO, expectDelegating(true));
  }

  @Test
  public void contextWideFilterIsDelegating() throws Exception {
    verifyIsDelegatingForContextWideFilter(Level.TRACE, expectDelegating(true));
    verifyIsDelegatingForContextWideFilter(Level.DEBUG, expectDelegating(true));
    verifyIsDelegatingForContextWideFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForContextWideFilter(Level.WARN, expectDelegating(true));
    verifyIsDelegatingForContextWideFilter(Level.ERROR, expectDelegating(true));
    verifyIsDelegatingForContextWideFilter(Level.FATAL, expectDelegating(true));
  }

  @Test
  public void loggerFilterIsDelegating() throws Exception {
    verifyIsDelegatingForLoggerFilter(Level.TRACE, expectDelegating(true));
    verifyIsDelegatingForLoggerFilter(Level.DEBUG, expectDelegating(true));
    verifyIsDelegatingForLoggerFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForLoggerFilter(Level.WARN, expectDelegating(true));
    verifyIsDelegatingForLoggerFilter(Level.ERROR, expectDelegating(true));
    verifyIsDelegatingForLoggerFilter(Level.FATAL, expectDelegating(true));
  }

  @Test
  public void appenderFilterIsDelegating() throws Exception {
    verifyIsDelegatingForAppenderFilter(Level.TRACE, expectDelegating(true));
    verifyIsDelegatingForAppenderFilter(Level.DEBUG, expectDelegating(true));
    verifyIsDelegatingForAppenderFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForAppenderFilter(Level.WARN, expectDelegating(true));
    verifyIsDelegatingForAppenderFilter(Level.ERROR, expectDelegating(true));
    verifyIsDelegatingForAppenderFilter(Level.FATAL, expectDelegating(true));
  }

  @Test
  public void appenderRefFilterIsDelegating() throws Exception {
    verifyIsDelegatingForAppenderRefFilter(Level.TRACE, expectDelegating(true));
    verifyIsDelegatingForAppenderRefFilter(Level.DEBUG, expectDelegating(true));
    verifyIsDelegatingForAppenderRefFilter(Level.INFO, expectDelegating(true));
    verifyIsDelegatingForAppenderRefFilter(Level.WARN, expectDelegating(true));
    verifyIsDelegatingForAppenderRefFilter(Level.ERROR, expectDelegating(true));
    verifyIsDelegatingForAppenderRefFilter(Level.FATAL, expectDelegating(true));
  }

  /**
   * Verifies FastLogger isDelegating if Level is DEBUG or TRACE.
   *
   * @param level the log Level
   * @param expectIsDelegating true if expecting FastLogger.isDelegating to be true
   */
  private void verifyIsDelegatingForDebugOrLower(final Level level,
      final boolean expectIsDelegating) throws Exception {
    writeSimpleConfigFile(configFile, level);
    loggerContextRule.reconfigure();
    configuration.configChanged();

    assertThat(logger.getLevel()).isEqualTo(level);

    assertThat(logger.isTraceEnabled()).isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled()).isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled()).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled()).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled()).isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled()).isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(logger.isTraceEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled(unusedMarker)).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled(unusedMarker)).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.FATAL));

    boolean delegating = ((FastLogger) logger).isDelegating();
    assertThat(delegating).isEqualTo(expectIsDelegating);
    assertThat(delegating).isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(delegating).isEqualTo(expectIsDelegating);
  }

  /**
   * Verifies FastLogger isDelegating if there is a Logger Filter.
   *
   * @param level the log Level
   * @param expectIsDelegating true if expecting FastLogger.isDelegating to be true
   */
  private void verifyIsDelegatingForLoggerFilter(final Level level,
      final boolean expectIsDelegating) throws Exception {
    assertThat(expectIsDelegating).isEqualTo(true); // always true for Logger Filter

    writeLoggerFilterConfigFile(configFile, level);
    loggerContextRule.reconfigure();
    configuration.configChanged();

    assertThat(logger.getLevel()).isEqualTo(level);

    assertThat(logger.isTraceEnabled()).isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled()).isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled()).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled()).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled()).isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled()).isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(logger.isTraceEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled(enabledMarker)).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled(enabledMarker)).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(logger.isTraceEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled(unusedMarker)).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled(unusedMarker)).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(((FastLogger) logger).isDelegating()).isEqualTo(expectIsDelegating);
  }

  /**
   * Verifies FastLogger isDelegating if there is a Context-wide Filter.
   *
   * @param level the log Level
   * @param expectIsDelegating true if expecting FastLogger.isDelegating to be true
   */
  private void verifyIsDelegatingForContextWideFilter(final Level level,
      final boolean expectIsDelegating) throws Exception {
    assertThat(expectIsDelegating).isEqualTo(true); // always true for Context-wide Filter

    writeContextWideFilterConfigFile(configFile, level);
    loggerContextRule.reconfigure();
    configuration.configChanged();

    assertThat(logger.getLevel()).isEqualTo(level);

    // note: unlike other filters, Context-wide filters are processed BEFORE isEnabled checks

    assertThat(logger.isTraceEnabled()).isEqualTo(false);
    assertThat(logger.isDebugEnabled()).isEqualTo(false);
    assertThat(logger.isInfoEnabled()).isEqualTo(false);
    assertThat(logger.isWarnEnabled()).isEqualTo(false);
    assertThat(logger.isErrorEnabled()).isEqualTo(false);
    assertThat(logger.isFatalEnabled()).isEqualTo(false);

    assertThat(logger.isTraceEnabled(enabledMarker)).isEqualTo(true);
    assertThat(logger.isDebugEnabled(enabledMarker)).isEqualTo(true);
    assertThat(logger.isInfoEnabled(enabledMarker)).isEqualTo(true);
    assertThat(logger.isWarnEnabled(enabledMarker)).isEqualTo(true);
    assertThat(logger.isErrorEnabled(enabledMarker)).isEqualTo(true);
    assertThat(logger.isFatalEnabled(enabledMarker)).isEqualTo(true);

    assertThat(logger.isTraceEnabled(unusedMarker)).isEqualTo(false);
    assertThat(logger.isDebugEnabled(unusedMarker)).isEqualTo(false);
    assertThat(logger.isInfoEnabled(unusedMarker)).isEqualTo(false);
    assertThat(logger.isWarnEnabled(unusedMarker)).isEqualTo(false);
    assertThat(logger.isErrorEnabled(unusedMarker)).isEqualTo(false);
    assertThat(logger.isFatalEnabled(unusedMarker)).isEqualTo(false);

    assertThat(((FastLogger) logger).isDelegating()).isEqualTo(expectIsDelegating);
  }

  /**
   * Verifies FastLogger isDelegating if there is a Appender Filter.
   *
   * @param level the log Level
   * @param expectIsDelegating true if expecting FastLogger.isDelegating to be true
   */
  private void verifyIsDelegatingForAppenderFilter(final Level level,
      final boolean expectIsDelegating) throws Exception {
    assertThat(expectIsDelegating).isEqualTo(true); // always true for Appender Filter

    writeAppenderFilterConfigFile(configFile, level);
    loggerContextRule.reconfigure();
    configuration.configChanged();

    assertThat(logger.getLevel()).isEqualTo(level);

    assertThat(logger.isTraceEnabled()).isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled()).isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled()).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled()).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled()).isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled()).isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(logger.isTraceEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled(enabledMarker)).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled(enabledMarker)).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(logger.isTraceEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled(unusedMarker)).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled(unusedMarker)).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(((FastLogger) logger).isDelegating()).isEqualTo(expectIsDelegating);
  }

  /**
   * Verifies FastLogger isDelegating if there is a AppenderRef Filter.
   *
   * @param level the log Level
   * @param expectIsDelegating true if expecting FastLogger.isDelegating to be true
   */
  private void verifyIsDelegatingForAppenderRefFilter(final Level level,
      final boolean expectIsDelegating) throws Exception {
    assertThat(expectIsDelegating).isEqualTo(true); // always true for AppenderRef Filter

    writeAppenderRefFilterConfigFile(configFile, level);
    loggerContextRule.reconfigure();
    configuration.configChanged();

    assertThat(logger.getLevel()).isEqualTo(level);

    assertThat(logger.isTraceEnabled()).isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled()).isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled()).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled()).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled()).isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled()).isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(logger.isTraceEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled(enabledMarker)).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled(enabledMarker)).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled(enabledMarker))
        .isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(logger.isTraceEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.TRACE));
    assertThat(logger.isDebugEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.DEBUG));
    assertThat(logger.isInfoEnabled(unusedMarker)).isEqualTo(level.isLessSpecificThan(Level.INFO));
    assertThat(logger.isWarnEnabled(unusedMarker)).isEqualTo(level.isLessSpecificThan(Level.WARN));
    assertThat(logger.isErrorEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.ERROR));
    assertThat(logger.isFatalEnabled(unusedMarker))
        .isEqualTo(level.isLessSpecificThan(Level.FATAL));

    assertThat(((FastLogger) logger).isDelegating()).isEqualTo(expectIsDelegating);
  }

  private boolean expectDelegating(final boolean value) {
    return value;
  }

  private static void writeSimpleConfigFile(final File configFile, final Level level)
      throws IOException {
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<Configuration monitorInterval=\"5\">"
            + "<Appenders><Console name=\"STDOUT\" target=\"SYSTEM_OUT\"/></Appenders>"
            + "<Loggers>" + "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"" + level.name()
            + "\" additivity=\"true\">" + "<AppenderRef ref=\"STDOUT\"/>" + "</Logger>"
            + "<Root level=\"FATAL\"/>" + "</Loggers>" + "</Configuration>";
    BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
  }

  private static void writeLoggerFilterConfigFile(final File configFile, final Level level)
      throws IOException {
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<Configuration monitorInterval=\"5\">"
            + "<Appenders><Console name=\"STDOUT\" target=\"SYSTEM_OUT\"/></Appenders>"
            + "<Loggers>" + "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"" + level.name()
            + "\" additivity=\"true\">" + "<filters>" + "<MarkerFilter marker=\""
            + ENABLED_MARKER_NAME + "\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" + "</filters>"
            + "<AppenderRef ref=\"STDOUT\"/>" + "</Logger>" + "<Root level=\"FATAL\"/>"
            + "</Loggers>" + "</Configuration>";
    BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
  }

  private static void writeContextWideFilterConfigFile(final File configFile, final Level level)
      throws IOException {
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<Configuration monitorInterval=\"5\">"
            + "<Appenders><Console name=\"STDOUT\" target=\"SYSTEM_OUT\"/></Appenders>"
            + "<Loggers>" + "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"" + level.name()
            + "\" additivity=\"true\">" + "</Logger>" + "<Root level=\"FATAL\">"
            + "<AppenderRef ref=\"STDOUT\"/>" + "</Root>" + "</Loggers>" + "<filters>"
            + "<MarkerFilter marker=\"" + ENABLED_MARKER_NAME
            + "\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" + "</filters>" + "</Configuration>";
    BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
  }

  private static void writeAppenderFilterConfigFile(final File configFile, final Level level)
      throws IOException {
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<Configuration monitorInterval=\"5\">"
            + "<Appenders>" + "<Console name=\"STDOUT\" target=\"SYSTEM_OUT\">" + "<filters>"
            + "<MarkerFilter marker=\"" + ENABLED_MARKER_NAME
            + "\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" + "</filters>" + "</Console>"
            + "</Appenders>" + "<Loggers>" + "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\""
            + level.name() + "\" additivity=\"true\">" + "</Logger>" + "<Root level=\"FATAL\">"
            + "<AppenderRef ref=\"STDOUT\"/>" + "</Root>" + "</Loggers>" + "</Configuration>";
    BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
  }

  private static void writeAppenderRefFilterConfigFile(final File configFile, final Level level)
      throws IOException {
    String xml =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<Configuration monitorInterval=\"5\">"
            + "<Appenders>" + "<Console name=\"STDOUT\" target=\"SYSTEM_OUT\">" + "</Console>"
            + "</Appenders>" + "<Loggers>" + "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\""
            + level.name() + "\" additivity=\"true\">" + "</Logger>" + "<Root level=\"FATAL\">"
            + "<AppenderRef ref=\"STDOUT\">" + "<filters>" + "<MarkerFilter marker=\""
            + ENABLED_MARKER_NAME + "\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" + "</filters>"
            + "</AppenderRef>" + "</Root>" + "</Loggers>" + "</Configuration>";
    BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
  }
}
