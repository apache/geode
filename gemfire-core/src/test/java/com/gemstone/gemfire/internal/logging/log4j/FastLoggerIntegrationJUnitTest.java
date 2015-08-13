package com.gemstone.gemfire.internal.logging.log4j;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests FastLogger isDebugEnabled and isTraceEnabled with various configurations.
 * 
 * For filters see https://logging.apache.org/log4j/2.0/manual/filters.html
 * 
 * @author Kirk Lund
 * @author David Hoots
 */
@Category(IntegrationTest.class)
public class FastLoggerIntegrationJUnitTest {

  private static final String TEST_LOGGER_NAME = FastLogger.class.getPackage().getName();
  private static final String ENABLED_MARKER_NAME = "ENABLED";
  private static final String UNUSED_MARKER_NAME = "UNUSED";
  
  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  private File configFile;
  private String configFileLocation;
  private Logger logger;
  private LoggerContext appenderContext;
  private Marker enabledMarker;
  private Marker unusedMarker;
  
  @Before
  public void setUp() throws Exception {
    System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    this.configFile = new File(this.temporaryFolder.getRoot(), "log4j2-test.xml");
    this.configFileLocation = this.configFile.toURI().toURL().toString();
    this.enabledMarker = MarkerManager.getMarker(ENABLED_MARKER_NAME);
    this.unusedMarker = MarkerManager.getMarker(UNUSED_MARKER_NAME);
    setUpLogService();
  }
  
  @After
  public void tearDown() {
    System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    LogService.reconfigure();
  }
  
  private void setUpLogService() throws Exception {
    // Load a base config and do some sanity checks
    writeSimpleConfigFile(this.configFile, Level.WARN);
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, this.configFileLocation);

    LogService.reconfigure();
    LogService.getLogger().getName(); // This causes the config file to be loaded
    this.logger = LogService.getLogger(TEST_LOGGER_NAME);
    this.appenderContext = ((org.apache.logging.log4j.core.Logger) LogService.getRootLogger()).getContext();
    
    assertThat(LogService.getLogger(LogService.BASE_LOGGER_NAME).getLevel(), is(Level.FATAL));
    assertThat(this.logger, is(instanceOf(FastLogger.class)));
    assertThat(this.logger.getLevel(), is(Level.WARN));
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
  private void verifyIsDelegatingForDebugOrLower(final Level level, final boolean expectIsDelegating) throws Exception {
    writeSimpleConfigFile(this.configFile, level);
    this.appenderContext.reconfigure();
    
    assertThat(this.logger.getLevel(), is(level));
    
    assertThat(this.logger.isTraceEnabled(), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(this.logger.isTraceEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.FATAL)));

    final boolean delegating = ((FastLogger)this.logger).isDelegating();
    assertThat(delegating, is(expectIsDelegating));
    assertThat(delegating, is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(delegating, is(expectIsDelegating));
  }
  
  /**
   * Verifies FastLogger isDelegating if there is a Logger Filter.
   * 
   * @param level the log Level
   * @param expectIsDelegating true if expecting FastLogger.isDelegating to be true
   */
  private void verifyIsDelegatingForLoggerFilter(final Level level, final boolean expectIsDelegating) throws Exception {
    assertThat(expectIsDelegating, is(true)); // always true for Logger Filter

    writeLoggerFilterConfigFile(this.configFile, level);
    this.appenderContext.reconfigure();
    
    assertThat(this.logger.getLevel(), is(level));
    
    assertThat(this.logger.isTraceEnabled(), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(this.logger.isTraceEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(this.logger.isTraceEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(((FastLogger)this.logger).isDelegating(), is(expectIsDelegating));
  }

  /**
   * Verifies FastLogger isDelegating if there is a Context-wide Filter.
   * 
   * @param level the log Level
   * @param expectIsDelegating true if expecting FastLogger.isDelegating to be true
   */
  private void verifyIsDelegatingForContextWideFilter(final Level level, final boolean expectIsDelegating) throws Exception {
    assertThat(expectIsDelegating, is(true)); // always true for Context-wide Filter
    
    writeContextWideFilterConfigFile(this.configFile, level);
    this.appenderContext.reconfigure();
    
    assertThat(this.logger.getLevel(), is(level));
    
    // note: unlike other filters, Context-wide filters are processed BEFORE isEnabled checks
    
    assertThat(this.logger.isTraceEnabled(), is(false));
    assertThat(this.logger.isDebugEnabled(), is(false));
    assertThat(this.logger.isInfoEnabled(), is(false));
    assertThat(this.logger.isWarnEnabled(), is(false));
    assertThat(this.logger.isErrorEnabled(), is(false));
    assertThat(this.logger.isFatalEnabled(), is(false));
    
    assertThat(this.logger.isTraceEnabled(this.enabledMarker), is(true));
    assertThat(this.logger.isDebugEnabled(this.enabledMarker), is(true));
    assertThat(this.logger.isInfoEnabled(this.enabledMarker), is(true));
    assertThat(this.logger.isWarnEnabled(this.enabledMarker), is(true));
    assertThat(this.logger.isErrorEnabled(this.enabledMarker), is(true));
    assertThat(this.logger.isFatalEnabled(this.enabledMarker), is(true));
    
    assertThat(this.logger.isTraceEnabled(this.unusedMarker), is(false));
    assertThat(this.logger.isDebugEnabled(this.unusedMarker), is(false));
    assertThat(this.logger.isInfoEnabled(this.unusedMarker), is(false));
    assertThat(this.logger.isWarnEnabled(this.unusedMarker), is(false));
    assertThat(this.logger.isErrorEnabled(this.unusedMarker), is(false));
    assertThat(this.logger.isFatalEnabled(this.unusedMarker), is(false));

    assertThat(((FastLogger)this.logger).isDelegating(), is(expectIsDelegating));
  }
  
  /**
   * Verifies FastLogger isDelegating if there is a Appender Filter.
   * 
   * @param level the log Level
   * @param expectIsDelegating true if expecting FastLogger.isDelegating to be true
   */
  private void verifyIsDelegatingForAppenderFilter(final Level level, final boolean expectIsDelegating) throws Exception {
    assertThat(expectIsDelegating, is(true)); // always true for Appender Filter

    writeAppenderFilterConfigFile(this.configFile, level);
    this.appenderContext.reconfigure();
    
    assertThat(this.logger.getLevel(), is(level));
    
    assertThat(this.logger.isTraceEnabled(), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(this.logger.isTraceEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(this.logger.isTraceEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(((FastLogger)this.logger).isDelegating(), is(expectIsDelegating));
  }

  /**
   * Verifies FastLogger isDelegating if there is a AppenderRef Filter.
   * 
   * @param level the log Level
   * @param expectIsDelegating true if expecting FastLogger.isDelegating to be true
   */
  private void verifyIsDelegatingForAppenderRefFilter(final Level level, final boolean expectIsDelegating) throws Exception {
    assertThat(expectIsDelegating, is(true)); // always true for AppenderRef Filter

    writeAppenderRefFilterConfigFile(this.configFile, level);
    this.appenderContext.reconfigure();
    
    assertThat(this.logger.getLevel(), is(level));
    
    assertThat(this.logger.isTraceEnabled(), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(this.logger.isTraceEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(this.enabledMarker), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(this.logger.isTraceEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.TRACE)));
    assertThat(this.logger.isDebugEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.DEBUG)));
    assertThat(this.logger.isInfoEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.INFO)));
    assertThat(this.logger.isWarnEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.WARN)));
    assertThat(this.logger.isErrorEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.ERROR)));
    assertThat(this.logger.isFatalEnabled(this.unusedMarker), is(level.isLessSpecificThan(Level.FATAL)));
    
    assertThat(((FastLogger)this.logger).isDelegating(), is(expectIsDelegating));
  }

  private boolean expectDelegating(final boolean value) {
    return value;
  }
  
  private static String writeSimpleConfigFile(final File configFile, final Level level) throws IOException {
    final String xml = 
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<Configuration monitorInterval=\"5\">" +
            "<Appenders><Console name=\"STDOUT\" target=\"SYSTEM_OUT\"/></Appenders>" +
            "<Loggers>" +
              "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"" + level.name() + "\" additivity=\"true\">" +
                "<AppenderRef ref=\"STDOUT\"/>" +
              "</Logger>" +
              "<Root level=\"FATAL\"/>" +
            "</Loggers>" +
           "</Configuration>";
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
    return xml;
  }
  
  private static String writeLoggerFilterConfigFile(final File configFile, final Level level) throws IOException {
    final String xml = 
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<Configuration monitorInterval=\"5\">" +
            "<Appenders><Console name=\"STDOUT\" target=\"SYSTEM_OUT\"/></Appenders>" +
            "<Loggers>" +
              "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"" + level.name() + "\" additivity=\"true\">" +
                "<filters>" +
                  "<MarkerFilter marker=\"" + ENABLED_MARKER_NAME + "\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" +
                "</filters>" +
                "<AppenderRef ref=\"STDOUT\"/>" +
              "</Logger>" +
              "<Root level=\"FATAL\"/>" +
            "</Loggers>" +
           "</Configuration>";
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
    return xml;
  }
  
  private static String writeContextWideFilterConfigFile(final File configFile, final Level level) throws IOException {
    final String xml = 
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<Configuration monitorInterval=\"5\">" +
            "<Appenders><Console name=\"STDOUT\" target=\"SYSTEM_OUT\"/></Appenders>" +
            "<Loggers>" +
              "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"" + level.name() + "\" additivity=\"true\">" +
              "</Logger>" +
              "<Root level=\"FATAL\">" +
                "<AppenderRef ref=\"STDOUT\"/>" +
              "</Root>" +
            "</Loggers>" +
            "<filters>" +
              "<MarkerFilter marker=\"" + ENABLED_MARKER_NAME + "\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" +
            "</filters>" +
           "</Configuration>";
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
    return xml;
  }

  private static String writeAppenderFilterConfigFile(final File configFile, final Level level) throws IOException {
    final String xml = 
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<Configuration monitorInterval=\"5\">" +
            "<Appenders>" +
              "<Console name=\"STDOUT\" target=\"SYSTEM_OUT\">" +
              "<filters>" +
                "<MarkerFilter marker=\"" + ENABLED_MARKER_NAME + "\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" +
              "</filters>" +
              "</Console>" +
            "</Appenders>" +
            "<Loggers>" +
              "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"" + level.name() + "\" additivity=\"true\">" +
              "</Logger>" +
              "<Root level=\"FATAL\">" +
                "<AppenderRef ref=\"STDOUT\"/>" +
              "</Root>" +
            "</Loggers>" +
           "</Configuration>";
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
    return xml;
  }

  private static String writeAppenderRefFilterConfigFile(final File configFile, final Level level) throws IOException {
    final String xml = 
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<Configuration monitorInterval=\"5\">" +
            "<Appenders>" +
              "<Console name=\"STDOUT\" target=\"SYSTEM_OUT\">" +
              "</Console>" +
            "</Appenders>" +
            "<Loggers>" +
              "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"" + level.name() + "\" additivity=\"true\">" +
              "</Logger>" +
              "<Root level=\"FATAL\">" +
                "<AppenderRef ref=\"STDOUT\">" +
                  "<filters>" +
                    "<MarkerFilter marker=\"" + ENABLED_MARKER_NAME + "\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" +
                  "</filters>" +
                "</AppenderRef>" +
              "</Root>" +
            "</Loggers>" +
           "</Configuration>";
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(xml);
    writer.close();
    return xml;
  }
}
