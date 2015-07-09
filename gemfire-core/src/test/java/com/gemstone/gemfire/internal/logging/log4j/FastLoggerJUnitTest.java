package com.gemstone.gemfire.internal.logging.log4j;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests FastLogger isDebugEnabled and isTraceEnabled.
 * 
 * @author Kirk Lund
 * @author David Hoots
 */
@Category(UnitTest.class)
public class FastLoggerJUnitTest {

  private static final String TEST_LOGGER_NAME = "com.gemstone.gemfire.cache.internal";
  
  private File configFile;
  
  @Before
  public void setUp() {
    System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
  }
  
  @After
  public void tearDown() {
    System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    LogService.reconfigure();
    if (this.configFile != null && this.configFile.exists()) {
      this.configFile.delete();
    }
  }
  
  /**
   * Verifies that when the configuration is changed the FastLogger
   * debugAvailable field is changed.
   */
  @Test
  public final void testRespondToConfigChange() throws Exception {
    final File configFile = new File(System.getProperty("java.io.tmpdir"), "log4j2-test.xml");
    
    // Load a base config and do some sanity checks
    writeBaseConfigFile(configFile, "WARN");
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, configFile.toURI().toURL().toString());

    LogService.reconfigure();
    
    LogService.getLogger().getName(); // This causes the config file to be loaded
    final Logger testLogger = LogService.getLogger(TEST_LOGGER_NAME);
    
    final LoggerContext appenderContext = ((org.apache.logging.log4j.core.Logger) LogService.getRootLogger()).getContext();
    assertEquals(Level.FATAL, LogService.getLogger(LogService.BASE_LOGGER_NAME).getLevel());
    assertEquals(Level.WARN, LogService.getLogger(TEST_LOGGER_NAME).getLevel());

    // Get a reference to the debugAvailable field in FastLogger
    Field debugAvailableField = FastLogger.class.getDeclaredField("debugAvailable");
    debugAvailableField.setAccessible(true);
    boolean debugAvailable = (Boolean) debugAvailableField.get(FastLogger.class);
    assertFalse(debugAvailable);

    // Modify the config and verify that the debugAvailable field has changed
    writeBaseConfigFile(configFile, "DEBUG");
    appenderContext.reconfigure();
    assertEquals(Level.DEBUG, LogService.getLogger(TEST_LOGGER_NAME).getLevel());
    debugAvailable = (Boolean) debugAvailableField.get(FastLogger.class);
    assertTrue(testLogger.isDebugEnabled());
    assertFalse(testLogger.isTraceEnabled());
    assertTrue(debugAvailable);

    // Modify the config and verify that the debugAvailable field has changed
    writeBaseConfigFile(configFile, "ERROR");
    appenderContext.reconfigure();
    assertEquals(Level.ERROR, LogService.getLogger(TEST_LOGGER_NAME).getLevel());
    assertFalse(testLogger.isDebugEnabled());
    assertFalse((Boolean) debugAvailableField.get(FastLogger.class));

    // Modify the config and verify that the debugAvailable field has changed
    writeBaseConfigFile(configFile, "TRACE");
    appenderContext.reconfigure();
    assertEquals(Level.TRACE, LogService.getLogger(TEST_LOGGER_NAME).getLevel());
    assertTrue(testLogger.isDebugEnabled());
    assertTrue(testLogger.isTraceEnabled());
    assertTrue((Boolean) debugAvailableField.get(FastLogger.class));
    
    // Modify the config and verify that the debugAvailable field has changed
    writeBaseConfigFile(configFile, "INFO");
    appenderContext.reconfigure();
    assertEquals(Level.INFO, LogService.getLogger(TEST_LOGGER_NAME).getLevel());
    assertFalse(testLogger.isDebugEnabled());
    assertFalse((Boolean) debugAvailableField.get(FastLogger.class));
    
    // A reset before the next filter test
    writeBaseConfigFile(configFile, "FATAL");
    appenderContext.reconfigure();
    assertFalse((Boolean) debugAvailableField.get(FastLogger.class));
    
    // Modify the config and verify that the debugAvailable field has changed
    writeLoggerFilterConfigFile(configFile);
    appenderContext.reconfigure();
    assertEquals(Level.ERROR, LogService.getLogger(TEST_LOGGER_NAME).getLevel());
    assertFalse(testLogger.isDebugEnabled());
    assertTrue((Boolean) debugAvailableField.get(FastLogger.class));
    
    // A reset before the next filter test
    writeBaseConfigFile(configFile, "FATAL");
    appenderContext.reconfigure();
    assertFalse((Boolean) debugAvailableField.get(FastLogger.class));
    
    // Modify the config and verify that the debugAvailable field has changed
    writeContextFilterConfigFile(configFile);
    appenderContext.reconfigure();
    assertEquals(Level.ERROR, LogService.getLogger(TEST_LOGGER_NAME).getLevel());
    assertFalse(testLogger.isDebugEnabled());
    assertTrue((Boolean) debugAvailableField.get(FastLogger.class));
  }
  
  /**
   * Verifies that default the configuration sets the FastLogger debugAvailable to false.
   */
  @Test
  public final void testDefaultConfig() throws Exception {
    LogService.reconfigure();
    assertTrue(LogService.isUsingGemFireDefaultConfig());
    
    // Get a reference to the debugAvailable field in FastLogger
    Field debugAvailableField = FastLogger.class.getDeclaredField("debugAvailable");
    debugAvailableField.setAccessible(true);
    boolean debugAvailable = (Boolean) debugAvailableField.get(FastLogger.class);
    assertFalse("FastLogger debugAvailable should be false for default config", debugAvailable);
  }
  
  private static void writeBaseConfigFile(final File configFile, final String level) throws IOException {
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<Configuration monitorInterval=\"5\">" +
          "<Appenders><Console name=\"STDOUT\" target=\"SYSTEM_OUT\"/></Appenders>" +
          "<Loggers>" +
            "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"" + level + "\" additivity=\"false\">" +
              "<AppenderRef ref=\"STDOUT\"/>" +
            "</Logger>" +
            "<Root level=\"FATAL\"/>" +
          "</Loggers>" +
         "</Configuration>"
         );
    writer.close();
  }
  
  private static void writeLoggerFilterConfigFile(final File configFile) throws IOException {
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<Configuration monitorInterval=\"5\">" +
          "<Appenders><Console name=\"STDOUT\" target=\"SYSTEM_OUT\"/></Appenders>" +
          "<Loggers>" +
            "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"ERROR\" additivity=\"false\">" +
              "<MarkerFilter marker=\"FLOW\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" +
              "<AppenderRef ref=\"STDOUT\"/>" +
            "</Logger>" +
            "<Root level=\"FATAL\"/>" +
          "</Loggers>" +
         "</Configuration>"
         );
    writer.close();
  }
  
  private static void writeContextFilterConfigFile(final File configFile) throws IOException {
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<Configuration monitorInterval=\"5\">" +
          "<Appenders><Console name=\"STDOUT\" target=\"SYSTEM_OUT\"/></Appenders>" +
          "<Loggers>" +
            "<Logger name=\"" + TEST_LOGGER_NAME + "\" level=\"ERROR\" additivity=\"false\">" +
              "<AppenderRef ref=\"STDOUT\"/>" +
            "</Logger>" +
            "<Root level=\"FATAL\"/>" +
          "</Loggers>" +
          "<MarkerFilter marker=\"FLOW\" onMatch=\"ACCEPT\" onMismatch=\"DENY\"/>" +
         "</Configuration>"
         );
    writer.close();
  }
}
