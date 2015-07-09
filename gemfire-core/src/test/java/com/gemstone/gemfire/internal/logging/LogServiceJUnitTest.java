package com.gemstone.gemfire.internal.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class LogServiceJUnitTest {
  
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
  
  @Test
  public void testPropSetAndGetLogger() throws Exception {
    this.configFile = new File(System.getProperty("java.io.tmpdir"), "log4j2.xml");
    final String configFileName = this.configFile.toURI().toString();

    writeConfigFile(this.configFile);
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, configFileName);
    LogService.reconfigure();
    assertFalse(LogService.isUsingGemFireDefaultConfig());
    
    // Assert that the correct log file will be found
    assertEquals(configFileName, System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY));

    // Assert that getLogger() returns a correctly named logger
    assertEquals(this.getClass().getName(), LogService.getLogger().getName());
  }
  
  @Test
  public void testInCurrentDirectory() throws Exception {
    // if working directory is in classpath this test will fail
    assumeFalse(SystemUtils.isInClassPath(System.getProperty("user.dir")));
    System.out.println("Executing testInCurrentDirectory");
    
    this.configFile = new File(System.getProperty("user.dir"), "log4j2-test.xml");
    String configFileURI = this.configFile.toURI().toString();

    writeConfigFile(this.configFile);
    LogService.reconfigure();
    assertFalse(LogService.isUsingGemFireDefaultConfig());
    ConfigurationFactory.getInstance().getConfiguration(null, null);
    
    assertEquals(configFileURI, System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY));
  }
  
  @Test
  public void testInGemfireJar() throws Exception {
    LogService.reconfigure();
    assertTrue(LogService.isUsingGemFireDefaultConfig());
    
    // This ensures that the JVM isn't going to have a problem finding the config
    // in a jar file.
    ConfigurationFactory.getInstance().getConfiguration(null, null);

    final String packagePath = LogService.class.getPackage().getName().replace('.', '/');
    final URL configUrl = ClassPathLoader.getLatest().getResource(packagePath + "/log4j/log4j2-default.xml");

    assertEquals(configUrl.toString(), System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY));
  }
  
  @Test
  public void testRemoveAddConsoleAppender() {
    LogService.reconfigure();
    assertTrue(LogService.isUsingGemFireDefaultConfig());
    
    assertEquals("", LogService.getRootLogger().getName());
    final Logger rootLogger = (Logger) LogService.getRootLogger();
    
    // assert "nothing" is not present for ROOT
    Appender appender = rootLogger.getAppenders().get("nothing");
    assertNull(appender);
    
    // assert "Console" is present for ROOT
    appender = rootLogger.getAppenders().get(LogService.STDOUT);
    assertNotNull("Missing STDOUT: " + rootLogger.getAppenders(), appender); // fails when test is last one run

    LogService.removeConsoleAppender();
    
    // assert "Console" is not present for ROOT
    appender = rootLogger.getAppenders().get(LogService.STDOUT);
    assertNull(appender);
    
    LogService.restoreConsoleAppender();

    // assert "Console" is present for ROOT
    appender = rootLogger.getAppenders().get(LogService.STDOUT);
    assertNotNull(appender);
  }
  
  @Test
  public void testInitializeAfterLogManager() {
    LogManager.getRootLogger();
    LogService.reconfigure();
    LogService.initialize();
    assertTrue(LogService.isUsingGemFireDefaultConfig());
  }
  
  @Test
  public void cliConfigLoadsAsResource() {
    URL configUrl = LogService.class.getResource(LogService.CLI_CONFIG);
    assertNotNull(configUrl);
    String configFilePropertyValue = configUrl.toString();
    assertTrue(configFilePropertyValue.contains(LogService.CLI_CONFIG));
  }
  
  @Test
  public void defaultConfigLoadsAsResource() {
    URL configUrl = LogService.class.getResource(LogService.CLI_CONFIG);
    assertNotNull(configUrl);
    String configFilePropertyValue = configUrl.toString();
    assertTrue(configFilePropertyValue.contains(LogService.CLI_CONFIG));
  }
  
  private static void writeConfigFile(final File configFile) throws IOException {
    final BufferedWriter writer = new BufferedWriter(new FileWriter(configFile));
    writer.write(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<Configuration>" +
          "<Loggers>" +
            "<Root level=\"DEBUG\"/>" +
          "</Loggers>" +
         "</Configuration>"
         );
    writer.close();
  }
}
