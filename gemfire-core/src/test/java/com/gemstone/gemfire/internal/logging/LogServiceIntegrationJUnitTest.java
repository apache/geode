package com.gemstone.gemfire.internal.logging;

import static com.gemstone.gemfire.internal.logging.LogServiceIntegrationTestSupport.*;
import static org.assertj.core.api.Assertions.*;

import java.io.File;
import java.net.URL;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.status.StatusLogger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.logging.log4j.Configurator;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for LogService and how it configures and uses log4j2
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class LogServiceIntegrationJUnitTest {
  
  private String beforeConfigFileProp;
  private Level beforeLevel;
  
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  @Rule
  public final ExternalResource externalResource = new ExternalResource() {
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
      
      if (beforeConfigFileProp != null) {
        System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, beforeConfigFileProp);
      }
      StatusLogger.getLogger().setLevel(beforeLevel);
      
      LogService.reconfigure();
      assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isTrue();
    }
  };
  
  private URL defaultConfigUrl;
  private URL cliConfigUrl;
  
  @Before
  public void setUp() {
    this.defaultConfigUrl = LogService.class.getResource(LogService.DEFAULT_CONFIG);
    this.cliConfigUrl = LogService.class.getResource(LogService.CLI_CONFIG);
  }
  
  @Test
  public void shouldPreferConfigInConfigurationFilePropertyIfSet() throws Exception {
    final File configFile = this.temporaryFolder.newFile("log4j2.xml");
    final String configFileName = configFile.toURI().toString();
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, configFileName);
    writeConfigFile(configFile, Level.DEBUG);
    
    LogService.reconfigure();
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isFalse();
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).isEqualTo(configFileName);
    assertThat(LogService.getLogger().getName()).isEqualTo(getClass().getName());
  }
  
  @Test
  public void shouldUseDefaultConfigIfNotConfigured() throws Exception {
    LogService.reconfigure();
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isTrue();
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).isEqualTo(this.defaultConfigUrl.toString());
  }
  
  @Test
  public void defaultConfigShouldBeLoadableAsResource() {
    final URL configUrlFromLogService = LogService.class.getResource(LogService.DEFAULT_CONFIG);
    final URL configUrlFromClassLoader = getClass().getClassLoader().getResource(LogService.DEFAULT_CONFIG.substring(1));
    final URL configUrlFromClassPathLoader = ClassPathLoader.getLatest().getResource(LogService.DEFAULT_CONFIG.substring(1));
    
    assertThat(configUrlFromLogService).isNotNull();
    assertThat(configUrlFromClassLoader).isNotNull();
    assertThat(configUrlFromClassPathLoader).isNotNull();
    assertThat(configUrlFromLogService)
        .isEqualTo(configUrlFromClassLoader)
        .isEqualTo(configUrlFromClassPathLoader);
  }
  
  @Test
  public void defaultConfigShouldIncludeStdout() {
    LogService.reconfigure();
    final Logger rootLogger = (Logger) LogService.getRootLogger();
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isTrue();
    assertThat(rootLogger.getAppenders().get(LogService.STDOUT)).isNotNull();
  }
  
  @Test
  public void removeConsoleAppenderShouldRemoveStdout() {
    LogService.reconfigure();
    final Logger rootLogger = (Logger) LogService.getRootLogger();
    
    LogService.removeConsoleAppender();
    
    assertThat(rootLogger.getAppenders().get(LogService.STDOUT)).isNull();
  }
  
  @Test
  public void restoreConsoleAppenderShouldRestoreStdout() {
    LogService.reconfigure();
    final Logger rootLogger = (Logger) LogService.getRootLogger();
    
    LogService.removeConsoleAppender();
    
    assertThat(rootLogger.getAppenders().get(LogService.STDOUT)).isNull();
    
    LogService.restoreConsoleAppender();

    assertThat(rootLogger.getAppenders().get(LogService.STDOUT)).isNotNull();
  }
  
  @Test
  public void removeAndRestoreConsoleAppenderShouldAffectRootLogger() {
    LogService.reconfigure();
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isTrue();
    
    final Logger rootLogger = (Logger) LogService.getRootLogger();
    
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
  public void intializeAfterUsingLoggerShouldReconfigure() {
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).as("log4j.configurationFile="+System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).isNullOrEmpty();
    
    Configurator.shutdown();
    
    LogManager.getRootLogger();

    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).as("log4j.configurationFile="+System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).isNullOrEmpty();
    
    LogService.reconfigure();
    LogService.initialize();
    
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).as("log4j.configurationFile="+System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).contains(LogService.DEFAULT_CONFIG);
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isTrue();
  }
  
  @Test
  public void cliConfigLoadsAsResource() {
    assertThat(this.cliConfigUrl).isNotNull();
    assertThat(this.cliConfigUrl.toString()).contains(LogService.CLI_CONFIG);
  }
  
  @Test
  public void defaultConfigLoadsAsResource() {
    assertThat(this.defaultConfigUrl).isNotNull();
    assertThat(this.defaultConfigUrl.toString()).contains(LogService.DEFAULT_CONFIG);
  }
  
  @Test
  public void shouldConvertConfigurationFilePropertyValueToURL() throws Exception {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, LogService.CLI_CONFIG);
    
    LogService.reconfigure();
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isFalse();
    assertThat(this.cliConfigUrl.toString()).contains(LogService.CLI_CONFIG);
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).isEqualTo(this.cliConfigUrl.toString());
    assertThat(LogService.getLogger().getName()).isEqualTo(getClass().getName());
  }
}
