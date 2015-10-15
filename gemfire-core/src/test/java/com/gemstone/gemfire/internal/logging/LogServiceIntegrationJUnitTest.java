package com.gemstone.gemfire.internal.logging;

import static com.gemstone.gemfire.internal.logging.LogServiceIntegrationTestSupport.*;
import static org.assertj.core.api.Assertions.*;

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

import com.gemstone.gemfire.internal.logging.log4j.Configurator;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for LogService and how it configures and uses log4j2
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class LogServiceIntegrationJUnitTest {
  
  private static final String DEFAULT_CONFIG_FILE_NAME = "log4j2.xml";
  private static final String CLI_CONFIG_FILE_NAME = "log4j2-cli.xml";
  
  @Rule
  public final SystemErrRule systemErrRule = new SystemErrRule().enableLog();
  
  @Rule
  public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();
  
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
      
      System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
      if (beforeConfigFileProp != null) {
        System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, beforeConfigFileProp);
      }
      StatusLogger.getLogger().setLevel(beforeLevel);
      
      LogService.reconfigure();
      assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isTrue();
    }
  };
  
  private String beforeConfigFileProp;
  private Level beforeLevel;
  
  private URL defaultConfigUrl;
  private URL cliConfigUrl;
  
  @Before
  public void setUp() {
    this.defaultConfigUrl = LogService.class.getResource(LogService.DEFAULT_CONFIG);
    this.cliConfigUrl = LogService.class.getResource(LogService.CLI_CONFIG);
  }
  
  @After
  public void after() {
    // if either of these fail then log4j2 probably logged a failure to stdout
    assertThat(this.systemErrRule.getLog()).isEmpty();
    assertThat(this.systemOutRule.getLog()).isEmpty();
  }
  
  @Test
  public void shouldPreferConfigurationFilePropertyIfSet() throws Exception {
    final File configFile = this.temporaryFolder.newFile(DEFAULT_CONFIG_FILE_NAME);
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
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).isNullOrEmpty();
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
  public void shouldNotUseDefaultConfigIfCliConfigSpecified() throws Exception {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, this.cliConfigUrl.toString());
    
    LogService.reconfigure();
  
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isFalse();
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).isEqualTo(this.cliConfigUrl.toString());
    assertThat(LogService.getLogger().getName()).isEqualTo(getClass().getName());
  }

  @Test
  public void isUsingGemFireDefaultConfigShouldBeTrueIfDefaultConfig() throws Exception {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, this.defaultConfigUrl.toString());
    
    assertThat(LogService.getConfiguration().getConfigurationSource().toString()).contains(DEFAULT_CONFIG_FILE_NAME);
    assertThat(LogService.isUsingGemFireDefaultConfig()).isTrue();
  }

  @Test
  public void isUsingGemFireDefaultConfigShouldBeFalseIfCliConfig() throws Exception {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, this.cliConfigUrl.toString());
    
    assertThat(LogService.getConfiguration().getConfigurationSource().toString()).doesNotContain(DEFAULT_CONFIG_FILE_NAME);
    assertThat(LogService.isUsingGemFireDefaultConfig()).isFalse();
  }

  @Test
  public void shouldUseCliConfigIfCliConfigIsSpecifiedViaClasspath() throws Exception {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, "classpath:"+CLI_CONFIG_FILE_NAME);
    
    assertThat(LogService.getConfiguration().getConfigurationSource().toString()).contains(CLI_CONFIG_FILE_NAME);
    assertThat(LogService.isUsingGemFireDefaultConfig()).isFalse();
  }
}
