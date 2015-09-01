package com.gemstone.gemfire.internal.logging;

import static junitparams.JUnitParamsRunner.$;
import static org.assertj.core.api.Assertions.*;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.gemstone.gemfire.internal.logging.log4j.AppenderContext;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * Unit tests for LogService
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
@RunWith(JUnitParamsRunner.class)
public class LogServiceJUnitTest {
  
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Test
  public void getAppenderContextShouldHaveEmptyName() throws Exception {
    final AppenderContext appenderContext = LogService.getAppenderContext();
    
    assertThat(appenderContext.getName()).isEmpty();
  }

  @Test
  public void getAppenderContextWithNameShouldHaveName() throws Exception {
    final String name = "someName";
    final AppenderContext appenderContext = LogService.getAppenderContext(name);
    
    assertThat(appenderContext.getName()).isEqualTo(name);
  }
  
  @Test
  public void isUsingGemFireDefaultConfigShouldBeTrueIfDefaultConfig() {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, LogService.DEFAULT_CONFIG);
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).isTrue();
  }

  @Test
  public void isUsingGemFireDefaultConfigShouldBeFalseIfEmpty() {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, "");
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).isFalse();
  }

  @Test
  public void isUsingGemFireDefaultConfigShouldBeFalseIfNull() {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, "");
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).isFalse();
  }

  @Test
  public void isUsingGemFireDefaultConfigShouldBeFalseIfCliConfig() {
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, LogService.CLI_CONFIG);
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).isFalse();
  }
  
  @Test
  @Parameters(method = "getToLevelParameters")
  public void toLevelShouldReturnMatchingLog4jLevel(final int intLevel, final Level level) {
    assertThat(LogService.toLevel(intLevel)).isSameAs(level);
  }

  @SuppressWarnings("unused")
  private static final Object[] getToLevelParameters() {
    return $(
        new Object[] { 0, Level.OFF },
        new Object[] { 100, Level.FATAL },
        new Object[] { 200, Level.ERROR },
        new Object[] { 300, Level.WARN },
        new Object[] { 400, Level.INFO },
        new Object[] { 500, Level.DEBUG },
        new Object[] { 600, Level.TRACE },
        new Object[] { Integer.MAX_VALUE, Level.ALL }
    );
  }
}
