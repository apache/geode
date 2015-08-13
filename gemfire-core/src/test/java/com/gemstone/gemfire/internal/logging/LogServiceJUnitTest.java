package com.gemstone.gemfire.internal.logging;

import static junitparams.JUnitParamsRunner.$;
import static org.assertj.core.api.Assertions.*;

import java.net.URL;

import org.apache.logging.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.gemstone.gemfire.internal.ClassPathLoader;
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
  
  private URL defaultConfigUrl;
  private URL cliConfigUrl;
  
  @Before
  public void setUp() {
    this.defaultConfigUrl = LogService.class.getResource(LogService.DEFAULT_CONFIG);
    this.cliConfigUrl = LogService.class.getResource(LogService.CLI_CONFIG);
  }
  
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
  @Parameters(method = "getToLevelParameters")
  public void toLevelShouldReturnMatchingLog4jLevel(final int intLevel, final Level level) {
    assertThat(LogService.toLevel(intLevel)).isSameAs(level);
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
