package com.gemstone.gemfire.internal.logging.log4j;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for FastLogger when using the default log4j2 config for GemFire.
 * 
 * @author Kirk Lund
 * @author David Hoots
 */
@Category(IntegrationTest.class)
public class FastLoggerWithDefaultConfigJUnitTest {

  private static final String TEST_LOGGER_NAME = FastLogger.class.getPackage().getName();
  
  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  private Logger logger;
  
  @Before
  public void setUp() throws Exception {
    System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    LogService.reconfigure();
  }
  
  /**
   * System property "log4j.configurationFile" should be "/com/gemstone/gemfire/internal/logging/log4j/log4j2-default.xml"
   */
  @Test
  public void configurationFilePropertyIsDefaultConfig() {
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY), isEmptyOrNullString());
  }
  
  /**
   * LogService isUsingGemFireDefaultConfig should be true
   */
  @Test
  public void isUsingGemFireDefaultConfig() {
    assertThat(LogService.isUsingGemFireDefaultConfig(), is(true));
  }
  
  /**
   * LogService getLogger should return loggers wrapped in FastLogger
   */
  @Test
  public void logServiceReturnsFastLoggers() {
    this.logger = LogService.getLogger(TEST_LOGGER_NAME);
    
    assertThat(this.logger, is(instanceOf(FastLogger.class)));
  }
  
  /**
   * FastLogger isDelegating should be false
   */
  @Test
  public void isDelegatingShouldBeFalse() {
    this.logger = LogService.getLogger(TEST_LOGGER_NAME);
    
    assertThat(((FastLogger)this.logger).isDelegating(), is(false));
  }
}
