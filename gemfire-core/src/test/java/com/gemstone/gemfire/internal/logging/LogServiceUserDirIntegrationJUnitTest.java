package com.gemstone.gemfire.internal.logging;

import static com.gemstone.gemfire.internal.logging.LogServiceIntegrationTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ClearSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;

import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests LogService when a log4j2 config file is in the user.dir
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class LogServiceUserDirIntegrationJUnitTest {

  @Rule
  public final ClearSystemProperties clearConfigFileProperty = new ClearSystemProperties(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
  
  @Rule
  public final ExternalResource externalResource = new ExternalResource() {
    @Override
    protected void after() {
      LogService.reconfigure();
      assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isTrue();
    }
  };
  
  private File configFile;
  
  @Before
  public void setUp() throws Exception {
    this.configFile = new File(System.getProperty("user.dir"), "log4j2-test.xml");
    writeConfigFile(this.configFile, Level.DEBUG);
    LogService.reconfigure();
  }
  
  @After
  public void tearDown() {
    this.configFile.delete();
  }
  
  @Test
  public void shouldPreferConfigInCurrentDirectoryIfFound() throws Exception {
    // if working directory is in classpath this test will fail
    assumeFalse(isUserDirInClassPath());
    
    assertThat(LogService.isUsingGemFireDefaultConfig()).as(LogService.getConfigInformation()).isFalse();
    //ConfigurationFactory.getInstance().getConfiguration(null, null); TODO: delete
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY)).isEqualTo(this.configFile.toURI().toString());
  }
  
  private static boolean isUserDirInClassPath() throws MalformedURLException {
    return SystemUtils.isInClassPath(System.getProperty("user.dir"));
  }
}
