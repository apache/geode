package com.gemstone.gemfire.internal.logging;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.internal.util.IOUtils;

/**
 * Tests performance of logging when level is OFF.
 * 
 * @author Kirk Lund
 */
public class LogWriterPerformanceTest extends LoggingPerformanceTestCase {

  public LogWriterPerformanceTest(String name) {
    super(name);
  }
  
  protected Properties createGemFireProperties() {
    final Properties props = new Properties();
    this.logFile = new File(this.configDirectory, "gemfire.log");
    final String logFilePath = IOUtils.tryGetCanonicalPathElseGetAbsolutePath(logFile);
    props.setProperty(DistributionConfig.LOG_FILE_NAME, logFilePath);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    return props;
  }
  
  protected void writeProperties(final Properties props, final File file) throws IOException {
    final FileOutputStream out = new FileOutputStream(file);
    try {
      props.store(out, null);
    }
    finally {
      out.close();
    }
  }

  protected LogWriter createLogWriter() {
    final Properties props = createGemFireProperties();
    
    // create configuration with log-file and log-level
    //this.configDirectory = new File(getUniqueName());
    
    this.configDirectory.mkdir();
    assertTrue(this.configDirectory.isDirectory() && this.configDirectory.canWrite());

    //this.gemfireProperties = new File(this.configDirectory, "gemfire.properties");
    //writeProperties(props, this.gemfireProperties);
    
    final DistributionConfig config = new DistributionConfigImpl(props, false, false);
    
    // create a LogWriter that writes to log-file
    final boolean appendToFile = false;
    final boolean isLoner = true;
    final boolean isSecurityLog = false;
    final boolean logConfig = true;
    final FileOutputStream[] fosHolder = null;
    
    final LogWriter logWriter = TestLogWriterFactory.createLogWriter(
        appendToFile, isLoner, isSecurityLog, config, logConfig, fosHolder);
    return logWriter;
  }
  
  protected PerformanceLogger createPerformanceLogger() {
    final LogWriter logWriter = createLogWriter();
    
    final PerformanceLogger perfLogger = new PerformanceLogger() {
      @Override
      public void log(final String message) {
        logWriter.info(message);
      }
      @Override
      public boolean isEnabled() {
        return logWriter.infoEnabled();
      }
    };
    
    return perfLogger;
  }
  
  @Override
  public void testCountBasedLogging() throws Exception {
    super.testCountBasedLogging();
  }

  @Override
  public void testTimeBasedLogging() throws Exception {
    super.testTimeBasedLogging();
  }

  @Override
  public void testCountBasedIsEnabled() throws Exception {
    super.testCountBasedIsEnabled();
  }

  @Override
  public void testTimeBasedIsEnabled() throws Exception {
    super.testTimeBasedIsEnabled();
  }
}
