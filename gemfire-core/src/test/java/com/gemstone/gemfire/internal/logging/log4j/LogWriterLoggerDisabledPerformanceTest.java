package com.gemstone.gemfire.internal.logging.log4j;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

public class LogWriterLoggerDisabledPerformanceTest extends LogWriterLoggerPerformanceTest {

  public LogWriterLoggerDisabledPerformanceTest(String name) {
    super(name);
  }

  protected PerformanceLogger createPerformanceLogger() throws IOException {
    final Logger logger = createLogger();
    
    final PerformanceLogger perfLogger = new PerformanceLogger() {
      @Override
      public void log(String message) {
        logger.debug(message);
      }
      @Override
      public boolean isEnabled() {
        return logger.isEnabled(Level.DEBUG);
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
