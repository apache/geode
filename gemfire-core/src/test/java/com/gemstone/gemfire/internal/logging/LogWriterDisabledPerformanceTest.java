package com.gemstone.gemfire.internal.logging;

import com.gemstone.gemfire.LogWriter;

public class LogWriterDisabledPerformanceTest extends LogWriterPerformanceTest {

  public LogWriterDisabledPerformanceTest(String name) {
    super(name);
  }

  @Override
  protected PerformanceLogger createPerformanceLogger() {
    final LogWriter logWriter = createLogWriter();
    
    final PerformanceLogger perfLogger = new PerformanceLogger() {
      @Override
      public void log(final String message) {
        logWriter.fine(message);
      }
      @Override
      public boolean isEnabled() {
        return logWriter.fineEnabled();
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
