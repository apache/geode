package com.gemstone.gemfire.test.golden;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Verifies that an example test should always pass even if the output contains
 * a severe that is expected.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class PassWithExpectedSevereJUnitTest extends PassWithExpectedProblemTestCase {
  
  @Override
  String problem() {
    return "severe";
  }
  
  @Override
  void outputProblemInProcess(final String message) {
    new LocalLogWriter(LogWriterImpl.INFO_LEVEL).severe(message);
  }
  
  public static void main(final String[] args) throws Exception {
    new PassWithExpectedSevereJUnitTest().executeInProcess();
  }
}
