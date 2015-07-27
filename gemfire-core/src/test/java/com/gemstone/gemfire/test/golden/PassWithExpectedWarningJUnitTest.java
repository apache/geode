package com.gemstone.gemfire.test.golden;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Verifies that an example test should always pass even if the output contains
 * a warning that is expected.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class PassWithExpectedWarningJUnitTest extends PassWithExpectedProblemTestCase {
  
  @Override
  String problem() {
    return "warning";
  }
  
  @Override
  void outputProblemInProcess(final String message) {
    new LocalLogWriter(LogWriterImpl.INFO_LEVEL).warning(message);
  }
  
  public static void main(final String[] args) throws Exception {
    new PassWithExpectedWarningJUnitTest().executeInProcess();
  }
}
