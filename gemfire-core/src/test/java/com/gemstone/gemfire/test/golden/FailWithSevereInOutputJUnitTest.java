package com.gemstone.gemfire.test.golden;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Verifies that test output containing an unexpected severe message
 * will fail with that severe message as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class FailWithSevereInOutputJUnitTest extends FailWithProblemInOutputTestCase {
  
  @Override
  String problem() {
    return "ExpectedStrings: Description of a problem.";
  }
  
  @Override
  void outputProblemInProcess(final String message) {
    new LocalLogWriter(LogWriterImpl.INFO_LEVEL).severe(message);
  }
  
  public static void main(final String[] args) throws Exception {
    new FailWithSevereInOutputJUnitTest().executeInProcess();
  }
}
