package com.gemstone.gemfire.test.golden;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Verifies that test output containing an unexpected WARN message
 * will fail with that WARN message as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class FailWithLoggerWarnInOutputJUnitTest extends FailWithProblemInOutputTestCase {
  
  @Override
  String problem() {
    return "ExpectedStrings: Description of a problem.";
  }
  
  @Override
  void outputProblemInProcess(final String message) {
    LogService.getLogger().warn(message);
  }
  
  public static void main(final String[] args) throws Exception {
    new FailWithLoggerWarnInOutputJUnitTest().executeInProcess();
  }
}
