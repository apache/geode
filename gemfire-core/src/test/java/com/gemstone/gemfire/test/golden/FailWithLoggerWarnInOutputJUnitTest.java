package com.gemstone.gemfire.test.golden;

import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Verifies that test output containing an unexpected WARN message
 * will fail with that WARN message as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class FailWithLoggerWarnInOutputJUnitTest extends FailWithProblemInOutputTestCase {
  
  public FailWithLoggerWarnInOutputJUnitTest() {
    super(FailWithLoggerWarnInOutputJUnitTest.class.getSimpleName());
  }
  
  @Override
  String problem() {
    return "ExpectedStrings: Description of a problem.";
  }
  
  @Override
  void outputProblem(String message) {
    Logger logger = LogService.getLogger();
    logger.warn(message);
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithLoggerWarnInOutputJUnitTest().execute();
  }
}
