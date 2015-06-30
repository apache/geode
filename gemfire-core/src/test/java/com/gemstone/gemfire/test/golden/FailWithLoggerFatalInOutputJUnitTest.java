package com.gemstone.gemfire.test.golden;

import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Verifies that test output containing an unexpected FATAL message
 * will fail with that FATAL message as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class FailWithLoggerFatalInOutputJUnitTest extends FailWithProblemInOutputTestCase {
  
  public FailWithLoggerFatalInOutputJUnitTest() {
    super(FailWithLoggerFatalInOutputJUnitTest.class.getSimpleName());
  }
  
  @Override
  String problem() {
    return "ExpectedStrings: Description of a problem.";
  }
  
  @Override
  void outputProblem(String message) {
    Logger logger = LogService.getLogger();
    logger.fatal(message);
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithLoggerFatalInOutputJUnitTest().execute();
  }
}
