package com.gemstone.gemfire.test.golden;

import org.apache.logging.log4j.Logger;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Verifies that test output containing an unexpected ERROR message
 * will fail with that ERROR message as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class FailWithLoggerErrorInOutputJUnitTest extends FailWithProblemInOutputTestCase {
  
  public FailWithLoggerErrorInOutputJUnitTest() {
    super(FailWithLoggerErrorInOutputJUnitTest.class.getSimpleName());
  }
  
  @Override
  String problem() {
    return "ExpectedStrings: Description of a problem.";
  }
  
  @Override
  void outputProblem(String message) {
    Logger logger = LogService.getLogger();
    logger.error(message);
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithLoggerErrorInOutputJUnitTest().execute();
  }
}
