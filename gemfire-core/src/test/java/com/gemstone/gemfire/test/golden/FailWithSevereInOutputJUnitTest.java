package com.gemstone.gemfire.test.golden;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Verifies that test output containing an unexpected severe message
 * will fail with that severe message as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class FailWithSevereInOutputJUnitTest extends FailWithProblemInOutputTestCase {
  
  public FailWithSevereInOutputJUnitTest() {
    super(FailWithSevereInOutputJUnitTest.class.getSimpleName());
  }
  
  @Override
  String problem() {
    return "ExpectedStrings: Description of a problem.";
  }
  
  @Override
  void outputProblem(String message) {
    LogWriter logWriter = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);
    logWriter.severe(message);
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithSevereInOutputJUnitTest().execute();
  }
}
