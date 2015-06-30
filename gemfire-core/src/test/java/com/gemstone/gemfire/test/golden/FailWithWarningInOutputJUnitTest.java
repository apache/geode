package com.gemstone.gemfire.test.golden;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Verifies that test output containing an unexpected warning message
 * will fail with that warning message as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class FailWithWarningInOutputJUnitTest extends FailWithProblemInOutputTestCase {
  
  public FailWithWarningInOutputJUnitTest() {
    super(FailWithWarningInOutputJUnitTest.class.getSimpleName());
  }
  
  @Override
  String problem() {
    return "ExpectedStrings: Description of a problem.";
  }
  
  @Override
  void outputProblem(String message) {
    LogWriter logWriter = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);
    logWriter.warning(message);
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithWarningInOutputJUnitTest().execute();
  }
}
