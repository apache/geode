package com.gemstone.gemfire.test.golden;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Verifies that an example test should always pass even if the output contains
 * a error that is expected.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class PassWithExpectedErrorJUnitTest extends PassWithExpectedProblemTestCase {
  
  public PassWithExpectedErrorJUnitTest() {
    super("PassWithExpectedErrorJUnitTest");
  }
  
  @Override
  String problem() {
    return "error";
  }
  
  @Override
  void outputProblem(String message) {
    LogWriter logWriter = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);
    logWriter.error(message);
  }
  
  public static void main(String[] args) throws Exception {
    new PassWithExpectedErrorJUnitTest().execute();
  }
}
