package com.gemstone.gemfire.test.golden;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Verifies that an example test should always pass even if the output contains
 * a severe that is expected.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class PassWithExpectedSevereJUnitTest extends PassWithExpectedProblemTestCase {
  
  public PassWithExpectedSevereJUnitTest() {
    super("PassWithExpectedSevereJUnitTest");
  }
  
  @Override
  String problem() {
    return "severe";
  }
  
  @Override
  void outputProblem(String message) {
    LogWriter logWriter = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);
    logWriter.severe(message);
  }
  
  public static void main(String[] args) throws Exception {
    new PassWithExpectedSevereJUnitTest().execute();
  }
}
