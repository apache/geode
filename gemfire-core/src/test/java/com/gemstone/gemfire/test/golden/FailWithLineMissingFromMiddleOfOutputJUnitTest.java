package com.gemstone.gemfire.test.golden;

import java.io.IOException;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.process.ProcessWrapper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.AssertionFailedError;

/**
 * Verifies that test output missing an expected line (at the middle 
 * of the golden file) will fail with that line as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class FailWithLineMissingFromMiddleOfOutputJUnitTest extends FailOutputTestCase {

  public FailWithLineMissingFromMiddleOfOutputJUnitTest() {
    super("FailWithLineMissingFromMiddleOfOutputJUnitTest");
  }
  
  @Override
  String problem() {
    return "This line is missing in actual output.";
  }
  
  @Override
  void outputProblem(String message) {
    // this tests that the message is missing from output
  }
  
  public void testFailWithLineMissingFromEndOfOutput() throws InterruptedException, IOException {
    final ProcessWrapper process = createProcessWrapper(getClass());
    process.execute(createProperties());
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    process.waitForOutputToMatch("Press Enter to continue\\.");
    process.sendInput();
    process.waitFor();
    String goldenString = "Begin " + name() + ".main" + "\n" 
        + "Press Enter to continue." + "\n" 
        + problem() + "\n"
        + "End " + name() + ".main" + "\n";
    innerPrintOutput(goldenString, "GOLDEN");
    try {
      assertOutputMatchesGoldenFile(process.getOutput(), goldenString);
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionFailedError expected) {
      assertTrue("AssertionFailedError message should contain \"" + problem() + "\"", 
          expected.getMessage().contains(problem()));
    }
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithLineMissingFromMiddleOfOutputJUnitTest().execute();
  }
}
