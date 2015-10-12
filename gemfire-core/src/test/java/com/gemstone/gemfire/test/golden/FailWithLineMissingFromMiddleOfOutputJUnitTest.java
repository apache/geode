package com.gemstone.gemfire.test.golden;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.process.ProcessWrapper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Verifies that test output missing an expected line (at the middle 
 * of the golden file) will fail with that line as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class FailWithLineMissingFromMiddleOfOutputJUnitTest extends FailOutputTestCase {

  @Override
  String problem() {
    return "This line is missing in actual output.";
  }
  
  @Override
  void outputProblemInProcess(final String message) {
    // this tests that the message is missing from output
  }
  
  @Test
  public void testFailWithLineMissingFromEndOfOutput() throws Exception {
    final String goldenString = 
        "Begin " + name() + ".main" + "\n" +
        "Press Enter to continue." + "\n" +
        problem() + "\n" +
        "End " + name() + ".main" + "\n";
    debug(goldenString, "GOLDEN");

    final ProcessWrapper process = createProcessWrapper(new ProcessWrapper.Builder(), getClass());
    process.execute(createProperties());
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    process.waitForOutputToMatch("Press Enter to continue\\.");
    process.sendInput();
    process.waitFor();
    
    try {
      assertOutputMatchesGoldenFile(process.getOutput(), goldenString);
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionError expected) {
      assertTrue("AssertionFailedError message should contain \"" + problem() + "\"", 
          expected.getMessage().contains(problem()));
    }
  }
  
  public static void main(final String[] args) throws Exception {
    new FailWithLineMissingFromMiddleOfOutputJUnitTest().executeInProcess();
  }
}
