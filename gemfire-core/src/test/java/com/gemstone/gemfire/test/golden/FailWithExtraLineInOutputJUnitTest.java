package com.gemstone.gemfire.test.golden;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.process.ProcessWrapper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Verifies that test output containing an unexpected extra line
 * will fail with that line as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class FailWithExtraLineInOutputJUnitTest extends FailOutputTestCase {
  
  @Override
  String problem() {
    return "This is an extra line";
  }
  
  @Override
  void outputProblemInProcess(final String message) {
    System.out.println(message);
  }
  
  /**
   * Process output has an extra line and should fail
   */
  @Test
  public void testFailWithExtraLineInOutput() throws Exception {
    final String goldenString = 
        "Begin " + name() + ".main" + "\n" +
        "Press Enter to continue." + "\n" + 
        "End " + name() + ".main" + "\n";
    debug(goldenString, "GOLDEN");

    final ProcessWrapper process = createProcessWrapper(new ProcessWrapper.Builder(), getClass());
    process.execute(createProperties());
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    process.waitForOutputToMatch("Press Enter to continue\\.");
    process.sendInput();
    process.waitForOutputToMatch("End " + name() + "\\.main");
    process.waitFor();
    
    try {
      assertOutputMatchesGoldenFile(process.getOutput(), goldenString);
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionError expected) {
      assertTrue(expected.getMessage().contains(problem()));
    }
  }
  
  public static void main(final String[] args) throws Exception {
    new FailWithExtraLineInOutputJUnitTest().executeInProcess();
  }
}
