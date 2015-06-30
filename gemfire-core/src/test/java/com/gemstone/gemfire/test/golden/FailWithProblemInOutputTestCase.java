package com.gemstone.gemfire.test.golden;

import java.io.IOException;

import com.gemstone.gemfire.test.process.ProcessWrapper;

import junit.framework.AssertionFailedError;

/**
 * Abstract test case for tests verifying that test output with a
 * log message of warning/error/severe will cause expected failures.
 * 
 * @author Kirk Lund
 */
public abstract class FailWithProblemInOutputTestCase extends FailOutputTestCase {

  FailWithProblemInOutputTestCase(String name) {
    super(name);
  }
  
  @Override
  protected String[] expectedProblemLines() {
    return new String[] { ".*" + name() + ".*" };
  }
  
  public void testFailWithProblemLogMessageInOutput() throws InterruptedException, IOException {
    final ProcessWrapper process = createProcessWrapper(getClass());
    process.execute(createProperties());
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    process.waitForOutputToMatch("Press Enter to continue\\.");
    process.sendInput();
    process.waitForOutputToMatch("End " + name() + "\\.main");
    process.waitFor();
    String goldenString = "Begin " + name() + ".main" + "\n" 
        + "Press Enter to continue." + "\n" 
        + "End " + name() + ".main" + "\n";
    innerPrintOutput(goldenString, "GOLDEN");
    try {
      assertOutputMatchesGoldenFile(process.getOutput(), goldenString);
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionFailedError expected) {
//      System.out.println("Problem: " + problem());
//      System.out.println("AssertionFailedError message: " + expected.getMessage());
      assertTrue(expected.getMessage().contains(problem()));
    }
  }
}
