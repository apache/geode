package com.gemstone.gemfire.test.golden;

import static org.junit.Assert.*;

import org.junit.Test;

import com.gemstone.gemfire.test.process.ProcessWrapper;

/**
 * Abstract test case for tests verifying that test output with a
 * log message of warning/error/severe will cause expected failures.
 * 
 * @author Kirk Lund
 */
public abstract class FailWithProblemInOutputTestCase extends FailOutputTestCase {

  @Override
  protected String[] expectedProblemLines() {
    return new String[] { ".*" + name() + ".*" };
  }
  
  @Test
  public void testFailWithProblemLogMessageInOutput() throws Exception {
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
}
