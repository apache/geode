package com.gemstone.gemfire.test.golden;

import java.io.IOException;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.process.ProcessWrapper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.AssertionFailedError;

/**
 * Verifies that test output containing an unexpected extra line
 * will fail with that line as the failure message.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class FailWithExtraLineInOutputJUnitTest extends FailOutputTestCase {
  
  public FailWithExtraLineInOutputJUnitTest() {
    super("FailWithExtraLineInOutputJUnitTest");
  }
  
  @Override
  String problem() {
    return "This is an extra line";
  }
  
  @Override
  void outputProblem(String message) {
    System.out.println(message);
  }
  
  public void testFailWithExtraLineInOutput() throws InterruptedException, IOException {
    // output has an extra line and should fail
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
      assertTrue(expected.getMessage().contains(problem()));
    }
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithExtraLineInOutputJUnitTest().execute();
  }
}
