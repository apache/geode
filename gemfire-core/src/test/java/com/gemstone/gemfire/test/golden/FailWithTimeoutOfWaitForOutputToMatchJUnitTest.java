package com.gemstone.gemfire.test.golden;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.process.ProcessWrapper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class FailWithTimeoutOfWaitForOutputToMatchJUnitTest extends FailOutputTestCase {
  
  private static final long timeoutMillis = 1000;

  private ProcessWrapper process;
  
  public void subTearDown() throws Exception {
    this.process.destroy();
  }
  
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
  public void testFailWithTimeoutOfWaitForOutputToMatch() throws Exception {
    this.process = createProcessWrapper(new ProcessWrapper.Builder().timeoutMillis(timeoutMillis), getClass());
    this.process.execute(createProperties());
    this.process.waitForOutputToMatch("Begin " + name() + "\\.main");
    
    try {
      this.process.waitForOutputToMatch(problem());
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionError expected) {
      assertTrue(expected.getMessage().contains(problem()));
    }
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithTimeoutOfWaitForOutputToMatchJUnitTest().executeInProcess();
  }
}
