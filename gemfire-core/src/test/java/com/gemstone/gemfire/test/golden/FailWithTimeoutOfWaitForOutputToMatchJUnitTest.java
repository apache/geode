package com.gemstone.gemfire.test.golden;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.process.ProcessWrapper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.AssertionFailedError;

@Category(UnitTest.class)
public class FailWithTimeoutOfWaitForOutputToMatchJUnitTest extends FailOutputTestCase {
  
  public FailWithTimeoutOfWaitForOutputToMatchJUnitTest() {
    super("FailWithTimeoutOfWaitForOutputToMatchJUnitTest");
  }
  
  @Override
  String problem() {
    return "This is an extra line";
  }
  
  @Override
  void outputProblem(String message) {
    System.out.println(message);
  }
  
  public void testFailWithTimeoutOfWaitForOutputToMatch() throws Exception {
    // output has an extra line and should fail
    final ProcessWrapper process = createProcessWrapper(getClass());
    process.execute(createProperties());
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    try {
      process.waitForOutputToMatch(problem());
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionFailedError expected) {
      assertTrue(expected.getMessage().contains(problem()));
    }
    // the following should generate no failures if timeout and tearDown are all working properly
    assertNotNull(process);
    assertTrue(process.isAlive());
    tearDown();
    process.waitFor();
  }
  
  public static void main(String[] args) throws Exception {
    new FailWithTimeoutOfWaitForOutputToMatchJUnitTest().execute();
  }
}
