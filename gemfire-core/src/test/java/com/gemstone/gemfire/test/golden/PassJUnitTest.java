package com.gemstone.gemfire.test.golden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.process.ProcessWrapper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Basic unit testing of the golden testing framework. This tests an 
 * example which is expected to always pass.
 * 
 * @author Kirk Lund
 */
@Category(UnitTest.class)
public class PassJUnitTest extends GoldenTestCase {
  
  public PassJUnitTest() {
    super("PassJUnitTest");
  }

  @Override
  protected GoldenComparator createGoldenComparator() {
    return new GoldenStringComparator(expectedProblemLines());
  }

  String name() {
    return getClass().getSimpleName();
  }

  public void testPass() throws InterruptedException, IOException {
    // output has no problems and should pass
    final ProcessWrapper process = createProcessWrapper(getClass());
    process.execute(createProperties());
    assertTrue(process.isAlive());
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    process.waitForOutputToMatch("Press Enter to continue\\.");
    process.sendInput();
    process.waitForOutputToMatch("End " + name() + "\\.main");
    process.waitFor();
    String goldenString = "Begin " + name() + ".main" + "\n" 
        + "Press Enter to continue." + "\n" 
        + "End " + name() + ".main" + "\n";
    assertOutputMatchesGoldenFile(process, goldenString);

    assertFalse(process.isAlive());
    //assertFalse(process.getOutputReader());
    assertFalse(process.getStandardOutReader().isAlive());
    assertFalse(process.getStandardErrorReader().isAlive());
  }

  void execute() throws IOException {
    System.out.println("Begin " + name() + ".main");
    System.out.println("Press Enter to continue.");
    BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
    inputReader.readLine();
    System.out.println("End " + name() + ".main");
  }
  
  public static void main(String[] args) throws Exception {
    new PassJUnitTest().execute();
  }
}
