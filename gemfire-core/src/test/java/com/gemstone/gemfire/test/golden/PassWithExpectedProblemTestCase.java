package com.gemstone.gemfire.test.golden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.gemstone.gemfire.test.process.ProcessWrapper;

/**
 * Abstract test case for golden testing framework. This provides basis for
 * unit tests which involve an example that is expected to always pass.
 * 
 * @author Kirk Lund
 */
public abstract class PassWithExpectedProblemTestCase extends GoldenTestCase {

  PassWithExpectedProblemTestCase(String name) {
    super(name);
  }
  
  @Override
  protected GoldenComparator createGoldenComparator() {
    return new GoldenStringComparator(expectedProblemLines());
  }

  @Override
  protected String[] expectedProblemLines() {
    return new String[] { ".*" + name() + ".*", "^\\[" + problem() + ".*\\] ExpectedStrings: This is an expected problem in the output" };
  }
  
  String name() {
    return getClass().getSimpleName();
  }
  
  abstract String problem();
  
  abstract void outputProblem(String message);
  
  public void testPassWithExpectedProblem() throws InterruptedException, IOException {
    // output has an expected warning/error/severe message and should pass
    final ProcessWrapper process = createProcessWrapper(getClass());
    process.execute(createProperties());
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    process.waitForOutputToMatch("Press Enter to continue\\.");
    process.sendInput();
    process.waitForOutputToMatch("End " + name() + "\\.main");
    process.waitFor();
    String goldenString = "Begin " + name() + ".main" + "\n" 
        + "Press Enter to continue." + "\n" 
        + "\n"
        + "^\\[" + problem() + ".*\\] ExpectedStrings: This is an expected problem in the output" + "\n"
        + "End " + name() + ".main" + "\n";
    innerPrintOutput(goldenString, "GOLDEN");
    String[] printMe = expectedProblemLines();
    for (String str : printMe) {
      System.out.println(str);
    }
    assertOutputMatchesGoldenFile(process.getOutput(), goldenString);
  }
  
  void execute() throws IOException {
    System.out.println("Begin " + name() + ".main");
    System.out.println("Press Enter to continue.");
    BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
    inputReader.readLine();
    outputProblem("ExpectedStrings: This is an expected problem in the output");
    System.out.println("End " + name() + ".main");
  }
}
