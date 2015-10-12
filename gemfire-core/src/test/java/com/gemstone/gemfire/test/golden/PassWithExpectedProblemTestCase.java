package com.gemstone.gemfire.test.golden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;

import com.gemstone.gemfire.test.process.ProcessWrapper;

/**
 * Abstract test case for golden testing framework. This provides basis for
 * unit tests which involve an example that is expected to always pass.
 * 
 * @author Kirk Lund
 */
public abstract class PassWithExpectedProblemTestCase extends GoldenTestCase implements ExecutableProcess {

  private int problemLine; 
  
  @Override
  protected GoldenComparator createGoldenComparator() {
    return new GoldenStringComparator(expectedProblemLines());
  }

  @Override
  protected String[] expectedProblemLines() {
    this.problemLine = 1; 
    return new String[] { 
        ".*" + name() + ".*", 
        "^\\[" + problem() + ".*\\] ExpectedStrings: This is an expected problem in the output" 
    };
  }
  
  String name() {
    return getClass().getSimpleName();
  }
  
  abstract String problem();
  
  abstract void outputProblemInProcess(String message);
  
  /**
   * Process output has an expected warning/error/severe message and should pass
   */
  @Test
  public void testPassWithExpectedProblem() throws Exception {
    final String goldenString = 
        "Begin " + name() + ".main" + "\n" + 
        "Press Enter to continue." + "\n" +
        "\n" +
        expectedProblemLines()[this.problemLine] + "\n" +
        "End " + name() + ".main" + "\n";
    debug(goldenString, "GOLDEN");

    final ProcessWrapper process = createProcessWrapper(new ProcessWrapper.Builder(), getClass());
    process.execute(createProperties());
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    process.waitForOutputToMatch("Press Enter to continue\\.");
    process.sendInput();
    process.waitForOutputToMatch("End " + name() + "\\.main");
    process.waitFor();
    
    assertOutputMatchesGoldenFile(process.getOutput(), goldenString);
  }
  
  @Override
  public final void executeInProcess() throws IOException {
    outputLine("Begin " + name() + ".main");
    outputLine("Press Enter to continue.");
    new BufferedReader(new InputStreamReader(System.in)).readLine();
    outputProblemInProcess("ExpectedStrings: This is an expected problem in the output");
    outputLine("End " + name() + ".main");
  }
}
