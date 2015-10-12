package com.gemstone.gemfire.test.golden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Abstract test case for tests verifying that unexpected test output will
 * cause expected failures.
 * 
 * @author Kirk Lund
 */
public abstract class FailOutputTestCase extends GoldenTestCase implements ExecutableProcess {
  
  @Override
  protected GoldenComparator createGoldenComparator() {
    return new GoldenStringComparator(expectedProblemLines());
  }

  String name() {
    return getClass().getSimpleName();
  }
  
  abstract String problem();
  
  abstract void outputProblemInProcess(String message);
  
  @Override
  public final void executeInProcess() throws IOException {
    outputLine("Begin " + name() + ".main");
    outputLine("Press Enter to continue.");
    new BufferedReader(new InputStreamReader(System.in)).readLine();
    outputProblemInProcess(problem());
    outputLine("End " + name() + ".main");
  }
}
