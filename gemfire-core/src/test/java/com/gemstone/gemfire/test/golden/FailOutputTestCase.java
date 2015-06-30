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
public abstract class FailOutputTestCase extends GoldenTestCase {
  
  FailOutputTestCase(String name) {
    super(name);
  }
  
  @Override
  protected GoldenComparator createGoldenComparator() {
    return new GoldenStringComparator(expectedProblemLines());
  }

  String name() {
    return getClass().getSimpleName();
  }
  
  abstract String problem();
  
  abstract void outputProblem(String message);
  
  void execute() throws IOException {
    System.out.println("Begin " + name() + ".main");
    System.out.println("Press Enter to continue.");
    BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
    inputReader.readLine();
    outputProblem(problem());
    System.out.println("End " + name() + ".main");
  }
}
