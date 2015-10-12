package com.gemstone.gemfire.test.golden;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.process.ProcessWrapper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Basic unit testing of the golden testing framework. This tests an 
 * example which is expected to always pass.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class PassJUnitTest extends GoldenTestCase implements ExecutableProcess {
  
  @Override
  protected GoldenComparator createGoldenComparator() {
    return new GoldenStringComparator(expectedProblemLines());
  }

  String name() {
    return getClass().getSimpleName();
  }

  /**
   * Process output has no problems and should pass
   */
  @Test
  public void testPass() throws Exception {
    final String goldenString = 
        "Begin " + name() + ".main" + "\n" + 
        "Press Enter to continue." + "\n" +
        "End " + name() + ".main" + "\n";
    
    final ProcessWrapper process = createProcessWrapper(new ProcessWrapper.Builder(), getClass());
    process.execute(createProperties());
    assertTrue(process.isAlive());
    
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    process.waitForOutputToMatch("Press Enter to continue\\.");
    process.sendInput();
    process.waitForOutputToMatch("End " + name() + "\\.main");
    process.waitFor();
    
    assertOutputMatchesGoldenFile(process, goldenString);
    assertFalse(process.isAlive());
    assertFalse(process.getStandardOutReader().isAlive());
    assertFalse(process.getStandardErrorReader().isAlive());
  }

  @Override
  public final void executeInProcess() throws IOException {
    outputLine("Begin " + name() + ".main");
    outputLine("Press Enter to continue.");
    new BufferedReader(new InputStreamReader(System.in)).readLine();
    outputLine("End " + name() + ".main");
  }
  
  public static void main(final String[] args) throws Exception {
    new PassJUnitTest().executeInProcess();
  }
}
