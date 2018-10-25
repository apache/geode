/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.test.golden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;

import org.apache.geode.test.process.ProcessWrapper;

/**
 * Abstract test case for golden testing framework. This provides basis for unit tests which involve
 * an example that is expected to always pass.
 *
 */
public abstract class PassWithExpectedProblemTestCase extends GoldenTestCase
    implements ExecutableProcess {

  private int problemLine;

  @Override
  protected GoldenComparator createGoldenComparator() {
    return new GoldenStringComparator(expectedProblemLines());
  }

  @Override
  protected String[] expectedProblemLines() {
    this.problemLine = 1;
    return new String[] {".*" + name() + ".*",
        "^\\[" + problem() + ".*\\] ExpectedStrings: This is an expected problem in the output"};
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
        "Begin " + name() + ".main" + "\n" + "Press Enter to continue." + "\n" + "\n"
            + expectedProblemLines()[this.problemLine] + "\n" + "End " + name() + ".main" + "\n";
    GoldenTestCase.debug(goldenString, "GOLDEN");

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
  public void executeInProcess() throws IOException {
    outputLine("Begin " + name() + ".main");
    outputLine("Press Enter to continue.");
    new BufferedReader(new InputStreamReader(System.in)).readLine();
    outputProblemInProcess("ExpectedStrings: This is an expected problem in the output");
    outputLine("End " + name() + ".main");
  }
}
