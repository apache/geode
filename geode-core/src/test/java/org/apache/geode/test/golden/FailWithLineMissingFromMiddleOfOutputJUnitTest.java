/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.test.golden;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.process.ProcessWrapper;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Verifies that test output missing an expected line (at the middle 
 * of the golden file) will fail with that line as the failure message.
 */
@Category(IntegrationTest.class)
public class FailWithLineMissingFromMiddleOfOutputJUnitTest extends FailOutputTestCase {

  @Override
  String problem() {
    return "This line is missing in actual output.";
  }
  
  @Override
  void outputProblemInProcess(final String message) {
    // this tests that the message is missing from output
  }
  
  @Test
  public void testFailWithLineMissingFromEndOfOutput() throws Exception {
    final String goldenString = 
        "Begin " + name() + ".main" + "\n" +
        "Press Enter to continue." + "\n" +
        problem() + "\n" +
        "End " + name() + ".main" + "\n";
    debug(goldenString, "GOLDEN");

    final ProcessWrapper process = createProcessWrapper(new ProcessWrapper.Builder(), getClass());
    process.execute(createProperties());
    process.waitForOutputToMatch("Begin " + name() + "\\.main");
    process.waitForOutputToMatch("Press Enter to continue\\.");
    process.sendInput();
    process.waitFor();
    
    try {
      assertOutputMatchesGoldenFile(process.getOutput(), goldenString);
      fail("assertOutputMatchesGoldenFile should have failed due to " + problem());
    } catch (AssertionError expected) {
      assertTrue("AssertionError message should contain \"" + problem() + "\"",
          expected.getMessage().contains(problem()));
    }
  }
  
  public static void main(final String[] args) throws Exception {
    new FailWithLineMissingFromMiddleOfOutputJUnitTest().executeInProcess();
  }
}
