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
