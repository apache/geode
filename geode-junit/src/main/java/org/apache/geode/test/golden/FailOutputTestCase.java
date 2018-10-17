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

/**
 * Abstract test case for tests verifying that unexpected test output will cause expected failures.
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
  public void executeInProcess() throws IOException {
    outputLine("Begin " + name() + ".main");
    outputLine("Press Enter to continue.");
    new BufferedReader(new InputStreamReader(System.in)).readLine();
    outputProblemInProcess(problem());
    outputLine("End " + name() + ".main");
  }
}
