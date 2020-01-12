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
package org.apache.geode.test.junit.rules;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A base class for Rules that require {@code Description} to set up an external resource before
 * a test and tear it down afterward. {@code DescribedExternalResource} is similar to
 * {@code ExternalResource} but includes {@code Description} as a parameter to both {@code before}
 * and {@code after}.
 *
 * <p>
 * {@code Description} allows the implementation to have access to the test class, its annotations
 * and information about JUnit lifecycle.
 */
public abstract class DescribedExternalResource implements TestRule {

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before(description);
        try {
          base.evaluate();
        } finally {
          after(description);
        }
      }
    };
  }

  /**
   * Override to set up your specific external resource.
   *
   * @throws Throwable if setup fails (which will prevent the invocation of {@code after})
   */
  protected void before(Description description) throws Throwable {
    // do nothing
  }

  /**
   * Override to tear down your specific external resource. Note: ExternalResource after
   * does not include {@code throws Throwable}.
   */
  protected void after(Description description) throws Throwable {
    // do nothing
  }
}
