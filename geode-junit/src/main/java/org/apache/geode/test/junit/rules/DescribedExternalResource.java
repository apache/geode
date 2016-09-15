/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.test.junit.rules;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * this class extends the capability of JUnit's ExternalResource in that
 * it provides a Description object in the before and after methods, so that
 * the implementation would have access to the annotation of the test methods
 */
public class DescribedExternalResource implements TestRule {
  public Statement apply(Statement base, Description description) {
    return statement(base, description);
  }

  private Statement statement(final Statement base, final Description description) {
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
   * @throws Throwable if setup fails (which will disable {@code after}
   */
  protected void before(Description description) throws Throwable {
    // do nothing
  }

  /**
   * Override to tear down your specific external resource.
   */
  protected void after(Description description) throws Throwable {
    // do nothing
  }
}
