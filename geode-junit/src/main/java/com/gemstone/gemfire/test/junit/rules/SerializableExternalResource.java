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
package com.gemstone.gemfire.test.junit.rules;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Serializable version of ExternalResource JUnit Rule. JUnit lifecycle is not
 * executed in remote JVMs. The <tt>after()</tt> callback has a throws-clause 
 * that matches <tt>before()</tt>.
 * 
 * Implementation copied from <tt>org.junit.rules.ExternalResource</tt>.
 * 
 * @author Kirk Lund
 */
@SuppressWarnings("serial")
public abstract class SerializableExternalResource implements SerializableTestRule {
  
  @Override
  public Statement apply(final Statement base, final Description description) {
    if (description.isTest()) {
      return statement(base);
    } else if (description.isSuite()) {
      return statementClass(base);
    }
    return base;
  }

  private Statement statement(final Statement base) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before();
        try {
          base.evaluate();
        } finally {
          after();
        }
      }
    };
  }

  private Statement statementClass(final Statement base) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        beforeClass();
        try {
          base.evaluate();
        } finally {
          afterClass();
        }
      }
    };
  }

  /**
   * Override to set up your specific external resource.
   *
   * @throws Throwable if setup fails (which will disable {@code after}
   */
  protected void before() throws Throwable {
    // do nothing
  }

  /**
   * Override to tear down your specific external resource.
   * 
   * @throws Throwable if teardown fails (which will disable {@code after}
   */
  protected void after() throws Throwable {
    // do nothing
  }

  /**
   * Override to set up your specific external resource.
   *
   * @throws Throwable if setup fails (which will disable {@code after}
   */
  protected void beforeClass() throws Throwable {
    // do nothing
  }

  /**
   * Override to tear down your specific external resource.
   *
   * @throws Throwable if teardown fails (which will disable {@code after}
   */
  protected void afterClass() throws Throwable {
    // do nothing
  }
}
