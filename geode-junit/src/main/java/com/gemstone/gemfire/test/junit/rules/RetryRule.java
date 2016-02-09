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

import java.io.Serializable;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.gemstone.gemfire.test.junit.Retry;

/**
 * JUnit Rule that enables retrying a failed test up to a maximum number of retries.
 * </p> 
 * RetryRule can be used globally for all tests in a test case by specifying a 
 * retryCount when instantiating it:
 * <pre>
 * {@literal @}Rule
 * public final RetryRule retryRule = new RetryRule(3);
 * 
 * {@literal @}Test
 * public void shouldBeRetriedUntilPasses() {
 *   ...
 * }
 * </pre>
 * </p> 
 * The above will result in 3 retries for every test in the test case.
 * </p> 
 * RetryRule can be used locally for specific tests by annotating the test 
 * method with {@literal @}Rule and specifying a retryCount for that test:
 * <pre>
 * {@literal @}Rule
 * public final RetryRule retryRule = new RetryRule();
 * 
 * {@literal @}Test
 * {@literal @}Retry(3)
 * public void shouldBeRetriedUntilPasses() {
 *   ...
 * }
 * </pre>
 * </p>
 * This version of RetryRule will retry a test that fails because of any kind 
 * of Throwable.
 */
@SuppressWarnings("serial")
public class RetryRule implements TestRule, Serializable {
  
  /**
   * Enables printing of failures to System.err even if test passes on a retry
   */
  private static final boolean LOG = false;
  
  private final AbstractRetryRule implementation;

  public RetryRule() {
    this.implementation = new LocalRetryRule();
  }

  public RetryRule(final int retryCount) {
    this.implementation = new GlobalRetryRule(retryCount);
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return this.implementation.apply(base, description);
  }

  protected abstract class AbstractRetryRule implements TestRule {
    protected AbstractRetryRule() {
    }
    protected void evaluate(final Statement base, final Description description, final int retryCount) throws Throwable {
      if (retryCount == 0) {
        
      }
      Throwable caughtThrowable = null;
      
      for (int count = 0; count < retryCount; count++) {
        try {
          base.evaluate();
          return;
        } catch (Throwable t) {
          caughtThrowable = t;
          debug(description.getDisplayName() + ": run " + (count + 1) + " failed");
        }
      }
      
      debug(description.getDisplayName() + ": giving up after " + retryCount + " failures");
      throw caughtThrowable;
    }
    private void debug(final String message) {
      if (LOG) {
        System.err.println(message);
      }
    }
  }
  
  /**
   * Implementation of RetryRule for all test methods in a test case
   */
  protected class GlobalRetryRule extends AbstractRetryRule {
    
    private final int retryCount;

    protected GlobalRetryRule(final int retryCount) {
      if (retryCount < 1) {
        throw new IllegalArgumentException("Retry count must be greater than zero");
      }
      this.retryCount = retryCount;
    }
    
    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          GlobalRetryRule.this.evaluatePerCase(base, description);
        }
      };
    }

    protected void evaluatePerCase(final Statement base, final Description description) throws Throwable {
      evaluate(base, description, this.retryCount);
    }
  }

  /**
   * Implementation of RetryRule for test methods annotated with Retry
   */
  protected class LocalRetryRule extends AbstractRetryRule {
    
    protected LocalRetryRule() {
    }
    
    @Override
    public Statement apply(final Statement base, final Description description) {
      return new Statement() {
        @Override 
        public void evaluate() throws Throwable {
          LocalRetryRule.this.evaluatePerTest(base, description);
        }
      };
    }

    protected void evaluatePerTest(final Statement base, final Description description) throws Throwable {
      if (isTest(description)) {
        Retry retry = description.getAnnotation(Retry.class);
        int retryCount = getRetryCount(retry);
        evaluate(base, description, retryCount);
      }
    }

    private int getRetryCount(final Retry retry) {
      int retryCount = Retry.DEFAULT;

      if (retry != null) {
        retryCount = retry.value();
      }

      return retryCount;
    }

    private boolean isTest(final Description description) {
      return (description.isSuite() || description.isTest());
    }
  }
}
