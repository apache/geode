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
package com.gemstone.gemfire.test.junit.rules.tests;

import static com.gemstone.gemfire.test.junit.rules.tests.TestRunner.*;
import static org.assertj.core.api.Assertions.*;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runners.model.Statement;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RuleAndClassRuleTest {

  @Test
  public void usingRuleAsRuleAndClassRuleShouldInvokeBeforeClass() {
    Result result = runTest(UsingRuleAsRuleAndClassRule.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(UsingRuleAsRuleAndClassRule.staticRule.beforeClassInvoked).isEqualTo(true);
  }
  
  @Test
  public void usingRuleAsRuleAndClassRuleShouldInvokeAfterClass() {
    Result result = runTest(UsingRuleAsRuleAndClassRule.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(UsingRuleAsRuleAndClassRule.staticRule.afterClassInvoked).isEqualTo(true);
  }

  @Test
  public void usingRuleAsRuleAndClassRuleShouldInvokeBefore() {
    Result result = runTest(UsingRuleAsRuleAndClassRule.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(UsingRuleAsRuleAndClassRule.staticRule.beforeInvoked).isEqualTo(true);
  }

  @Test
  public void usingRuleAsRuleAndClassRuleShouldInvokeAfter() {
    Result result = runTest(UsingRuleAsRuleAndClassRule.class);
    
    assertThat(result.wasSuccessful()).isTrue();
    assertThat(UsingRuleAsRuleAndClassRule.staticRule.afterInvoked).isEqualTo(true);
  }

  public static class SpyRule implements TestRule {
    boolean beforeClassInvoked;
    boolean afterClassInvoked;
    boolean beforeInvoked;
    boolean afterInvoked;
    
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
    
    private void beforeClass() {
      this.beforeClassInvoked = true;
    }
    
    private void afterClass() {
      this.afterClassInvoked = true;
    }
    
    private void before() {
      this.beforeInvoked = true;
    }
    
    private void after() {
      this.afterInvoked = true;
    }
  };
  
  public static class UsingRuleAsRuleAndClassRule {
    @ClassRule
    public static SpyRule staticRule = new SpyRule();
    @Rule
    public SpyRule rule = staticRule;
    @Test
    public void doTest() throws Exception {
    }
  }
}
