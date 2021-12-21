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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.Result;

import org.apache.geode.test.junit.runners.TestRunner;

/**
 * Unit tests for {@link RuleList}.
 */
public class RuleListTest {

  private static AtomicInteger counter;
  private static Invocations[] invocations;

  @BeforeClass
  public static void setUpClass() {
    counter = new AtomicInteger();
    invocations = new Invocations[] {new Invocations(counter), new Invocations(counter),
        new Invocations(counter)};
  }

  @AfterClass
  public static void tearDownClass() {
    counter = null;
    invocations = null;
    ThreeRules.ruleListStatic = null;
  }

  @Test
  public void firstShouldBeFirstBeforeLastAfter() {
    Result result = TestRunner.runTest(ThreeRules.class);

    assertThat(result.wasSuccessful()).isTrue();

    assertThat(counter.get()).isEqualTo(9);

    assertThat(invocations[0].beforeInvocation).isEqualTo(1);
    assertThat(invocations[1].beforeInvocation).isEqualTo(2);
    assertThat(invocations[2].beforeInvocation).isEqualTo(3);

    assertThat(invocations[0].testInvocation).isEqualTo(4);
    assertThat(invocations[1].testInvocation).isEqualTo(5);
    assertThat(invocations[2].testInvocation).isEqualTo(6);

    assertThat(invocations[2].afterInvocation).isEqualTo(7);
    assertThat(invocations[1].afterInvocation).isEqualTo(8);
    assertThat(invocations[0].afterInvocation).isEqualTo(9);
  }

  /**
   * Used by test {@link #firstShouldBeFirstBeforeLastAfter()}
   */
  public static class ThreeRules {

    static RuleList ruleListStatic;

    public SpyRule ruleOne = new SpyRule("ruleOne", invocations[0]);
    public SpyRule ruleTwo = new SpyRule("ruleTwo", invocations[1]);
    public SpyRule ruleThree = new SpyRule("ruleThree", invocations[2]);

    @Rule
    public RuleList ruleList = new RuleList().add(ruleThree).add(ruleTwo).add(ruleOne);

    @Test
    public void doTest() throws Exception {
      ruleListStatic = ruleList;
      invocations[0].invokedTest();
      invocations[1].invokedTest();
      invocations[2].invokedTest();
    }
  }

  /**
   * Structure of rule callback and test invocations
   */
  public static class Invocations {

    private final AtomicInteger counter;
    int beforeInvocation = 0;
    int testInvocation = 0;
    int afterInvocation = 0;

    Invocations(AtomicInteger counter) {
      this.counter = counter;
    }

    void invokedTest() {
      testInvocation = counter.incrementAndGet();
    }

    void invokedBefore() {
      beforeInvocation = counter.incrementAndGet();
    }

    void invokedAfter() {
      afterInvocation = counter.incrementAndGet();
    }

    @Override
    public String toString() {
      return "Invocations{" + "counter=" + counter + ", beforeInvocation=" + beforeInvocation
          + ", testInvocation=" + testInvocation + ", afterInvocation=" + afterInvocation + '}';
    }
  }

  /**
   * Implementation of TestRule that records the order of callbacks invoked on it. Used by
   * {@link RuleListTest}.
   */
  public static class SpyRule extends ExternalResource {

    static SpyRuleBuilder builder() {
      return new SpyRuleBuilder();
    }

    private final String name;
    private final Invocations invocations;
    private final Throwable beforeThrowable;

    SpyRule(String name, Invocations invocations) {
      this.name = name;
      this.invocations = invocations;
      beforeThrowable = null;
    }

    SpyRule(SpyRuleBuilder builder) {
      name = builder.name;
      invocations = builder.invocations;
      beforeThrowable = builder.beforeThrowable;
    }

    Invocations invocations() {
      return invocations;
    }

    void test() {
      invocations.invokedTest();
    }

    @Override
    protected void before() throws Throwable {
      invocations.invokedBefore();
      if (beforeThrowable != null) {
        throw beforeThrowable;
      }
    }

    @Override
    protected void after() {
      invocations.invokedAfter();
    }

    @Override
    public String toString() {
      return "SpyRule{" + "name='" + name + '\'' + '}';
    }
  }

  /**
   * Builder for more control of constructing an instance of {@link SpyRule}
   */
  public static class SpyRuleBuilder {

    String name;
    Invocations invocations;
    Throwable beforeThrowable;

    SpyRuleBuilder withName(String name) {
      this.name = name;
      return this;
    }

    SpyRuleBuilder withInvocations(Invocations invocations) {
      this.invocations = invocations;
      return this;
    }

    SpyRuleBuilder beforeThrows(Throwable throwable) {
      beforeThrowable = throwable;
      return this;
    }

    SpyRule build() {
      return new SpyRule(this);
    }
  }
}
