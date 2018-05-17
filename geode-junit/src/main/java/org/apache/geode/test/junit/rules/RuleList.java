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

import java.util.ArrayList;
import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * The {@code RuleList} rule enables ordering of TestRules.
 *
 * <p>
 * Example:
 *
 * <pre>
 * public class SomeTest {
 *
 *   {@literal @}Rule
 *   public RuleList rules = new RuleList().add(new FirstRule()
 *                                         .add(new SecondRule()
 *                                         .add(new ThirdRule();
 * </pre>
 */
public class RuleList implements TestRule {

  private final List<TestRule> rules = new ArrayList<>();

  /**
   * Creates an empty {@code RuleList}.
   */
  public RuleList() {}

  /**
   * Creates a {@code RuleList} containing a single {@link TestRule}.
   *
   * @param rule the first rule of the {@code RuleList}
   */
  public RuleList(final TestRule rule) {
    this.rules.add(rule);
  }

  /**
   * Creates a new {@code RuleList} containing the specified {@link TestRule}s.
   *
   * @param rules the list of {@code TestRule}s to add
   */
  protected RuleList(final List<TestRule> rules) {
    this.rules.addAll(rules);
  }

  /**
   * Adds a new {@code TestRule} to the end of the current {@code RuleList}.
   *
   * @param rule the rule to add.
   * @return the {@code RuleList} with a new TestRule added
   */
  public RuleList add(final TestRule rule) {
    this.rules.add(rule);
    return this;
  }

  @Override
  public Statement apply(Statement base, final Description description) {
    for (TestRule each : this.rules) {
      base = each.apply(base, description);
    }
    return base;
  }

  /**
   * Returns a reference to the actual list of {@code TestRule}s. For use by subclasses and tests.
   */
  protected List<TestRule> rules() {
    return this.rules;
  }
}
