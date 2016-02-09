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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Serializable version of TemporaryFolder JUnit Rule. JUnit lifecycle is not
 * executed in remote JVMs.
 * 
 * Implementation copied from <tt>org.junit.rules.RuleChain</tt>.
 * 
 * The SerializableRuleChain rule allows ordering of TestRules. You create a
 * {@code RuleChain} with {@link #outerRule(TestRule)} and subsequent calls of
 * {@link #around(TestRule)}:
 *
 * <pre>
 * public static class UseRuleChain {
 *  &#064;Rule
 *  public RuleChain chain= RuleChain
 *                         .outerRule(new LoggingRule("outer rule")
 *                         .around(new LoggingRule("middle rule")
 *                         .around(new LoggingRule("inner rule");
 *
 *  &#064;Test
 *  public void example() {
 *    assertTrue(true);
 *     }
 * }
 * </pre>
 *
 * writes the log
 *
 * <pre>
 * starting outer rule
 * starting middle rule
 * starting inner rule
 * finished inner rule
 * finished middle rule
 * finished outer rule
 * </pre>
 *
 * @author Kirk Lund
 */
@SuppressWarnings("serial")
public class SerializableRuleChain implements SerializableTestRule {
  private static final SerializableRuleChain EMPTY_CHAIN = new SerializableRuleChain(Collections.<TestRule>emptyList());

  private transient List<TestRule> rulesStartingWithInnerMost;

  /**
  * Returns a {@code SerializableRuleChain} without a {@link TestRule}. This method may
  * be the starting point of a {@code SerializableRuleChain}.
  *
  * @return a {@code SerializableRuleChain} without a {@link TestRule}.
  */
  public static SerializableRuleChain emptyRuleChain() {
    return EMPTY_CHAIN;
  }

  /**
  * Returns a {@code SerializableRuleChain} with a single {@link TestRule}. This method
  * is the usual starting point of a {@code SerializableRuleChain}.
  *
  * @param outerRule the outer rule of the {@code SerializableRuleChain}.
  * @return a {@code SerializableRuleChain} with a single {@link TestRule}.
  */
  public static SerializableRuleChain outerRule(TestRule outerRule) {
    return emptyRuleChain().around(outerRule);
  }

  private SerializableRuleChain(List<TestRule> rules) {
    this.rulesStartingWithInnerMost = rules;
  }

  /**
  * Create a new {@code SerializableRuleChain}, which encloses the {@code nextRule} with
  * the rules of the current {@code SerializableRuleChain}.
  *
  * @param enclosedRule the rule to enclose.
  * @return a new {@code SerializableRuleChain}.
  */
  public SerializableRuleChain around(TestRule enclosedRule) {
    List<TestRule> rulesOfNewChain = new ArrayList<TestRule>();
    rulesOfNewChain.add(enclosedRule);
    rulesOfNewChain.addAll(rulesStartingWithInnerMost);
    return new SerializableRuleChain(rulesOfNewChain);
  }

  /**
  * {@inheritDoc}
  */
  public Statement apply(Statement base, Description description) {
    for (TestRule each : rulesStartingWithInnerMost) {
      base = each.apply(base, description);
    }
    return base;
  }
}
