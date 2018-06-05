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

package org.apache.geode.test.dunit.rules.tests;

import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.sun.tools.javac.util.List;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.core.Predicate;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class MemberStarterRuleAwaitIntegrationTest {

  private static MemberStarterRule locatorStarterRule = new LocatorStarterRule();
  private static MemberStarterRule serverStarterRule = new ServerStarterRule();

  @Parameter(0)
  public MemberStarterRule ruleToUse;

  @Parameter(1)
  public String ruleToUseAsString;

  @Parameters(name = "{index}: Testing {1}")
  public static Collection<Object[]> useBothRules() {
    return Arrays.asList(
        new Object[][] {
            {locatorStarterRule, locatorStarterRule.getClass().getSimpleName()},
            {serverStarterRule, serverStarterRule.getClass().getSimpleName()}});
  }

  @Test
  public void testWithDefaultPresentation() throws Exception {
    Callable<Boolean> alwaysFalseProvider = () -> false;
    Predicate<Boolean> booleanIdentityPredicate = b -> b.equals(true);
    String description = "Awaiting until boolean becomes true.";
    assertThatThrownBy(() -> ruleToUse.waitUntilSatisfied(alwaysFalseProvider,
        booleanIdentityPredicate, description, 1, TimeUnit.SECONDS))
            .isInstanceOf(ConditionTimeoutException.class)
            .hasMessageContaining("false")
            .hasMessageContaining(description);
  }

  @Test
  public void waitCanAcceptNullsIfPredicateAcceptsNulls() throws Exception {
    Callable<Boolean> alwaysNullProvider = () -> null;
    Predicate<Boolean> booleanIdentityPredicate = b -> b != null && b.equals(true);
    String description = "Awaiting until boolean becomes not null and also true.";
    assertThatThrownBy(() -> ruleToUse.waitUntilSatisfied(alwaysNullProvider,
        booleanIdentityPredicate, description, 1, TimeUnit.SECONDS))
            .isInstanceOf(ConditionTimeoutException.class)
            .hasMessageContaining("null")
            .hasMessageContaining(description);
  }

  @Test
  public void waitCanPrintMoreComplexResults() throws Exception {
    Callable<List<String>> abcListProvider = () -> List.of("A", "B", "C");
    Predicate<List<String>> isListEmptyPredicate = l -> l.isEmpty();
    String description = "Awaiting until list becomes empty.";
    assertThatThrownBy(() -> ruleToUse.waitUntilSatisfied(abcListProvider, isListEmptyPredicate,
        description, 1, TimeUnit.SECONDS))
            .isInstanceOf(ConditionTimeoutException.class)
            .hasMessageContaining("A,B,C")
            .hasMessageContaining(description);
  }
}
