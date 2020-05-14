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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.MemberStarterRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

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
    Supplier<Boolean> alwaysFalseProvider = () -> false;
    String description = "Awaiting until boolean becomes true.";

    assertThatThrownBy(printExceptionWrapper(() -> ruleToUse.waitUntilEqual(alwaysFalseProvider,
        UnaryOperator.identity(), true, description, 1, TimeUnit.SECONDS)))
            .isInstanceOf(ConditionTimeoutException.class)
            .hasMessageContaining("false")
            .hasMessageContaining(description);
  }

  @Test
  public void waitCanAcceptNullsIfPredicateAcceptsNulls() throws Exception {
    Supplier<Boolean> alwaysNullProvider = () -> null;
    Predicate<Boolean> booleanIdentityPredicate = b -> b != null && b.equals(true);
    String description = "Awaiting until boolean becomes not null and also true.";
    assertThatThrownBy(printExceptionWrapper(() -> ruleToUse.waitUntilEqual(alwaysNullProvider,
        UnaryOperator.identity(), true, description, 1, TimeUnit.SECONDS)))
            .isInstanceOf(ConditionTimeoutException.class)
            .hasMessageContaining("null")
            .hasMessageContaining(description);
  }

  @Test
  public void waitCanPrintMoreComplexResults() throws Exception {
    Supplier<List<String>> abcListProvider = () -> Arrays.asList("A", "B", "C");
    Function<List<String>, Integer> examiner = list -> list.size();
    String description = "Awaiting until list becomes empty.";
    assertThatThrownBy(printExceptionWrapper(() -> ruleToUse.waitUntilEqual(abcListProvider,
        examiner, 0, description, 1, TimeUnit.SECONDS)))
            .isInstanceOf(ConditionTimeoutException.class)
            .hasMessageContaining("A, B, C")
            .hasMessageContaining(description);
  }

  private ThrowingCallable printExceptionWrapper(ThrowingCallable throwingCallable) {
    return () -> {
      try {
        throwingCallable.call();
      } catch (Exception e) {
        System.out.println(e);
        throw (e);
      }
    };
  }
}
