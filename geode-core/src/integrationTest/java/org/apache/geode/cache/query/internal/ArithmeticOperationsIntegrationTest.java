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
package org.apache.geode.cache.query.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(OQLQueryTest.class)
public class ArithmeticOperationsIntegrationTest {
  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule().withAutoStart();
  private QueryService queryService;

  @Before
  public void setup() {
    Cache cache = serverStarterRule.getCache();
    assertThat(cache).isNotNull();
    Region<Integer, Portfolio> region = cache.<Integer, Portfolio>createRegionFactory()
        .setDataPolicy(DataPolicy.REPLICATE).create("portfolio");
    Portfolio p = new Portfolio(1);
    region.put(1, p);
    Portfolio p2 = new Portfolio(2);
    region.put(2, p2);
    Portfolio p3 = new Portfolio(3);
    region.put(3, p3);

    queryService = cache.getQueryService();
  }

  @Test
  public void modOnNonNumericShouldThrowException() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where p % 2 = 1").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Arithmetic Operations can only be applied to numbers");
  }

  @Test
  public void modOnNullShouldThrowException() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where null % 2 = 1").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Cannot evaluate arithmetic operations on null values");
  }

  @Test
  public void modOnNumericShouldApplyOperationAndReturnMatchingResults() throws Exception {
    SelectResults modPercentageResults = (SelectResults) queryService
        .newQuery("select * from /portfolio p where p.ID % 2 = 1").execute();
    SelectResults modKeywordResults = (SelectResults) queryService
        .newQuery("select * from /portfolio p where p.ID MOD 2 = 1").execute();

    assertThat(modPercentageResults.size()).isEqualTo(2);
    assertThat(modKeywordResults.size()).isEqualTo(modPercentageResults.size());
  }

  @Test
  public void modOperationAsProjectionShouldCorrectlyModReturnValue() throws Exception {
    SelectResults modPercentageResults = (SelectResults) queryService
        .newQuery("select (ID % 2) from /portfolio p where p.ID % 2 = 1").execute();
    SelectResults modKeywordResults = (SelectResults) queryService
        .newQuery("select ID mod 2 from /portfolio p where p.ID MOD 2 = 1").execute();

    assertThat(modPercentageResults.size()).isEqualTo(2);
    assertThat(modKeywordResults.size()).isEqualTo(modPercentageResults.size());
    for (Object o : modKeywordResults)
      assertThat(o).isEqualTo(1);
  }

  @Test
  public void addOperationShouldApplyOpInQuery() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select * from /portfolio p where p.ID + 1 = 2").execute();
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void addOperationShouldThrowExceptionWhenAddingNonNumeric() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where p.ID + '1' = 2").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Arithmetic Operations can only be applied to numbers");
  }

  @Test
  public void addOperationShouldThrowExceptionWhenAddingNull() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where p.ID + null = 2").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Cannot evaluate arithmetic operations on null values");
  }

  @Test
  public void subtractOperationShouldThrowExceptionWhenAddingNonNumeric() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where p.ID - '1' = 2").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Arithmetic Operations can only be applied to numbers");
  }

  @Test
  public void subtractOperationShouldThrowExceptionWhenAddingNull() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where p.ID - null = 2").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Cannot evaluate arithmetic operations on null values");
  }

  @Test
  public void subtractOperationShouldCorrectlySubtractInQuery() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select * from /portfolio p where (p.ID - 1) = 2").execute();
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void multiplyOperationShouldThrowExceptionWhenAddingNonNumeric() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where p.ID * '1' = 2").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Arithmetic Operations can only be applied to numbers");
  }

  @Test
  public void multiplyOperationShouldThrowExceptionWhenAddingNull() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where p.ID * null = 2").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Cannot evaluate arithmetic operations on null values");
  }

  @Test
  public void multiplyOperationShouldCorrectlyExecuteInQuery() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select * from /portfolio p where (p.ID * 1) = 3").execute();
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void divideOperationShouldThrowExceptionWhenDividingNonNumeric() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where p.ID / '1' = 2").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Arithmetic Operations can only be applied to numbers");
  }

  @Test
  public void divideOperationShouldThrowExceptionWhenDividingNull() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where p.ID / null = 2").execute())
            .isInstanceOf(TypeMismatchException.class)
            .hasMessage("Cannot evaluate arithmetic operations on null values");
  }

  @Test
  public void divideByZeroShouldThrowException() {
    assertThatThrownBy(
        () -> queryService.newQuery("select * from /portfolio p where (p.ID / 0) = 3").execute())
            .isInstanceOf(QueryInvocationTargetException.class)
            .hasCauseInstanceOf(ArithmeticException.class);
  }

  @Test
  public void divideOperationShouldCorrectlyExecuteInQuery() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select * from /portfolio p where (p.ID / 1) = 3").execute();
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void combinationOfArithmeticOperationsShouldExecuteOperationsAndNotThrowException()
      throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery(
            "select * from /portfolio p where (p.ID - 1 + 1 - 1 + 1 - 1 + 1 * 1 / 1 - 1 + 1 - 1) = 2")
        .execute();
    assertThat(results.size()).isEqualTo(1);
  }

  @Test
  public void arithmeticOperationsRetainCorrectOperatorPrecedenceWithAdditionAndMultiplication()
      throws Exception {
    SelectResults results =
        (SelectResults) queryService.newQuery("select 1 + 2 * 3 from /portfolio p").execute();
    assertThat(results.iterator().next()).isEqualTo(7);
  }

  @Test
  public void arithmeticOperationsRetainCorrectOperatorPrecedenceWhenAdditionClauseIsAtEnd()
      throws Exception {
    SelectResults results =
        (SelectResults) queryService.newQuery("select 2 * 3 + 1 from /portfolio p").execute();
    assertThat(results.iterator().next()).isEqualTo(7);
  }

  @Test
  public void arithmeticOperationsRetainCorrectOperatorPrecedenceWithParenthesis()
      throws Exception {
    SelectResults results =
        (SelectResults) queryService.newQuery("select (1 + 2) * 3 from /portfolio p").execute();
    assertThat(results.iterator().next()).isEqualTo(9);
  }

  @Test
  public void arithmeticOperationsRetainCorrectOperatorPrecedence() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select 1 + 2 * 12 - 4 / 2 + 10 from /portfolio p").execute();
    assertThat(results.iterator().next()).isEqualTo(33);
  }
}
