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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

@Category({OQLQueryTest.class})
public class ArithmeticOperationsIntegrationTest {
  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule().withAutoStart();

  private Cache cache;
  QueryService queryService;

  @Before
  public void setup() {
    String regionName = "portfolio";

    cache = serverStarterRule.getCache();
    assertNotNull(cache);
    Region region =
        cache.createRegionFactory().setDataPolicy(DataPolicy.REPLICATE).create(regionName);

    Portfolio p = new Portfolio(1);
    region.put(1, p);
    Portfolio p2 = new Portfolio(2);
    region.put(2, p2);
    Portfolio p3 = new Portfolio(3);
    region.put(3, p3);

    queryService = cache.getQueryService();
  }

  @Test
  public void modOnNonNumericShouldThrowException() throws Exception {
    try {
      queryService.newQuery("select * from /portfolio p where p % 2 = 1").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Arithmetic Operations can only be applied to numbers", qite.getMessage());
    }
  }

  @Test
  public void modOnNullShouldThrowException() throws Exception {
    try {
      queryService.newQuery("select * from /portfolio p where null % 2 = 1").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Cannot evaluate arithmetic operations on null values", qite.getMessage());
    }
  }

  @Test
  public void modOnNumericShouldApplyOperationAndReturnMatchingResults() throws Exception {
    SelectResults modPercentageResults = (SelectResults) queryService
        .newQuery("select * from /portfolio p where p.ID % 2 = 1").execute();

    SelectResults modKeywordResults = (SelectResults) queryService
        .newQuery("select * from /portfolio p where p.ID MOD 2 = 1").execute();

    assertEquals(2, modPercentageResults.size());
    assertEquals(modPercentageResults.size(), modKeywordResults.size());
  }

  @Test
  public void modOperationAsProjectionShouldCorrectlyModReturnValue() throws Exception {
    SelectResults modPercentageResults = (SelectResults) queryService
        .newQuery("select (ID % 2) from /portfolio p where p.ID % 2 = 1").execute();

    SelectResults modKeywordResults = (SelectResults) queryService
        .newQuery("select ID mod 2 from /portfolio p where p.ID MOD 2 = 1").execute();

    assertEquals(2, modPercentageResults.size());
    assertEquals(modPercentageResults.size(), modKeywordResults.size());
    for (Object o : modKeywordResults) {
      assertEquals(1, o);
    }
  }

  @Test
  public void addOperationShouldApplyOpInQuery() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select * from /portfolio p where p.ID + 1 = 2").execute();
    assertEquals(1, results.size());
  }

  @Test
  public void addOperationShouldThrowExceptionWhenAddingNonNumeric() throws Exception {
    try {
      SelectResults results = (SelectResults) queryService
          .newQuery("select * from /portfolio p where p.ID + '1' = 2").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Arithmetic Operations can only be applied to numbers", qite.getMessage());
    }
  }

  @Test
  public void addOperationShouldThrowExceptionWhenAddingNull() throws Exception {
    try {
      SelectResults results = (SelectResults) queryService
          .newQuery("select * from /portfolio p where p.ID + null = 2").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Cannot evaluate arithmetic operations on null values", qite.getMessage());
    }
  }

  @Test
  public void subtractOperationShouldThrowExceptionWhenAddingNonNumeric() throws Exception {
    try {
      SelectResults results = (SelectResults) queryService
          .newQuery("select * from /portfolio p where p.ID - '1' = 2").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Arithmetic Operations can only be applied to numbers", qite.getMessage());
    }
  }

  @Test
  public void subtractOperationShouldThrowExceptionWhenAddingNull() throws Exception {
    try {
      SelectResults results = (SelectResults) queryService
          .newQuery("select * from /portfolio p where p.ID - null = 2").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Cannot evaluate arithmetic operations on null values", qite.getMessage());
    }
  }

  @Test
  public void subtractOperationShouldCorrectlySubtractInQuery() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select * from /portfolio p where (p.ID - 1) = 2").execute();

    assertEquals(1, results.size());
  }

  @Test
  public void multiplyOperationShouldThrowExceptionWhenAddingNonNumeric() throws Exception {
    try {
      SelectResults results = (SelectResults) queryService
          .newQuery("select * from /portfolio p where p.ID * '1' = 2").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Arithmetic Operations can only be applied to numbers", qite.getMessage());
    }
  }

  @Test
  public void multiplyOperationShouldThrowExceptionWhenAddingNull() throws Exception {
    try {
      SelectResults results = (SelectResults) queryService
          .newQuery("select * from /portfolio p where p.ID * null = 2").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Cannot evaluate arithmetic operations on null values", qite.getMessage());
    }
  }

  @Test
  public void multiplyOperationShouldCorrectlyExecuteInQuery() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select * from /portfolio p where (p.ID * 1) = 3").execute();

    assertEquals(1, results.size());
  }

  @Test
  public void divideOperationShouldThrowExceptionWhenDividingNonNumeric() throws Exception {
    try {
      SelectResults results = (SelectResults) queryService
          .newQuery("select * from /portfolio p where p.ID / '1' = 2").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Arithmetic Operations can only be applied to numbers", qite.getMessage());
    }
  }

  @Test
  public void divideOperationShouldThrowExceptionWhenDividingNull() throws Exception {
    try {
      SelectResults results = (SelectResults) queryService
          .newQuery("select * from /portfolio p where p.ID / null = 2").execute();
      fail();
    } catch (TypeMismatchException qite) {
      assertEquals("Cannot evaluate arithmetic operations on null values", qite.getMessage());
    }
  }

  @Test
  public void divideByZeroShouldThrowException() throws Exception {
    try {
      SelectResults results = (SelectResults) queryService
          .newQuery("select * from /portfolio p where (p.ID / 0) = 3").execute();
      fail();
    } catch (QueryInvocationTargetException e) {
      assertTrue(e.getCause() instanceof ArithmeticException);
    }
  }

  @Test
  public void divideOperationShouldCorrectlyExecuteInQuery() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select * from /portfolio p where (p.ID / 1) = 3").execute();

    assertEquals(1, results.size());
  }

  @Test
  public void combinationOfArithmeticOperationsShouldExecuteOperationsAndNotThrowException()
      throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery(
            "select * from /portfolio p where (p.ID - 1 + 1 - 1 + 1 - 1 + 1 * 1 / 1 - 1 + 1 - 1) = 2")
        .execute();

    assertEquals(1, results.size());
  }

  @Test
  public void arithmeticOperationsRetainCorrectOperatorPrecedenceWithAdditionAndMultiplication()
      throws Exception {
    SelectResults results =
        (SelectResults) queryService.newQuery("select 1 + 2 * 3 from /portfolio p").execute();

    assertEquals(7, results.iterator().next());
  }

  @Test
  public void arithmeticOperationsRetainCorrectOperatorPrecedenceWhenAdditionClauseIsAtEnd()
      throws Exception {
    SelectResults results =
        (SelectResults) queryService.newQuery("select 2 * 3 + 1 from /portfolio p").execute();

    assertEquals(7, results.iterator().next());
  }

  @Test
  public void arithmeticOperationsRetainCorrectOperatorPrecedenceWithParenthesis()
      throws Exception {
    SelectResults results =
        (SelectResults) queryService.newQuery("select (1 + 2) * 3 from /portfolio p").execute();

    assertEquals(9, results.iterator().next());
  }

  @Test
  public void arithmeticOperationsRetainCorrectOperatorPrecedence() throws Exception {
    SelectResults results = (SelectResults) queryService
        .newQuery("select 1 + 2 * 12 - 4 / 2 + 10 from /portfolio p").execute();

    assertEquals(33, results.iterator().next());
  }
}
