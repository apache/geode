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
/*
 * LogicalOperatorsJUnitTest.java JUnit based test
 *
 * Created on March 11, 2005, 12:50 PM
 */

package org.apache.geode.cache.query.functional;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class LogicalOperatorsJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for (int i = 0; i < 5; i++) {
      region.put("" + i, new Portfolio(i));
    }
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  Object[] validOperands = {Boolean.TRUE, Boolean.FALSE, null, QueryService.UNDEFINED};

  Object[] invalidOperands = {0, "a"};


  @Test
  public void testAND() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    Object[] params = new Object[2];
    for (final Object operand : validOperands) {
      for (final Object validOperand : validOperands) {
        Query query = qs.newQuery("$1 AND $2");
        params[0] = operand;
        params[1] = validOperand;
        Object result = query.execute(params);
        // CacheUtils.log("LogicalTest "+validOperands[i]+" AND "+validOperands[j]+" = "+result+"
        // "+checkResult("AND", result, validOperands[i], validOperands[j]));
        if (!checkResult("AND", result, operand, validOperand)) {
          fail(operand + " AND " + validOperand + " returns " + result);
        }
      }
    }
    for (final Object validOperand : validOperands) {
      for (int j = 0; j < invalidOperands.length; j++) {
        Query query = qs.newQuery("$1 AND $2");
        params[0] = validOperand;
        params[1] = invalidOperands[j];
        try {
          Object result = query.execute(params);
          fail(validOperand + " AND " + validOperands[j] + " returns " + result);
        } catch (Exception ignored) {

        }
      }
    }
  }

  @Test
  public void testOR() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    Object[] params = new Object[2];
    for (final Object operand : validOperands) {
      for (final Object validOperand : validOperands) {
        Query query = qs.newQuery("$1 OR $2");
        params[0] = operand;
        params[1] = validOperand;
        Object result = query.execute(params);
        // CacheUtils.log("LogicalTest "+validOperands[i]+" OR "+validOperands[j]+" = "+result+"
        // "+checkResult("OR", result, validOperands[i], validOperands[j]));
        if (!checkResult("OR", result, operand, validOperand)) {
          fail(operand + " OR " + validOperand + " returns " + result);
        }
      }
    }

    for (final Object validOperand : validOperands) {
      for (int j = 0; j < invalidOperands.length; j++) {
        Query query = qs.newQuery("$1 OR $2");
        params[0] = validOperand;
        params[1] = invalidOperands[j];
        try {
          Object result = query.execute(params);
          fail(validOperand + " OR " + validOperands[j] + " returns " + result);
        } catch (Exception ignored) {

        }
      }
    }
  }

  @Test
  public void testNOT() throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    for (int i = 0; i < validOperands.length; i++) {
      Query query = qs.newQuery("NOT $" + (i + 1));
      Object result = query.execute(validOperands);
      // CacheUtils.log("LogicalTest "+"NOT "+validOperands[i]+" = "+result+" "+checkResult("NOT",
      // result, validOperands[i], null));
      if (!checkResult("NOT", result, validOperands[i], null)) {
        fail("NOT " + validOperands[i] + " returns " + result);
      }
    }

    for (int j = 0; j < invalidOperands.length; j++) {
      Query query = qs.newQuery("NOT $" + (j + 1));
      try {
        Object result = query.execute(invalidOperands);
        fail("NOT " + invalidOperands[j] + " returns " + result);
      } catch (Exception ignored) {

      }
    }
  }

  private boolean checkResult(String operator, Object result, Object operand1, Object operand2) {
    try {
      Object temp;
      if (operand1 == null) {
        temp = operand1;
        operand1 = operand2;
        operand2 = temp;
      }
      if (operand1 == null) {
        return result.equals(QueryService.UNDEFINED);
      }

      if (operator.equalsIgnoreCase("AND")) {

        if (operand1.equals(Boolean.FALSE)) {
          return result.equals(Boolean.FALSE);
        }

        if (operand2 != null && operand2.equals(Boolean.FALSE)) {
          return result.equals(Boolean.FALSE);
        }

        if (operand1 == QueryService.UNDEFINED || operand2 == QueryService.UNDEFINED) {
          return result.equals(QueryService.UNDEFINED);
        }

        if (operand2 == null) {
          return result.equals(QueryService.UNDEFINED);
        }

        return result.equals(Boolean.TRUE);

      } else if (operator.equalsIgnoreCase("OR")) {

        if (operand1.equals(Boolean.TRUE)) {
          return result.equals(Boolean.TRUE);
        }

        if (operand2 != null && operand2.equals(Boolean.TRUE)) {
          return result.equals(Boolean.TRUE);
        }

        if (operand1 == QueryService.UNDEFINED || operand2 == QueryService.UNDEFINED) {
          return result == QueryService.UNDEFINED;
        }

        if (/* operand1 == null || not possible */ operand2 == null) {
          return result == QueryService.UNDEFINED;
        }

        return result.equals(Boolean.FALSE);

      } else if (operator.equalsIgnoreCase("NOT")) {
        if (operand1 instanceof Boolean) {
          return ((Boolean) result).booleanValue() != ((Boolean) operand1).booleanValue();
        }
        return result == QueryService.UNDEFINED;
      }
    } catch (Exception ignored) {
    }
    return false;
  }
}
