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
 * ComparisonOperatorsJUnitTest.java JUnit based test
 *
 * Created on March 10, 2005, 3:14 PM
 */
package org.apache.geode.cache.query.functional;



import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;

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
public class ComparisonOperatorsJUnitTest {

  public ComparisonOperatorsJUnitTest() {}

  public String getName() {
    return getClass().getSimpleName();
  }

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  String[] operators = {"=", "<>", "!=", "<", "<=", ">", ">="};

  @Test
  public void testCompareWithInt() throws Exception {
    String var = "ID";
    int value = 2;
    QueryService qs = CacheUtils.getQueryService();
    for (int i = 0; i < operators.length; i++) {
      Query query =
          qs.newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where " + var
              + operators[i] + value);
      Object result = query.execute();
      if (result instanceof Collection) {
        Iterator iter = ((Collection) result).iterator();
        while (iter.hasNext()) {
          boolean isPassed = false;
          Portfolio p = (Portfolio) iter.next();
          switch (i) {
            case 0:
              isPassed = (p.getID() == value);
              break;
            case 1:
              isPassed = (p.getID() != value);
              break;
            case 2:
              isPassed = (p.getID() != value);
              break;
            case 3:
              isPassed = (p.getID() < value);
              break;
            case 4:
              isPassed = (p.getID() <= value);
              break;
            case 5:
              isPassed = (p.getID() > value);
              break;
            case 6:
              isPassed = (p.getID() >= value);
              break;
          }
          if (!isPassed) {
            fail(getName() + " failed for operator " + operators[i]);
          }
        }
      } else {
        fail(getName() + " failed for operator " + operators[i]);
      }
    }
  }

  @Test
  public void testCompareWithString() throws Exception {
    String var = "P1.secId";
    String value = "DELL";
    QueryService qs = CacheUtils.getQueryService();
    for (int i = 0; i < operators.length; i++) {
      Query query = qs.newQuery(
          "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where " + var + operators[i] + "'"
              + value + "'");
      Object result = query.execute();
      if (result instanceof Collection) {
        Iterator iter = ((Collection) result).iterator();
        while (iter.hasNext()) {
          boolean isPassed = false;
          Portfolio p = (Portfolio) iter.next();
          switch (i) {
            case 0:
              isPassed = (p.getP1().getSecId().compareTo(value) == 0);
              break;
            case 1:
              isPassed = (p.getP1().getSecId().compareTo(value) != 0);
              break;
            case 2:
              isPassed = (p.getP1().getSecId().compareTo(value) != 0);
              break;
            case 3:
              isPassed = (p.getP1().getSecId().compareTo(value) < 0);
              break;
            case 4:
              isPassed = (p.getP1().getSecId().compareTo(value) <= 0);
              break;
            case 5:
              isPassed = (p.getP1().getSecId().compareTo(value) > 0);
              break;
            case 6:
              isPassed = (p.getP1().getSecId().compareTo(value) >= 0);
              break;
          }
          if (!isPassed) {
            fail(getName() + " failed for operator " + operators[i]);
          }
        }
      } else {
        fail(getName() + " failed for operator " + operators[i]);
      }
    }
  }

  @Test
  public void testCompareWithNULL() throws Exception {
    String var = "P2";
    Object value = null;
    QueryService qs = CacheUtils.getQueryService();
    for (int i = 0; i < operators.length; i++) {
      Query query =
          qs.newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where " + var
              + operators[i] + value);
      Object result = query.execute();
      if (result instanceof Collection) {
        Iterator iter = ((Collection) result).iterator();
        while (iter.hasNext()) {
          boolean isPassed = false;
          Portfolio p = (Portfolio) iter.next();
          switch (i) {
            case 0:
              isPassed = (p.getP2() == value);
              break;
            default:
              isPassed = (p.getP2() != value);
              break;
          }
          if (!isPassed) {
            fail(getName() + " failed for operator " + operators[i]);
          }
        }
      } else {
        fail(getName() + " failed for operator " + operators[i]);
      }
    }
  }

  @Test
  public void testCompareWithUNDEFINED() throws Exception {
    String var = "P2.secId";
    QueryService qs = CacheUtils.getQueryService();
    for (int i = 0; i < operators.length; i++) {
      // According to docs:
      // To perform equality or inequality comparisons with UNDEFINED, use the
      // IS_DEFINED and IS_UNDEFINED preset query functions instead of these
      // comparison operators.
      if (!operators[i].equals("=") && !operators[i].equals("!=") && !operators[i].equals("<>")) {
        Query query = qs.newQuery(
            "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where " + var + operators[i]
                + " UNDEFINED");
        Object result = query.execute();
        if (result instanceof Collection) {
          if (((Collection) result).size() != 0) {
            fail(getName() + " failed for operator " + operators[i]);
          }
        } else {
          fail(getName() + " failed for operator " + operators[i]);
        }
      }
    }
  }
}
