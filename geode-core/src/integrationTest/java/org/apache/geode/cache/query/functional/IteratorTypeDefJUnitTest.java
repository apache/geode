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
 * IteratorTypeDefJUnitTest.java
 *
 * Created on April 7, 2005, 12:40 PM
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
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class IteratorTypeDefJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
    CacheUtils.log(region);
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testIteratorDefSyntax() throws Exception {
    String queries[] = {
        "IMPORT org.apache.geode.cache.\"query\".data.Position;"
            + "SELECT DISTINCT secId FROM /portfolios,  positions.values pos TYPE Position WHERE iD > 0",
        "IMPORT org.apache.geode.cache.\"query\".data.Position;"
            + "SELECT DISTINCT secId FROM /portfolios, positions.values AS pos TYPE Position WHERE iD > 0",
        "IMPORT org.apache.geode.cache.\"query\".data.Position;"
            + "SELECT DISTINCT pos.secId FROM /portfolios, pos IN positions.values TYPE Position WHERE iD > 0",
        "SELECT DISTINCT pos.secId FROM /portfolios,  positions.values AS pos  WHERE iD > 0",
        "SELECT DISTINCT pos.secId FROM /portfolios, pos IN positions.values  WHERE iD > 0",};
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.log("TestCase:testIteratorDefSyntax PASS");
  }

  @Test
  public void testIteratorDefSyntaxForObtainingResultBag() throws Exception {
    String queries[] = {"IMPORT org.apache.geode.cache.\"query\".data.Position;"
        + "SELECT DISTINCT secId FROM /portfolios, (set<Position>)positions.values WHERE iD > 0",};
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
        if (!(r instanceof SelectResults)) {
          fail(
              "testIteratorDefSyntaxForObtainingResultBag: Test failed as obtained Result Data not an instance of SelectResults. Query= "
                  + q.getQueryString());
        }
        if (((SelectResults) r).getCollectionType().allowsDuplicates()) {
          fail(
              "testIteratorDefSyntaxForObtainingResultBag: results of query should not allow duplicates, but says it does");
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.log("TestCase:testIteratorDefSyntaxForObtainingResultSet PASS");
  }


  @Test
  public void testNOValueconstraintInCreatRegion() throws Exception {
    CacheUtils.createRegion("pos", null);
    String queries[] = {"IMPORT org.apache.geode.cache.\"query\".data.Portfolio;"
        + "SELECT DISTINCT * FROM (set<Portfolio>)/pos where iD > 0"};
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.log("TestCase: testNOValueconstraintInCreatRegion PASS");
  }

  @Test
  public void testNOConstraintOnRegion() throws Exception {
    Region region = CacheUtils.createRegion("portfl", null);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
    CacheUtils.log(region);
    String queries[] = {"IMPORT org.apache.geode.cache.\"query\".data.Position;"
        + "IMPORT org.apache.geode.cache.\"query\".data.Portfolio;"
        + "SELECT DISTINCT secId FROM (set<Portfolio>)/portfl, (set<Position>)positions.values WHERE iD > 0",};
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        Object r = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    CacheUtils.log("TestCase: testNOConstraintOnRegion PASS");
  }

}
