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
 * ConstantsJUnitTest.java JUnit based test
 *
 * Created on March 10, 2005, 6:26 PM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.fail;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class ConstantsJUnitTest {

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

  @Test
  public void testTRUE() throws Exception {
    Query query =
        CacheUtils.getQueryService()
            .newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where TRUE");
    Object result = query.execute();
    if (!(result instanceof Collection) || ((Collection) result).size() != 4) {
      fail(query.getQueryString());
    }
  }

  @Test
  public void testFALSE() throws Exception {
    Query query =
        CacheUtils.getQueryService()
            .newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where FALSE");
    Object result = query.execute();
    if (!(result instanceof Collection) || ((Collection) result).size() != 0) {
      fail(query.getQueryString());
    }
  }

  @Test
  public void testUNDEFINED() throws Exception {
    Query query =
        CacheUtils.getQueryService()
            .newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where UNDEFINED");
    Object result = query.execute();
    if (!(result instanceof Collection) || ((Collection) result).size() != 0) {
      fail(query.getQueryString());
    }

    query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM UNDEFINED");
    result = query.execute();
    if (!(result instanceof Collection) || ((Collection) result).size() != 0) {
      fail(query.getQueryString());
    }
  }

  @Test
  public void testNULL() throws Exception {
    Query query =
        CacheUtils.getQueryService()
            .newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where NULL");
    Object result = query.execute();
    if (!(result instanceof Collection) || ((Collection) result).size() != 0) {
      fail(query.getQueryString());
    }

    query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM NULL");
    result = query.execute();
    if (!(result instanceof Collection) || ((Collection) result).size() != 0) {
      fail(query.getQueryString());
    }
  }
}
