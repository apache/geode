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
import org.apache.geode.cache.query.data.Keywords;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class ReservedKeywordsJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testReservedKeywords() throws Exception {
    String[] keywords = {"select", "distinct", "from", "where", "TRUE", "FALSE", "undefined",
        "element", "not", "and", "or", "type"};
    Region region = CacheUtils.createRegion("Keywords", Keywords.class);
    region.put("0", new Keywords());
    Query query;
    Collection result;
    for (final String s : keywords) {
      String qStr =
          "SELECT DISTINCT * FROM " + SEPARATOR + "Keywords where \"" + s + "\"";
      CacheUtils.log(qStr);
      query = CacheUtils.getQueryService().newQuery(qStr);
      result = (Collection) query.execute();
      if (result.size() != 1) {
        fail(query.getQueryString());
      }
    }
    for (final String keyword : keywords) {
      String qStr =
          "SELECT DISTINCT * FROM " + SEPARATOR + "Keywords where \"" + keyword.toUpperCase()
              + "\"()";
      CacheUtils.log(qStr);
      query = CacheUtils.getQueryService().newQuery(qStr);
      result = (Collection) query.execute();
      if (result.size() != 1) {
        fail(query.getQueryString());
      }
    }
  }
}
