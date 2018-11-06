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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class CompiledGroupBySelectJUnitTest {
  private Region rgn;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    this.rgn = this.createRegion("portfolio", Portfolio.class);
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  public Region createRegion(String regionName, Class valueConstraint) {
    AttributesFactory af = new AttributesFactory();
    af.setValueConstraint(valueConstraint);
    Region r1 = CacheUtils.createRegion(regionName, af.create(), false);
    return r1;

  }

  @Test
  public void testCompiledSelectCreation() throws Exception {
    String queryStr = "select count(*) from /portfolio pf where pf.ID > 0";
    QueryService qs = CacheUtils.getQueryService();
    DefaultQuery query = (DefaultQuery) qs.newQuery(queryStr);
    CompiledSelect cs = query.getSimpleSelect();
    assertFalse(cs instanceof CompiledGroupBySelect);

    queryStr = "select count(pf.ShortID) from /portfolio pf where pf.ID > 0";
    query = (DefaultQuery) qs.newQuery(queryStr);
    cs = query.getSimpleSelect();
    assertTrue(cs instanceof CompiledGroupBySelect);

    queryStr = "select count(distinct pf.ShortID) from /portfolio pf where pf.ID > 0";
    query = (DefaultQuery) qs.newQuery(queryStr);
    cs = query.getSimpleSelect();
    assertTrue(cs instanceof CompiledGroupBySelect);

    queryStr = "select count(*) , max(pf.ID) from /portfolio pf where pf.ID > 0";
    query = (DefaultQuery) qs.newQuery(queryStr);
    cs = query.getSimpleSelect();
    assertTrue(cs instanceof CompiledGroupBySelect);

    queryStr = "select  max(pf.ID) from /portfolio pf where pf.ID > 0";
    query = (DefaultQuery) qs.newQuery(queryStr);
    cs = query.getSimpleSelect();
    assertTrue(cs instanceof CompiledGroupBySelect);

    queryStr =
        "select count(*) , pf.shortID from /portfolio pf where pf.ID > 0 group by pf.shortID";
    query = (DefaultQuery) qs.newQuery(queryStr);
    cs = query.getSimpleSelect();
    assertTrue(cs instanceof CompiledGroupBySelect);

    queryStr = "select * from /portfolio pf where pf.ID > 0 group by pf";
    query = (DefaultQuery) qs.newQuery(queryStr);
    cs = query.getSimpleSelect();
    assertTrue(cs instanceof CompiledSelect);

    queryStr = "select * from /portfolio pf, pf.positions pos where pf.ID > 0 group by pf, pos";
    query = (DefaultQuery) qs.newQuery(queryStr);
    cs = query.getSimpleSelect();
    assertTrue(cs instanceof CompiledSelect);

    queryStr = "select pf.status as status , pf.shortID as shid from /portfolio pf where pf.ID > 0 "
        + "group by status , shid order by shid";
    query = (DefaultQuery) qs.newQuery(queryStr);
    cs = query.getSimpleSelect();
    assertTrue(cs instanceof CompiledGroupBySelect);
  }

  @Test
  public void testInvalidQuery() throws Exception {
    String queryStr = "select count(*) , pf.shortID from /portfolio pf where pf.ID > 0 ";
    QueryService qs = CacheUtils.getQueryService();
    try {
      DefaultQuery query = (DefaultQuery) qs.newQuery(queryStr);
      fail("query creation should have failed");
    } catch (QueryInvalidException qie) {
      assertTrue(qie.toString().indexOf(
          "Query contains projected column not present in group by clause") != -1);
    }

    queryStr = "select * from /portfolio pf where pf.ID > 0 group by pf.ID";
    try {
      DefaultQuery query = (DefaultQuery) qs.newQuery(queryStr);
      fail("query creation should have failed");
    } catch (QueryInvalidException qie) {
      assertTrue(qie.toString().indexOf(
          "Query contains projected column not present in group by clause") != -1);
    }

    queryStr = "select * from /portfolio pf, pf.positions pos where pf.ID > 0 group by pf";
    try {
      DefaultQuery query = (DefaultQuery) qs.newQuery(queryStr);
      fail("query creation should have failed");
    } catch (QueryInvalidException qie) {
      assertTrue(qie.toString()
          .indexOf("Query contains projected column not present in group by clause") != -1);
    }
  }

  @Test
  public void testUnsupportedQuery() throws Exception {
    String queryStr = "select count(*)  from /portfolio pf where pf.ID > 0  group by pf.shortID";
    QueryService qs = CacheUtils.getQueryService();
    try {
      DefaultQuery query = (DefaultQuery) qs.newQuery(queryStr);
      fail("query creation should have failed");
    } catch (QueryInvalidException qie) {
      assertTrue(qie.toString().indexOf(
          "Query contains group by columns not present in projected fields") != -1);
    }
  }

}
