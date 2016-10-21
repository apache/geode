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
package org.apache.geode.cache.query.cq;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class CQJUnitTest {

  private DistributedSystem ds;
  private Cache cache;
  private QueryService qs;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "config");
    this.ds = DistributedSystem.connect(props);
    this.cache = CacheFactory.create(ds);
    this.qs = cache.getQueryService();
  }

  @After
  public void tearDown() throws Exception {
    this.cache.close();
    this.ds.disconnect();
  }

  /**
   * Test to make sure CQs that have invalid syntax throw QueryInvalidException, and CQs that have
   * unsupported CQ features throw UnsupportedOperationException
   */
  @Test
  public void testValidateCQ() throws Exception {

    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();
    // The order by query computes dependency after compilation so the region has to be present
    // for the query to progress further to throw UnsupportedOperationException
    cache.createRegion("region", regionAttributes);

    // default attributes
    CqAttributes attrs = new CqAttributesFactory().create();

    // valid CQ
    this.qs.newCq("SELECT * FROM /region WHERE status = 'active'", attrs);

    // invalid syntax
    try {
      this.qs.newCq("this query is garbage", attrs);
      fail("should have thrown a QueryInvalidException");
    } catch (QueryInvalidException e) {
      // pass
    }

    String[] unsupportedCQs = new String[] {
        // not "just" a select statement
        "(select * from /region where status = 'active').isEmpty",

        // cannot be DISTINCT
        "select DISTINCT * from /region WHERE status = 'active'",

        // references more than one region
        "select * from /region1 r1, /region2 r2 where r1 = r2",

        // where clause refers to a region
        "select * from /region r where r.val = /region.size",

        // more than one iterator in FROM clause
        "select * from /portfolios p1, p1.positions p2 where p2.id = 'IBM'",

        // first iterator in FROM clause is not just a region path
        "select * from /region.entries e where e.value.id = 23",

        // has projections
        "select id from /region where status = 'active'",

        // has ORDER BY
        "select * from /region where status = 'active' ORDER BY id",};

    for (int i = 0; i < unsupportedCQs.length; i++) {
      try {
        this.qs.newCq(unsupportedCQs[i], attrs);
        fail("should have thrown UnsupportedOperationException for query #" + i);
      } catch (UnsupportedOperationException e) {
        // pass
      }
    }
  }

}
