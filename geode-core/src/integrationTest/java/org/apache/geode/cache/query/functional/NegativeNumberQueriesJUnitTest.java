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
 * NegativeNumberQueriesJUnitTest.java
 *
 * Created on October 5, 2005, 2:44 PM
 */

package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Numbers;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.OQLQueryTest;

// TODO:TEST clean this up and add assertions
@Category({OQLQueryTest.class})
public class NegativeNumberQueriesJUnitTest {

  private Cache cache;
  private Region region;
  private Index index;
  private DistributedSystem ds;
  private QueryService qs;

  private final int cnt = 1;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
    /* create region with to contain Portfolio objects */

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Numbers.class);
    factory.setIndexMaintenanceSynchronous(true);
    region = cache.createRegion("numbers", factory.create());
  }

  @After
  public void tearDown() throws Exception {
    if (ds != null) {
      ds.disconnect();
    }
  }

  @Test
  public void testBug33474() throws Exception {

    populateRegionsWithNumbers();
    // createIndexOnNumbers();

    QueryService qs;
    qs = cache.getQueryService();
    String queryStr = "SELECT DISTINCT * FROM " + SEPARATOR + "numbers num WHERE num.id1 >= -200";
    Query q = qs.newQuery(queryStr);
    SelectResults rs = (SelectResults) q.execute();
    CacheUtils
        .log("--------------------- Size of Result Set is: -------------------------" + rs.size());

  }// end of testGetQueryTimes

  private void populateRegionsWithNumbers() throws Exception {
    CacheUtils.log("--------------------- Populating Data -------------------------");
    for (int i = 0; i < 100; i++) {
      region.put(String.valueOf(i), new Numbers(i));
    }
    for (int i = -100; i > -200; i--) {
      region.put(String.valueOf(i), new Numbers(i));
    }
    CacheUtils.log("--------------------- Data Populatio done -------------------------");
  }// end of populateRegions

  private void createIndexOnNumbers() throws Exception {
    CacheUtils.log("--------------------- Creating Indices -------------------------");
    QueryService qs;
    qs = cache.getQueryService();
    qs.createIndex("id", IndexType.FUNCTIONAL, "num.id", SEPARATOR + "numbers num");
    qs.createIndex("id1", IndexType.FUNCTIONAL, "num.id1", SEPARATOR + "numbers num");
    qs.createIndex("avg", IndexType.FUNCTIONAL, "num.max1", SEPARATOR + "numbers num");
    qs.createIndex("l", IndexType.FUNCTIONAL, "num.l", SEPARATOR + "numbers num");
    CacheUtils.log("--------------------- Index Creation Done -------------------------");
  }// end of createIndex

}// end of NegativeNumberQueriesJUnitTest
