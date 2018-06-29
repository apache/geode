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
package org.apache.geode.cache.query;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({IntegrationTest.class, OQLQueryTest.class})
@RunWith(JUnitParamsRunner.class)
public class JoinQueriesIntegrationTest {

  public static class Customer implements Serializable {
    public int pkid;
    public int id;
    public int joinId;
    public String name;

    public Customer(int pkid, int id) {
      this.pkid = pkid;
      this.id = id;
      this.joinId = id;
      this.name = "name" + pkid;
    }

    public Customer(int pkid, int id, int joinId) {
      this.pkid = pkid;
      this.id = id;
      this.joinId = joinId;
      this.name = "name" + pkid;
    }

    public String toString() {
      return "Customer pkid = " + pkid + ", id: " + id + " name:" + name + " joinId: " + joinId;
    }
  }


  private static Object[] getQueryStrings() {
    return new Object[] {
        new Object[] {
            "<trace>select STA.id as STACID, STA.pkid as STAacctNum, STC.id as STCCID, STC.pkid as STCacctNum from /region1 STA, /region2 STC where STA.pkid = 1 AND STA.joinId = STC.joinId AND STA.id = STC.id",
            20},
        new Object[] {
            "<trace>select STA.id as STACID, STA.pkid as STAacctNum, STC.id as STCCID, STC.pkid as STCacctNum from /region1 STA, /region2 STC where STA.pkid = 1 AND STA.joinId = STC.joinId OR STA.id = STC.id",
            22}};
  }

  @Test
  @Parameters(method = "getQueryStrings")
  public void testJoinTwoRegions(String queryString, int expectedResultSize) throws Exception {
    Cache cache = CacheUtils.getCache();
    try {
      Region region1 =
          cache.createRegionFactory().setDataPolicy(DataPolicy.REPLICATE).create("region1");
      Region region2 =
          cache.createRegionFactory().setDataPolicy(DataPolicy.REPLICATE).create("region2");

      populateRegionWithData(region1, region2);

      QueryService queryService = cache.getQueryService();

      SelectResults results = (SelectResults) queryService.newQuery(queryString).execute();
      int resultsWithoutIndex = results.size();
      assertEquals(expectedResultSize, resultsWithoutIndex);

      queryService.createIndex("pkidregion1", "p.pkid", "/region1 p");
      queryService.createIndex("pkidregion2", "p.pkid", "/region2 p");
      queryService.createIndex("indexIDRegion2", "p.id", "/region2 p");
      queryService.createIndex("indexIDRegion1", "p.id", "/region1 p");
      queryService.createIndex("joinIdregion1", "p.joinId", "/region1 p");
      queryService.createIndex("joinIdregion2", "p.joinId", "/region2 p");
      queryService.createIndex("nameIndex", "p.name", "/region2 p");

      results = (SelectResults) queryService.newQuery(queryString).execute();
      int resultsSizeWithIndex = results.size();
      assertEquals(expectedResultSize, resultsWithoutIndex);
      assertEquals(resultsSizeWithIndex, resultsWithoutIndex);
    } finally {
      cache.getRegion("region1").destroyRegion();
      cache.getRegion("region2").destroyRegion();
    }

  }

  private void populateRegionWithData(Region region1, Region region2) {
    for (int i = 1; i < 11; i++) {
      if (i == 1 || i == 3 || i == 8 || i == 2 || i == 5) {
        region1.put(i, new Customer(1, 1, 1));
      } else {
        region1.put(i, new Customer(i, i, i));
      }
      if (i == 1 || i == 4 || i == 7 || i == 10) {
        region2.put(i, new Customer(1, 1, 1));
      } else {
        region2.put(i, new Customer(i % 5, i, i % 3));
      }
    }
  }
}
