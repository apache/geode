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
package org.apache.geode.cache.query.partitioned;

import static org.apache.geode.cache.query.Utils.createNewPortfoliosAndPositions;
import static org.apache.geode.cache.query.Utils.createPortfoliosAndPositions;

import java.util.ArrayList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import parReg.query.unittest.NewPortfolio;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.functional.StructSetOrResultsSet;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({OQLQueryTest.class})
public class PRColocatedEquiJoinTest {
  private static final int count = 100;

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  @Test
  public void prQueryWithHeteroIndex() throws Exception {
    InternalCache cache = server.getCache();
    QueryService qs = cache.getQueryService();

    // create a local and Partition region for 1st select query
    Region<Integer, Portfolio> r1 = server.createRegion(RegionShortcut.LOCAL, "region1",
        rf -> rf.setValueConstraint(Portfolio.class));
    qs.createIndex("IdIndex1", "r.ID", "/region1 r, r.positions.values pos");
    Region<Integer, NewPortfolio> r2 = server.createRegion(RegionShortcut.PARTITION, "region2",
        rf -> rf.setValueConstraint(NewPortfolio.class));
    qs.createIndex("IdIndex2", "r.id", "/region2 r");

    // create two local regions for 2nd select query to compare the result set
    Region<Integer, Portfolio> r3 = server.createRegion(RegionShortcut.LOCAL, "region3",
        rf -> rf.setValueConstraint(Portfolio.class));
    Region<Integer, NewPortfolio> r4 = server.createRegion(RegionShortcut.LOCAL, "region4",
        rf -> rf.setValueConstraint(NewPortfolio.class));

    Portfolio[] portfolio = createPortfoliosAndPositions(count);
    NewPortfolio[] newPortfolio = createNewPortfoliosAndPositions(count);

    for (int i = 0; i < count; i++) {
      r1.put(i, portfolio[i]);
      r2.put(i, newPortfolio[i]);
      r3.put(i, portfolio[i]);
      r4.put(i, newPortfolio[i]);
    }

    ArrayList<?>[][] results = new ArrayList<?>[whereClauses.length][2];
    for (int i = 0; i < whereClauses.length; i++) {
      // issue the first select on region 1 and region 2
      @SuppressWarnings("unchecked")
      SelectResults<ArrayList<ArrayList<?>>> selectResults =
          (SelectResults<ArrayList<ArrayList<?>>>) qs.newQuery("<trace> Select "
              + (whereClauses[i].contains("ORDER BY") ? "DISTINCT" : "")
              + "* from /region1 r1, /region2 r2 where " + whereClauses[i])
              .execute();
      results[i][0] = (ArrayList<?>) selectResults.asList();

      // issue the second select on region 3 and region 4
      @SuppressWarnings("unchecked")
      SelectResults<ArrayList<ArrayList<?>>> queryResult =
          (SelectResults<ArrayList<ArrayList<?>>>) qs.newQuery("<trace> Select "
              + (whereClauses[i].contains("ORDER BY") ? "DISTINCT" : "")
              + "* from /region3 r1, /region4 r2 where " + whereClauses[i])
              .execute();
      results[i][1] = (ArrayList<?>) queryResult.asList();
    }

    // compare the resultsets and expect them to be equal
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsAsListWithoutAndWithIndexes(results, whereClauses.length, false,
        false,
        whereClauses);
  }

  private static String[] whereClauses = new String[] {"r2.ID = r1.id",
      "r1.ID = r2.id AND r1.ID > 5",
      "r1.ID = r2.id AND r1.status = 'active'",
      "r1.ID = r2.id ORDER BY r1.ID", "r1.ID = r2.id ORDER BY r2.id",
      "r1.ID = r2.id ORDER BY r2.status", "r1.ID = r2.id AND r1.status != r2.status",
      "r1.ID = r2.id AND r1.status = r2.status",
      "r1.ID = r2.id AND r1.positions.size = r2.positions.size",
      "r1.ID = r2.id AND r1.positions.size > r2.positions.size",
      "r1.ID = r2.id AND r1.positions.size < r2.positions.size",
      "r1.ID = r2.id AND r1.positions.size = r2.positions.size AND r2.positions.size > 0",
      "r1.ID = r2.id AND (r1.positions.size > r2.positions.size OR r2.positions.size > 0)",
      "r1.ID = r2.id AND (r1.positions.size < r2.positions.size OR r1.positions.size > 0)",
  };
}
