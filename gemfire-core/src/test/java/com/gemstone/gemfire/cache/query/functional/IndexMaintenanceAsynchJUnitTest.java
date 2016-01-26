/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * IndexMaintenanceAsynchJUnitTest.java
 *
 * Created on May 10, 2005, 5:26 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 * @author Ketan
 */
@Category(IntegrationTest.class)
public class IndexMaintenanceAsynchJUnitTest {

  @Before
  public void setUp() throws Exception {
    if(!isInitDone){
      init();
    }
  }

  @After
  public void tearDown() throws Exception {
  }

  static QueryService qs;
  static boolean isInitDone = false;
  static Region region;
  static IndexProtocol index;
  private static void init(){
    try{
      String queryString;
      Query query;
      Object result;
      Cache cache = CacheUtils.getCache();
      region = CacheUtils.createRegion("portfolios",Portfolio.class, false);
      for (int i = 0; i < 4; i++){
        region.put(""+i,new Portfolio(i));
      }
      qs = cache.getQueryService();
      index = (IndexProtocol)qs.createIndex("statusIndex", IndexType.FUNCTIONAL,"status","/portfolios");
      IndexStatistics stats = index.getStatistics();
      assertEquals(4, stats.getNumUpdates());

      // queryString= "SELECT DISTINCT * FROM /portfolios p, p.positions.values pos where pos.secId='IBM'";
      queryString= "SELECT DISTINCT * FROM /portfolios";
      query = CacheUtils.getQueryService().newQuery(queryString);

      result = query.execute();
      CacheUtils.log(Utils.printResult(result));

    }catch(Exception e){
      e.printStackTrace();
    }
    isInitDone = true;
  }

  @Test
  public void testAddEntry() throws Exception {

    new NewThread(region, index);
    //assertEquals(5, stats.getNumberOfValues());
    Thread.sleep(12000);
  }


  class NewThread implements Runnable {
    String queryString;
    Query query;
    Object result;
    Thread t;
    Region region;
    IndexProtocol index;
    NewThread(Region region, IndexProtocol index) {
      t = new Thread(this,"Demo");
      this.region = region;
      this.index = index;
      t.setPriority(10);
      t.start();
    }
    public void run() {
      try {
        IndexStatistics stats = index.getStatistics();
        for (int i = 5; i < 9; i++){
          region.put(""+i,new Portfolio(i));
        }
        final IndexStatistics st = stats;
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return st.getNumUpdates() == 8;
          }
          public String description() {
            return "index updates never became 8";
          }
        };
        DistributedTestCase.waitForCriterion(ev, 5000, 200, true);

        //queryString= "SELECT DISTINCT * FROM /portfolios p, p.positions.values pos where pos.secId='IBM'";
        queryString= "SELECT DISTINCT * FROM /portfolios where status = 'active'";
        query = CacheUtils.getQueryService().newQuery(queryString);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);

        result = query.execute();
        if(!observer.isIndexesUsed){
          fail("NO INDEX USED");
        }
        CacheUtils.log(Utils.printResult(result));
        if (((Collection)result).size() != 4 ) {
          fail("Did not obtain expected size of result for the query");
        }
        // Task ID: IMA 1

      } catch (Exception e) {
        e.printStackTrace();

      }
    }
  }
  class QueryObserverImpl extends QueryObserverAdapter{
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if(results != null){
        isIndexesUsed = true;
      }
    }
  }
}
