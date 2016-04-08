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
 * MultipleRegionsJUnitTest.java
 *
 * Created on May 18, 2005, 5:02 PM
 */

package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.fail;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Data;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class MultipleRegionsJUnitTest {


  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region1 = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for(int i=0;i<5;i++){
      region1.put(""+i, new Portfolio(i));
    }
    Region region2 = CacheUtils.createRegion("Portfolios2", Portfolio.class);
    for(int i=0;i<2;i++){
      region2.put(""+i, new Portfolio(i));
    }
    Region region3 = CacheUtils.createRegion("Data", Data.class);
    for(int i=0;i<2;i++){
      region3.put(""+i, new Data());
    }
    Region region4 = CacheUtils.createRegion("Portfolios3", Portfolio.class);
    for(int i=0;i<4;i++){
      region4.put(""+i, new Portfolio(i));
    }
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testQueriesExecutionOnMultipleRegion() throws Exception {
    int SizeArray[]={5,2,0,8,80,10,8,10,48};
    QueryService qs = CacheUtils.getQueryService();
    String queries[]={
        // Multiple Regions Available. Execute queries on any of the Region.
        "select distinct * from /Portfolios",
        "SELECT DISTINCT * FROM /Portfolios2,  positions.values where status='active'",
        "SELECT DISTINCT * from /Portfolios pf , pf.positions.values pos where pos.getSecId = 'IBM' and status = 'inactive'",
        "Select distinct * from /Portfolios3 pf, pf.positions",
        // Multiple Regions in a Query         
        "Select distinct * from /Portfolios, /Portfolios2, /Portfolios3, /Data",
        "Select distinct * from /Portfolios, /Portfolios2",
        "Select distinct * from /Portfolios3, /Data",
        "Select distinct * from /Portfolios, /Data",  
        "Select distinct * from /Portfolios pf, /Portfolios2, /Portfolios3, /Data where pf.status='active'"
    };
    try{
      for(int i=0;i<queries.length;i++){
        Query query = qs.newQuery(queries[i]);
        Object result = query.execute();
        //  CacheUtils.log(Utils.printResult(result));
        if (((Collection)result).size()!= SizeArray[i])
          fail("Size of Result is not as Expected");
      }
    }catch(Exception e){
      e.printStackTrace();
      fail();
    }
  }


}

