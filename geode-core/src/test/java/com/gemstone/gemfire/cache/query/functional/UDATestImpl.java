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
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;

/**
 * Tests the group by queries with or without aggreagte functions
 * 
 * @author Asif
 *
 *
 */
public abstract class UDATestImpl implements UDATestInterface {

 
  public abstract Region createRegion(String regionName, Class constraint);

  @Test
  public void testUDANoGroupBy() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    int sum = 0;
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
      sum += pf.ID;
    }
    String queryStr = "select myUDA(p.ID) from /portfolio p where p.ID > 0  ";
    QueryService qs = CacheUtils.getQueryService();
    qs.createUDA("myUDA", "com.gemstone.gemfire.cache.query.functional.UDATestImpl$SumUDA");
    Query query = qs.newQuery(queryStr);
    SelectResults sr = (SelectResults) query.execute();
    assertEquals(sum, ((Integer)sr.asList().get(0)).intValue());
  }

  @Test
  public void testUDAWithGroupBy() throws Exception {
    Region region = this.createRegion("portfolio", Portfolio.class);
    int sumActive = 0;
    int sumInactive = 0;
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
      if(pf.status.equals("active")) {
        sumActive += pf.ID;
      }else {
        sumInactive += pf.ID;
      }
      
    }
    String queryStr = "select p.status , myUDA(p.ID) from /portfolio p where p.ID > 0 group by p.status order by p.status";
    QueryService qs = CacheUtils.getQueryService();
    qs.createUDA("myUDA", "com.gemstone.gemfire.cache.query.functional.UDATestImpl$SumUDA");
    Query query = qs.newQuery(queryStr);
    SelectResults sr = (SelectResults) query.execute();
    List<Struct> structs = (List<Struct>)sr.asList();
    assertEquals(2, structs.size());
    assertTrue(structs.get(0).getFieldValues()[0].equals("active"));
    assertEquals(sumActive, ((Integer)structs.get(0).getFieldValues()[1]).intValue());
    
    assertTrue(structs.get(1).getFieldValues()[0].equals("inactive"));
    assertEquals(sumInactive, ((Integer)structs.get(1).getFieldValues()[1]).intValue());
  }

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    CacheUtils.getCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }
  
  public static class SumUDA implements Aggregator, Serializable {

    private int sum =0;
    
    public SumUDA() {
      
    }
    @Override
    public void accumulate(Object value) {
      sum += ((Integer)value).intValue();
      
    }

    @Override
    public void init() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public Object terminate() {      
      return Integer.valueOf(sum);
    }

    @Override
    public void merge(Aggregator otherAggregator) {
      SumUDA uda = (SumUDA)otherAggregator;
      this.sum += uda.sum;      
    }
    
  }
}
