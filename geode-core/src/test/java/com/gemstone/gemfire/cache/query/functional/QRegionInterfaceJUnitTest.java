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
 * QRegionInterfaceJUnitTest.java
 * JUnit based test
 *
 * Created on March 10, 2005, 6:07 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class QRegionInterfaceJUnitTest {
  
  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    region.put("0",new Portfolio(0));
    region.put("1",new Portfolio(1));
    region.put("2",new Portfolio(2));
    region.put("3",new Portfolio(3));
  }
  
  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  
  @Test
  public void testGetKeys() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("select distinct * from /Portfolios.keys where toString = '1'");
    Collection result = (Collection)query.execute();
    if(!result.iterator().next().equals("1"))
      fail(query.getQueryString());
  }
  
  @Test
  public void testGetValues() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("select distinct * from /Portfolios.values where ID = 1");
    Collection result = (Collection)query.execute();
    Portfolio p = (Portfolio)result.iterator().next();
    if(p.getID() != 1)
      fail(query.getQueryString());
  }
  
  @Test
  public void testGetEntries() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("select distinct * from /Portfolios.entries where value.ID = 1 and key = '1'");
    Collection result = (Collection)query.execute();
    Region.Entry entry = (Region.Entry)result.iterator().next();
    if(!entry.getKey().equals("1") || ((Portfolio)entry.getValue()).getID() != 1)
      fail(query.getQueryString());
  }
  
  @Test
  public void testMiscQueries() throws Exception{
    String testData[][] ={
      {"/Portfolios.fullPath","/Portfolios"},
      {"/Portfolios.size","4"},
      {"/Portfolios.size > 0","true"},
    };
    for(int i=0;i<testData.length;i++){
      Query query = CacheUtils.getQueryService().newQuery(testData[i][0]);
      String result = query.execute().toString();
      if(!result.equals(testData[i][1]))
        fail(query.getQueryString());
    }
  }
  
  @Test
  public void testBug35905KeySet() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("select distinct * from /Portfolios.keySet where toString = '1'");
    Collection result = (Collection)query.execute();
    if(!result.iterator().next().equals("1"))
      fail(query.getQueryString());
  }
  
  @Test
  public void testBug35905EntrySet() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("select distinct key from /Portfolios.entrySet , value.positions.values   where value.ID = 1 and key = '1'");
    Collection result = (Collection)query.execute();
    if(!result.iterator().next().equals("1"))
      fail(query.getQueryString());
  }
  
  @Test
  public void testBug35905ContainsValue() throws Exception{
    String testData[][] ={       
        {"/Portfolios.containsValue($1)","true"},
      };
      for(int i=0;i<testData.length;i++){
        Query query = CacheUtils.getQueryService().newQuery(testData[i][0]);
        String result = query.execute(new Object[]{CacheUtils.getRegion("/Portfolios").get("1")}).toString();
        if(!result.equals(testData[i][1]))
          fail(query.getQueryString());
      }
  }  
  
  @Test
  public void testUnsupportedExceptionInSubregionsMethod() {
    Region region1 = null;
    Region region2 = null;
    try{
      region1 = CacheUtils.getRegion("/Portfolios").createSubregion("region1", CacheUtils.getRegion("/Portfolios").getAttributes());
      region2 =  CacheUtils.getRegion("/Portfolios").createSubregion("region2", CacheUtils.getRegion("/Portfolios").getAttributes());
    }catch(RegionExistsException ree) {
      fail("Test failed because of Exception= "+ree);
    }
    region1.put("5",new Portfolio(0));
    region1.put("5",new Portfolio(1));
    region2.put("6",new Portfolio(2));
    region2.put("6",new Portfolio(3));
    Query query = CacheUtils.getQueryService().newQuery("select distinct * from /Portfolios.subregions(false) where remove('5')!= null");
    try{
      query.execute();
      assertTrue(!region1.containsKey("5"));
      assertTrue(!region2.containsKey("5"));
      fail("The test should have thrown TypeMismatchException exception");
    }catch (TypeMismatchException tme ) {
      //Test has passed successfuly as this exception is expected
    }catch(Exception e) {
      fail("Test failed because of Exception= "+e);
    }
  }
  
  
  
}
