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
 * ParameterBindingJUnitTest.java
 * JUnit based test
 *
 * Created on March 10, 2005, 2:42 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 * @author vaibhav
 */
@Category(IntegrationTest.class)
public class ParameterBindingJUnitTest {
  
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
  public void testBindCollectionInFromClause() throws Exception {
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM $1 ");
    Object params[] = new Object[1];
    Region region = CacheUtils.getRegion("/Portfolios");
    params[0] = region.values();
    Object result = query.execute(params);
    if(result instanceof Collection){
      int resultSize = ((Collection)result).size();
      if( resultSize != region.values().size())
        fail("Results not as expected");
    }else
      fail("Invalid result");
  }
  
  @Test
  public void testBindArrayInFromClause() throws Exception {
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM $1 ");
    Object params[] = new Object[1];
    Region region = CacheUtils.getRegion("/Portfolios");
    params[0] = region.values().toArray();
    Object result = query.execute(params);
    if(result instanceof Collection){
      int resultSize = ((Collection)result).size();
      if( resultSize != region.values().size())
        fail("Results not as expected");
    }else
      fail("Invalid result");
  }
  
  @Test
  public void testBindMapInFromClause() throws Exception {
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM $1 ");
    Object params[] = new Object[1];
    Map map = new HashMap();
    Region region = CacheUtils.getRegion("/Portfolios");
    Iterator iter = region.entries(false).iterator();
    while(iter.hasNext()){
      Region.Entry entry = (Region.Entry)iter.next();
      map.put(entry.getKey(), entry.getValue());
    }
    params[0] = map;
    Object result = query.execute(params);
    if(result instanceof Collection){
      int resultSize = ((Collection)result).size();
      if( resultSize != region.values().size())
        fail("Results not as expected");
    }else
      fail("Invalid result");
  }
  
  @Test
  public void testBindRegionInFromClause() throws Exception {
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM $1 ");
    Object params[] = new Object[1];
    Region region = CacheUtils.getRegion("/Portfolios");
    params[0] = region;
    Object result = query.execute(params);
    if(result instanceof Collection){
      int resultSize = ((Collection)result).size();
      if( resultSize != region.values().size())
        fail("Results not as expected");
    }else
      fail("Invalid result");
  }
  
  
  @Test
  public void testBindValueAsMethodParamter() throws Exception {
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where status.equals($1)");
    Object params[] = new Object[1];
    params[0] = "active";
    Object result = query.execute(params);
    if(result instanceof Collection){
      int resultSize = ((Collection)result).size();
      if( resultSize != 2)
        fail("Results not as expected");
    }else
      fail("Invalid result");
  }
  
  @Test
  public void testBindString() throws Exception {
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where status = $1");
    Object params[] = new Object[1];
    params[0] = "active";
    Object result = query.execute(params);
    if(result instanceof Collection){
      int resultSize = ((Collection)result).size();
      if( resultSize != 2)
        fail("Results not as expected");
    }else
      fail("Invalid result");
  }
  
  @Test
  public void testBindInt() throws Exception {
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where ID = $1");
    Object params[] = new Object[1];
    params[0] = new Integer(1);
    Object result = query.execute(params);
    if(result instanceof Collection){
      int resultSize = ((Collection)result).size();
      if( resultSize != 1)
        fail("Results not as expected");
    }else
      fail("Invalid result");
  }
  
}
