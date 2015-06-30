/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ConstantsJUnitTest.java
 * JUnit based test
 *
 * Created on March 10, 2005, 6:26 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.*;

/**
 *
 * @author vaibhav
 */
@Category(IntegrationTest.class)
public class ConstantsJUnitTest {
  
  public ConstantsJUnitTest() {
  }
  
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
  public void testTRUE() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where TRUE");
    Object result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 4)
      fail(query.getQueryString());
  }
  
  @Test
  public void testFALSE() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where FALSE");
    Object result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 0)
      fail(query.getQueryString());
  }
  
  @Test
  public void testUNDEFINED() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where UNDEFINED");
    Object result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 0)
      fail(query.getQueryString());
    
    query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM UNDEFINED");
    result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 0)
      fail(query.getQueryString());
  }
  
  @Test
  public void testNULL() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where NULL");
    Object result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 0)
      fail(query.getQueryString());
    
    query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM NULL");
    result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 0)
      fail(query.getQueryString());
  }
}
