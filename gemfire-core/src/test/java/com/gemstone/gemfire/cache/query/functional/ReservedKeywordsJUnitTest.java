/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ReservedKeywordsJUnitTest.java
 * JUnit based test
 *
 * Created on March 10, 2005, 7:14 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.fail;

//import com.gemstone.gemfire.cache.query.data.Portfolio;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.data.Keywords;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author vaibhav
 */
@Category(IntegrationTest.class)
public class ReservedKeywordsJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testReservedKeywords() throws Exception {
    String keywords[] = { "select", "distinct", "from", "where", "TRUE",
        "FALSE", "undefined", "element", "not", "and", "or", "type"};
    Region region = CacheUtils.createRegion("Keywords", Keywords.class);
    region.put("0", new Keywords());
    Query query;
    Collection result;
    for (int i = 0; i < keywords.length; i++) {
      String qStr = "SELECT DISTINCT * FROM /Keywords where \"" + keywords[i]
          + "\"";
      CacheUtils.log(qStr);
      query = CacheUtils.getQueryService().newQuery(qStr);
      result = (Collection) query.execute();
      if (result.size() != 1) fail(query.getQueryString());
    }
    for (int i = 0; i < keywords.length; i++) {
      String qStr = "SELECT DISTINCT * FROM /Keywords where \""
          + keywords[i].toUpperCase() + "\"()";
      CacheUtils.log(qStr);
      query = CacheUtils.getQueryService().newQuery(qStr);
      result = (Collection) query.execute();
      if (result.size() != 1) fail(query.getQueryString());
    }
  }
}
