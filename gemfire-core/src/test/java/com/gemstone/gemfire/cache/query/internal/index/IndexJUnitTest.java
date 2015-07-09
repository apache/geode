/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IndexJUnitTest.java
 * JUnit based test
 *
 * Created on March 9, 2005, 3:30 PM
 */
package com.gemstone.gemfire.cache.query.internal.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author vaibhav
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class IndexJUnitTest {
  
  private static final String indexName = "testIndex";
  private static Index index;
  private static Region region;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    CacheUtils.startCache();
    QueryService qs = CacheUtils.getQueryService();
    region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    index = qs.createIndex(indexName, IndexType.FUNCTIONAL,"p.status","/Portfolios p");
  }
  
  @AfterClass
  public static void afterClass() {
    CacheUtils.closeCache();
    region = null;
    index = null;
  }
  
  @Test
  public void testGetName() {
    assertEquals("Index.getName does not return correct index name", indexName, index.getName());
  }
  
  @Test
  public void testGetType() {
    assertSame("Index.getName does not return correct index type", IndexType.FUNCTIONAL, index.getType());
  }
  
  @Test
  public void testGetRegion() {
    assertSame("Index.getName does not return correct region", region, index.getRegion());
  }
  
  @Test
  public void testGetFromClause() {
    CacheUtils.log("testGetCanonicalizedFromClause");
    assertEquals("Index.getName does not return correct from clause", "/Portfolios index_iter1", index.getCanonicalizedFromClause());
  }
  
  @Test
  public void testGetCanonicalizedIndexedExpression() {
    assertEquals("Index.getName does not return correct index expression", "index_iter1.status", index.getCanonicalizedIndexedExpression());
  }
  
  @Test
  public void testGetCanonicalizedProjectionAttributes() {
    assertEquals("Index.getName does not return correct projection attributes", "*", index.getCanonicalizedProjectionAttributes());
  }
}
