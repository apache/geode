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
