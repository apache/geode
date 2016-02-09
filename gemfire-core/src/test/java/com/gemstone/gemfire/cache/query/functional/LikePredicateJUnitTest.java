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
 * InOperatorTest.java
 * 
 * Created on March 24, 2005, 5:08 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.ResultsBag;
import com.gemstone.gemfire.cache.query.internal.ResultsCollectionWrapper;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager.TestHook;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author asif
 */
@Category(IntegrationTest.class)
public class LikePredicateJUnitTest {

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testLikePercentageTerminated_1_withoutBindParam() throws Exception {
    likePercentageTerminated_1(false);
  }
  @Test
  public void testLikePercentageTerminated_1_withBindParam() throws Exception {
    likePercentageTerminated_1(true);
  }
  /**
   * Tests simple % terminated pattern with atleast one preceding character
   * @throws Exception
   */
  private void likePercentageTerminated_1(boolean useBindParam) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);

    char ch = 'd';
    String base = "abc";
    for (int i = 1; i < 6; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + ch;
      ch += 1;
      region.put(new Integer(i), pf);
    }

    base = "abd";
    ch = 'd';
    for (int i = 6; i < 11; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + ch;
      ch += 1;
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();

    Query q;
    SelectResults results;
    SelectResults expectedResults;
    String predicate = "";
    if(useBindParam) {
      predicate = "$1";
    }else {
      predicate = " 'abc%'";
    }
    
    q = qs
        .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+predicate);
    if(useBindParam) {
      results = (SelectResults)q.execute(new Object[]{"abc%"});
    }else {
      results = (SelectResults)q.execute();
    }
    ResultsBag bag = new ResultsBag(null);
    for (int i = 1; i < 6; ++i) {
      bag.add(region.get(new Integer(i)));
    }
    expectedResults = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
                                                   bag.asSet());
    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);

    // Create Index
    qs.createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    QueryObserver old = QueryObserverHolder
        .setInstance(new QueryObserverAdapter() {
          private boolean indexCalled = false;

          public void afterIndexLookup(Collection results) {
            indexCalled = true;
          }

          public void endQuery() {
            assertTrue(indexCalled);
          }

        });
    if(useBindParam) {
      results = (SelectResults)q.execute(new Object[]{"abc%"});
    }else {
      results = (SelectResults)q.execute();
    }

    rs[0][0] = results;
    rs[0][1] = expectedResults;
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    QueryObserverHolder.setInstance(old);

  }
  
  @Test
  public void testLikePercentageTerminated_2_withoutParams() throws Exception {
    likePercentageTerminated_2(false);
  }
  
  @Test
  public void testLikePercentageTerminated_2_withParams() throws Exception {
    likePercentageTerminated_2(true);
  }

  /**
   * Tests a pattern which just contains a single % indicating all match 
   * @throws Exception
   */
  private void likePercentageTerminated_2(boolean useBindParam) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);

    char ch = 'd';
    String base = "abc";
    for (int i = 1; i < 6; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + ch;
      ch += 1;
      region.put(new Integer(i), pf);
    }

    base = "abd";
    ch = 'd';
    for (int i = 6; i < 11; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + ch;
      ch += 1;
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();

    Query q;
    SelectResults results;
    SelectResults expectedResults;
    String predicate = "";
    if(useBindParam) {
      predicate = "$1";
    }else {
      predicate = " '%'";
    }
    
    q = qs.newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+ predicate);
    if(useBindParam) {
      results = (SelectResults)q.execute(new Object[]{"%"});
    }else {
      results = (SelectResults)q.execute();
    }
    ResultsBag bag = new ResultsBag(null);
    for (int i = 1; i < 11; ++i) {
      bag.add(region.get(new Integer(i)));
    }
    expectedResults = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
                                                   bag.asSet());
    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);

    // Create Index
    qs.createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    QueryObserver old = QueryObserverHolder
        .setInstance(new QueryObserverAdapter() {
          private boolean indexCalled = false;

          public void afterIndexLookup(Collection results) {
            indexCalled = true;
          }

          public void endQuery() {
            assertTrue(indexCalled);
          }

        });
    if(useBindParam) {
      results = (SelectResults)q.execute(new Object[]{"%"});
    }else {
      results = (SelectResults)q.execute();
    }

    rs[0][0] = results;
    rs[0][1] = expectedResults;
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    QueryObserverHolder.setInstance(old);

  }
  
  @Test
  public void testLikePercentageTerminated_3_withoutBindPrms() throws Exception {
    likePercentageTerminated_3(false);
  }
  
  @Test
  public void testLikePercentageTerminated_3_withBindPrms() throws Exception {
    likePercentageTerminated_3(true);
  }
  
  /**
   * Tests a simple % terminated like predicate with an OR condition
   * @throws Exception
   */
  private void likePercentageTerminated_3(boolean useBindPrm) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);

    char ch = 'd';
    String base = "abc";
    for (int i = 1; i < 6; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + ch;
      ch += 1;
      region.put(new Integer(i), pf);
    }

    base = "abd";
    ch = 'd';
    for (int i = 6; i < 11; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + ch;
      ch += 1;
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();

    Query q;
    SelectResults results;
    SelectResults expectedResults;
    String predicate = "";
    if(useBindPrm) {
      predicate = "$1";
    }else {
      predicate = " 'abc%'";
    }
    q = qs
        .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+predicate+" OR ps.ID > 6");
    if(useBindPrm) {
      results = (SelectResults)q.execute(new Object[]{"abc%"});
    }else {
      results = (SelectResults)q.execute();
    }
    ResultsBag bag = new ResultsBag(null);
    for (int i = 1; i < 11; ++i) {
      if (i != 6) {
        bag.add(region.get(new Integer(i)));
      }
    }
    expectedResults = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
                                                   bag.asSet());
    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);

    // Create Index
    qs.createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    
    QueryObserver old = QueryObserverHolder
        .setInstance(new QueryObserverAdapter() {
          private boolean indexCalled = false;

          public void afterIndexLookup(Collection results) {
            indexCalled = true;
          }

          public void endQuery() {
            assertFalse(indexCalled);
          }

        });
    if(useBindPrm) {
      results = (SelectResults)q.execute(new Object[]{"abc%"});
    }else {
      results = (SelectResults)q.execute();
    }

    rs[0][0] = results;
    rs[0][1] = expectedResults;
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    qs.createIndex("id", IndexType.FUNCTIONAL, "ps.ID", "/pos ps");
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private boolean[] indexCalled = new boolean[] { false, false };

      private int i = 0;

      public void afterIndexLookup(Collection results) {
        indexCalled[i++] = true;
      }

      public void endQuery() {
        for (int i = 0; i < indexCalled.length; ++i) {
          assertTrue(indexCalled[i]);
        }
      }

    });

    if(useBindPrm) {
      results = (SelectResults)q.execute(new Object[]{"abc%"});
    }else {
      results = (SelectResults)q.execute();
    }

    rs[0][0] = results;
    rs[0][1] = expectedResults;
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    QueryObserverHolder.setInstance(old);

  }
  @Test
  public void testLikePercentageTerminated_4_withoutBindPrms() throws Exception {
    likePercentageTerminated_4(false);
  }
  
  @Test
  public void testLikePercentageTerminated_4_withBindPrms() throws Exception {
    likePercentageTerminated_4(true);
  }
  /**
   * Tests a simple % terminated like predicate with an AND condition
   * @throws Exception
   */
  public void likePercentageTerminated_4(boolean useBindPrm) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);

    
    String base = "abc";
    String pattern = base;
    //so we will get string like abcdcdcdcdcdc
    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pattern +="dc";
      pf.status = pattern;      
      region.put(new Integer(i), pf);
    }

    base = "abd";
    pattern = base;
    //so we will get string like abddcdcdcd
    for (int i = 201; i < 400; ++i) {
      Portfolio pf = new Portfolio(i);
      pattern +="dc";
      pf.status = pattern;
      
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();

    Query q;
    SelectResults results;
    SelectResults expectedResults;
    String predicate = "";
    if(useBindPrm) {
      predicate = "$1";
    }else {
      predicate = " 'abc%'";
    }
    q = qs
        .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+predicate+" AND ps.ID > 2 AND ps.ID < 150");
    if(useBindPrm) {
      results = (SelectResults)q.execute(new Object[]{"abc%"});
    }else {
      results = (SelectResults)q.execute();
    }
    ResultsBag bag = new ResultsBag(null);
    for (int i = 3; i < 150; ++i) {
      bag.add(region.get(new Integer(i)));
    }
    expectedResults = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
                                                   bag.asSet());
    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);

    // Create Index
    qs.createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    QueryObserver old = QueryObserverHolder
        .setInstance(new QueryObserverAdapter() {
          private boolean[] indexCalled = new boolean[] { false, false };

          private int i = 0;

          public void afterIndexLookup(Collection results) {
            indexCalled[i++] = true;
          }

          public void endQuery() {
            assertTrue(indexCalled[0]);
            assertFalse(indexCalled[1]);
          }

        });
    if(useBindPrm) {
      results = (SelectResults)q.execute(new Object[]{"abc%"});
    }else {
      results = (SelectResults)q.execute();
    }

    rs[0][0] = results;
    rs[0][1] = expectedResults;
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    qs.createIndex("id", IndexType.FUNCTIONAL, "ps.ID", "/pos ps");
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private boolean[] indexCalled = new boolean[] { false, false };

      private int i = 0;

      public void afterIndexLookup(Collection results) {
        indexCalled[i++] = true;
      }

      public void endQuery() {
        //Only one indexed condition should be called
        boolean indexInvoked = false;
        for (int i = 0; i < indexCalled.length; ++i) {
          indexInvoked = indexInvoked || indexCalled[i];
        }
        assertTrue(indexInvoked);
      }

    });

    if(useBindPrm) {
      results = (SelectResults)q.execute(new Object[]{"abc%"});
    }else {
      results = (SelectResults)q.execute();
    }

    rs[0][0] = results;
    rs[0][1] = expectedResults;
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    QueryObserverHolder.setInstance(old);

  }
  
  @Test
  public void testLikePercentageTerminated_5_withoutBindPrms() throws Exception {
    likePercentageTerminated_5(false);
  }
  
  @Test
  public void testLikePercentageTerminated_5_withBindPrms() throws Exception {
    likePercentageTerminated_5(true);
  }
  
  
  
  private void likePercentageTerminated_5(boolean useBindPrm) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);

    char ch = 'd';
    String base = "abc";
    for (int i = 1; i < 6; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + ch;
      ch += 1;
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();

    Query q;
    SelectResults results;
    SelectResults expectedResults;
    String predicate = "";
    if(useBindPrm) {
      predicate = "$1";
    }else {
      predicate = " 'a%c%'";
    }
      q = qs
          .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+ predicate);
      
      if(useBindPrm) {
        results = (SelectResults)q.execute(new Object[]{"a%bc%"});
      }else {
        results = (SelectResults)q.execute();
      }
      
    ResultsBag bag = new ResultsBag(null);
    for (int i = 1; i < 6; ++i) {
      bag.add(region.get(new Integer(i)));
    }
    expectedResults = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
                                                   bag.asSet());
    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    // Create Index
    qs.createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");

    q = qs
        .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+ predicate);
    
    if(useBindPrm) {
      results = (SelectResults)q.execute(new Object[]{"a%bc%"});
    }else {
      results = (SelectResults)q.execute();
    }

    rs = new SelectResults[][] { { results, expectedResults } };
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    
      if(useBindPrm) {
        predicate = "$1";
      }else {
        predicate = "'abc_'";
      }
      q = qs
          .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+ predicate);
      
      if(useBindPrm) {
        results = (SelectResults)q.execute(new Object[]{"abc_"});
      }else {
        results = (SelectResults)q.execute();
      }
      
      rs = new SelectResults[][] { { results, expectedResults } };
      CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
  
      if(useBindPrm) {
        predicate = "$1";
      }else {
        predicate = "'_bc_'";
      }
      q = qs
          .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+ predicate);
      if(useBindPrm) {
        results = (SelectResults)q.execute(new Object[]{"_bc_"});
      }else {
        results = (SelectResults)q.execute();
      }

      rs = new SelectResults[][] { { results, expectedResults } };
      CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
  }
  
  @Test
  public void testEqualityForm_1_withoutBindParams() throws Exception {
    equalityForm_1(false);
  }
  
  @Test
  public void testEqualityForm_1_withBindParams() throws Exception {
    equalityForm_1(true);
  }
  /**
   * Tests simple non % or non _ terminated string which in effect means equality
   * @throws Exception
   */
  private void equalityForm_1(boolean useBindPrms) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);

    char ch = 'd';
    String base = "abc";
    for (int i = 1; i < 6; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + ch;
      ch += 1;
      region.put(new Integer(i), pf);
    }

    base = "abd";
    ch = 'd';
    for (int i = 6; i < 11; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + ch;
      ch += 1;
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();

    Query q;
    SelectResults results;
    SelectResults expectedResults;
    String predicate = "";
    if(useBindPrms) {
      predicate = "$1";
    }else {
      predicate = " 'abcd'";
    }
    q = qs
        .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+ predicate);
    if(useBindPrms) {
      results = (SelectResults)q.execute(new Object[]{"abcd"});
    }else {
      results = (SelectResults)q.execute();
    }
    
    ResultsBag bag = new ResultsBag(null);
   
    bag.add(region.get(new Integer(1)));
    expectedResults = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
                                                   bag.asSet());
    
    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);

    // Create Index
    qs.createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    QueryObserver old = QueryObserverHolder
        .setInstance(new QueryObserverAdapter() {
          private boolean indexCalled = false;

          public void afterIndexLookup(Collection results) {
            indexCalled = true;
          }

          public void endQuery() {
            assertTrue(indexCalled);
          }

        });
    if(useBindPrms) {
      results = (SelectResults)q.execute(new Object[]{"abcd"});
    }else {
      results = (SelectResults)q.execute();
    }
    
    rs[0][0] = results;
    rs[0][1] = expectedResults;
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    QueryObserverHolder.setInstance(old);

  }
  
  @Test
  public void testEqualityForm_2_withoutBindPrms() throws Exception {
    equalityForm_2(false);
  }
  
  @Test
  public void testEqualityForm_2_withBindPrms() throws Exception {
    equalityForm_2(true);
  }
  /**
   * Tests simple \% or \ _ terminated string which in effect means equality
   * 
   * @throws Exception
   */
  private void equalityForm_2(boolean useBindPrms) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);

    String str = "d_";
    String base = "abc";
    for (int i = 1; i < 6; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + str;
      region.put(new Integer(i), pf);
    }

    base = "abc";
    str = "d%";
    for (int i = 6; i < 11; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.status = base + str;
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();

    Query q, q1;
    SelectResults results;
    SelectResults expectedResults;
    String predicate = "";
    if(useBindPrms) {
      predicate = "$1";
    }else {
      predicate = " 'abcd\\_'";
    }
    q = qs
        .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+ predicate);
    if(useBindPrms) {
      results = (SelectResults)q.execute(new Object[]{"abcd\\_"});
    }else {
      results = (SelectResults)q.execute();
    }
    
    ResultsBag bag = new ResultsBag(null);

    for (int i = 1; i < 6; ++i) {
      bag.add(region.get(new Integer(i)));
    }
    expectedResults = new ResultsCollectionWrapper(new ObjectTypeImpl(Object.class),
                                                   bag.asSet());

    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);
    predicate = "";
    if(useBindPrms) {
      predicate = "$1";
    }else {
      predicate = " 'abcd\\%'";
    }
    q1 = qs
        .newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+ predicate);
    if(useBindPrms) {
      results = (SelectResults)q1.execute(new Object[]{"abcd\\%"});
    }else {
      results = (SelectResults)q1.execute();
    }
    bag = new ResultsBag(null);
    for (int i = 6; i < 11; ++i) {
      bag.add(region.get(new Integer(i)));
    }
    SelectResults expectedResults1 = new ResultsCollectionWrapper(
        new ObjectTypeImpl(Object.class), bag.asSet());


    SelectResults rs1[][] = new SelectResults[][] { { results, expectedResults1 } };
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs1, this);

    // Create Index
    qs.createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    QueryObserver old = QueryObserverHolder
        .setInstance(new QueryObserverAdapter() {
          private boolean indexCalled = false;

          public void afterIndexLookup(Collection results) {
            indexCalled = true;
          }

          public void endQuery() {
            assertTrue(indexCalled);
          }

        });
    if(useBindPrms) {
      results = (SelectResults)q.execute(new Object[]{"abcd\\_"});
    }else {
      results = (SelectResults)q.execute();
    }
    rs[0][0] = results;
    rs[0][1] = expectedResults;
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);

    if(useBindPrms) {
      results = (SelectResults)q1.execute(new Object[]{"abcd\\%"});
    }else {
      results = (SelectResults)q1.execute();
    }
    rs1[0][0] = results;
    rs1[0][1] = expectedResults1;
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs1, this);
    QueryObserverHolder.setInstance(old);

  }

  @Test
  public void testRegexMetaCharWithoutBindPrms() throws Exception {
    regexMetaChar(false);
  }
  
  @Test
  public void testRegexMetaCharWithBindPrms() throws Exception {
    regexMetaChar(true);
  }
  
  /**
   * Tests for regular expression meta chars. This has no special meaning with Like.
   * 
   * @throws Exception
   */
  private void regexMetaChar(boolean useBindPrms) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    
    String[] values = new String[] {
        "active",
        "act**ve",
        "ac+t+ve",
        "?act?ve",
        "act)ve^",
        "|+act(ve",
        "act*+|ve",
        "^+act.ve+^",
        "act[]ve",
        "act][ve",
        "act^[a-z]ve",
        "act/ve",
        "inactive",
        "acxtxve",
        "ac(tiv)e",
        "act()ive",
        "act{}ive",
        "act{ive"
       };

    // Add values to region.
    for (int i = 0; i < values.length; i++) {
      region.put(new Integer(i), values[i]);
    }
    
    // Add % and _ with escape char.
    region.put(new Integer(values.length + 1), "act%+ive");
    region.put(new Integer(values.length + 2), "act_+ive");
    
    QueryService qs = cache.getQueryService();
    Query q;
    SelectResults results;
    
    for (int i = 0; i < values.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos.values p where p like '" + values[i] + "'");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos.values p where p like $1");
        results = (SelectResults)q.execute(new Object[] {values[i]});
      }
      List r = results.asList();
      if (r.size() !=1 || !r.get(0).equals(values[i])) {
        fail("Unexpected result. expected :" + values[i] + " for the like predicate: " + values[i] + 
            " found : " + (r.size() ==1? r.get(0): "Result size not equal to 1"));
      }
    }
    
    // Create Index
    qs.createIndex("p", IndexType.FUNCTIONAL, "p", "/pos.values p");

    for (int i = 0; i < values.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos.values p where p like '" + values[i] + "'");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos.values p where p like $1");
        results = (SelectResults)q.execute(new Object[] {values[i]});
      }
      List r = results.asList();
      if (r.size() !=1 || !r.get(0).equals(values[i])) {
        fail("Unexpected result. expected :" + values[i] + " for the like predicate: " + values[i] + 
            " found : " + (r.size() ==1? r.get(0): "Result size not equal to 1"));
      }
    }
  }

  @Test
  public void testEnhancedLikeWithoutBindPrms() throws Exception {
    enhancedLike(false);
  }
  
  @Test
  public void testEnhancedLikeWithBindPrms() throws Exception {
    enhancedLike(true);
  }

  /**
   * Tests with combination of % and _ 
   * Supported from 6.6
   * @throws Exception
   */
  private void enhancedLike(boolean useBindPrms) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    
    String[] values = new String[] {
        "active",
    };

    String[] likePredicates = new String[] {
        "active",
        "act%%ve",
        "a%e",
        "%ctiv%",
        "%c%iv%",
        "%ctive",
        "%%ti%",
        "activ_",
        "_ctive",
        "ac_ive",
        "_c_iv_",
        "_ctiv%",
        "__tive",
        "act__e",
        "a%iv_",
        "a_tiv%",
        "%",
        "ac%",
    };

    for (int i = 0; i < values.length; i++) {
      region.put(new Integer(i), values[i]);
    }

    QueryService qs = cache.getQueryService();
    Query q;
    SelectResults results;
    
    for (int i = 0; i < likePredicates.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos.values p where p like '" + likePredicates[i] + "'");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos.values p where p like $1");
        results = (SelectResults)q.execute(new Object[] {likePredicates[i]});
      }
      List r = results.asList();
      if (r.size() !=1 || !r.get(0).equals(values[0])) {
        fail("Unexpected result. expected :" + values[0] + " for the like predicate: " + likePredicates[i] + 
            " found : " + (r.size() ==1? r.get(0): "Result size not equal to 1"));
      }
    }
    
    // Create Index
    qs.createIndex("p", IndexType.FUNCTIONAL, "p", "/pos.values p");

    for (int i = 0; i < likePredicates.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos.values p where p like '" + likePredicates[i] + "'");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos.values p where p like $1");
        results = (SelectResults)q.execute(new Object[] {likePredicates[i]});
      }
      List r = results.asList();
      if (r.size() !=1 || !r.get(0).equals(values[0])) {
        fail("Unexpected result. expected :" + values[0] + " for the like predicate: " + likePredicates[i] + 
            " found : " + (r.size() ==1? r.get(0): "Result size not equal to 1"));
      }
    }
  }

  @Test
  public void testEnhancedLike2WithoutBindPrms() throws Exception {
    enhancedLike2(false);
  }
  
  @Test
  public void testEnhancedLike2WithBindPrms() throws Exception {
    enhancedLike2(true);
  }
  
  private void enhancedLike2(boolean useBindPrms) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    
    String[] values = new String[] {
        "ac\\tive",
        "X\\\\X",
        "Y%Y",
        "Z\\%Z",
        "pass\\ive",
        "inact\\%+ive",
        "1inact\\_+ive",
    };

    String[] likePredicates = new String[] {
        "ac\\\\tive",
        "ac\\\\%",
        "ac_tive",
        "Y\\%Y",
        "X__X",
        "X%X",
        "Z\\\\\\%Z",
        "inact\\\\%+ive",
        "1inact\\\\_+ive",
    };
    
    String[] result = new String[] {
        "ac\\tive",
        "ac\\tive",
        "ac\\tive",
        "Y%Y",
        "X\\\\X",
        "X\\\\X",
        "Z\\%Z",
        "inact\\%+ive",
        "1inact\\_+ive",
    };
    

    for (int i = 0; i < values.length; i++) {
      region.put(new Integer(i), values[i]);
    }

    QueryService qs = cache.getQueryService();
    Query q;
    SelectResults results;
    
    for (int i = 0; i < likePredicates.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos.values p where p like '" + likePredicates[i] + "'");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos.values p where p like $1");
        results = (SelectResults)q.execute(new Object[] {likePredicates[i]});
      }
      List r = results.asList();
      if (r.size() !=1 || !r.get(0).equals(result[i])) {
        fail("Unexpected result. expected :" + result[i] + " for the like predicate: " + likePredicates[i] + 
            " found : " + (r.size() ==1? r.get(0): "Result size not equal to 1"));
      }
    }
    
    // Create Index
    qs.createIndex("p", IndexType.FUNCTIONAL, "p", "/pos.values p");

    for (int i = 0; i < likePredicates.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos.values p where p like '" + likePredicates[i] + "'");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos.values p where p like $1");
        results = (SelectResults)q.execute(new Object[] {likePredicates[i]});
      }
      List r = results.asList();
      if (r.size() !=1 || !r.get(0).equals(result[i])) {
        fail("Unexpected result. expected :" + result[i] + " for the like predicate: " + likePredicates[i] + 
            " found : " + (r.size() ==1? r.get(0): "Result size not equal to 1"));
      }
    }
  }

  /**
   * Query with index on other fields.
   * @throws Exception
   */
  @Test
  public void testLikeWithOtherIndexedField() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);

    String base = "abc";
    for (int i = 1; i <= 10; i++) {
      Portfolio pf = new Portfolio(i);
      pf.pkid = "1";
      if ((i%4) == 0) {
        pf.status = base;
      } else if ((i <= 2)) {
        pf.pkid = "2";
      }
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();
    
    Query q;
    SelectResults results;
    SelectResults expectedResults;
    
    int expectedResultSize = 2;
    
    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%'");
    results = (SelectResults)q.execute();
    if (results.size() != expectedResultSize) {
      fail("Unexpected result. expected :" + expectedResultSize + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' or ps.pkid = '2' ");
    results = (SelectResults)q.execute();
    if (results.size() != (expectedResultSize * 2)) {
      fail("Unexpected result. expected :" + (expectedResultSize * 2) + " found : " + results.size());    
    }

    // Query to be compared with indexed results.
    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' and ps.pkid = '1' ");
    expectedResults = (SelectResults)q.execute();
    if (expectedResults.size() != expectedResultSize) {
      fail("Unexpected result. expected :" + expectedResultSize + " found : " + expectedResults.size());    
    }

    // Create Index
    qs.createIndex("pkid", IndexType.FUNCTIONAL, "ps.pkid", "/pos ps");

    QueryObserver old = QueryObserverHolder
    .setInstance(new QueryObserverAdapter() {
      private boolean indexCalled = false;
      public void afterIndexLookup(Collection results) {
        indexCalled = true;
      }
      public void endQuery() {
        assertTrue(indexCalled);
      }
    });
    
    results = (SelectResults)q.execute();
    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };

    //rs[0][0] = results;
    //rs[0][1] = expectedResults;
    if (results.size() != expectedResultSize) {
      fail("Unexpected result. expected :" + expectedResultSize + " found : " + results.size());    
    }
    // compare results.
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' or ps.pkid = '2' ");
    results = (SelectResults)q.execute();
    if (results.size() != (expectedResultSize * 2)) {
      fail("Unexpected result. expected :" + (expectedResultSize * 2) + " found : " + results.size());    
    }

    QueryObserverHolder.setInstance(old);

  }

 /**
   * Query with index on other fields.
   * @throws Exception
   */
  @Test
  public void testLikeWithOtherIndexedField2() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    int size = 10;
    
    String base = "abc";
    for (int i = 1; i <= size; i++) {
      Portfolio pf = new Portfolio(i);
      pf.pkid = "1";
      if ((i%4) == 0) {
        pf.status = base;
      } else if ((i <= 2)) {
        pf.pkid = "2";
      }
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();
    
    Query q;
    SelectResults results;
    SelectResults expectedResults;
    
    int expectedResultSize = 2;
    
    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%'");
    results = (SelectResults)q.execute();
    if (results.size() != expectedResultSize) {
      fail("Unexpected result. expected :" + expectedResultSize + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' or ps.pkid = '2' ");
    results = (SelectResults)q.execute();
    if (results.size() != (expectedResultSize * 2)) {
      fail("Unexpected result. expected :" + (expectedResultSize * 2) + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' or ps.ID > 0 ");
    results = (SelectResults)q.execute();
    if (results.size() != size) {
      fail("Unexpected result. expected :" + size + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' or ps.ID > 4 ");
    results = (SelectResults)q.execute();
    if (results.size() != 7) {
      fail("Unexpected result. expected :" + 7 + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' and ps.ID > 3 ");
    results = (SelectResults)q.execute();
    if (results.size() != 2) {
      fail("Unexpected result. expected :" + 2 + " found : " + results.size());    
    }

    // Query to be compared with indexed results.
    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' and ps.pkid = '1' ");
    expectedResults = (SelectResults)q.execute();
    if (expectedResults.size() != expectedResultSize) {
      fail("Unexpected result. expected :" + expectedResultSize + " found : " + expectedResults.size());    
    }

    // Create Index
    qs.createIndex("pkid", IndexType.FUNCTIONAL, "ps.pkid", "/pos ps");
    qs.createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    qs.createIndex("id", IndexType.FUNCTIONAL, "ps.ID", "/pos ps");
    
    QueryObserver old = QueryObserverHolder
    .setInstance(new QueryObserverAdapter() {
      private boolean indexCalled = false;
      public void afterIndexLookup(Collection results) {
        indexCalled = true;
      }
      public void endQuery() {
        assertTrue(indexCalled);
      }
    });
    
    results = (SelectResults)q.execute();
    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };

    //rs[0][0] = results;
    //rs[0][1] = expectedResults;
    if (results.size() != expectedResultSize) {
      fail("Unexpected result. expected :" + expectedResultSize + " found : " + results.size());    
    }
    // compare results.
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '_b_' or ps.pkid = '2' ");
    results = (SelectResults)q.execute();
    if (results.size() != (expectedResultSize * 2)) {
      fail("Unexpected result. expected :" + (expectedResultSize * 2) + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' or ps.ID > 0 ");
    results = (SelectResults)q.execute();
    if (results.size() != size) {
      fail("Unexpected result. expected :" + size + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' or ps.ID > 4 ");
    results = (SelectResults)q.execute();
    if (results.size() != 7) {
      fail("Unexpected result. expected :" + 7 + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE ps.status like '%b%' and ps.ID > 3 ");
    results = (SelectResults)q.execute();
    if (results.size() != 2) {
      fail("Unexpected result. expected :" + 2 + " found : " + results.size());    
    }

    QueryObserverHolder.setInstance(old);

  }
  
  @Test
  public void testMultipleWhereClausesWithIndexes() throws Exception{
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    QueryService qs = cache.getQueryService();
    Query q;
    String[] queries = new String[]{
        " SELECT  status, pkid FROM /pos  WHERE status like 'inactive' and pkid like '1' ",
        " SELECT  status, pkid FROM /pos  WHERE status like 'active' or pkid like '1' ",
        " SELECT  status, pkid FROM /pos  WHERE status like 'in%' and pkid like '1' ",
        " SELECT  status  FROM /pos  WHERE status like 'in%' or pkid like '1'", 
        " SELECT  pkid FROM /pos  WHERE status like 'inact%' and pkid like '1%' ",
        " SELECT  *  FROM /pos  WHERE status like 'inact%' or pkid like '1%' ",
        " SELECT  *  FROM /pos  WHERE status like 'inactiv_' or pkid like '1%' ",
        " SELECT  status, pkid  FROM /pos  WHERE status like '_nactive' or pkid like '1%' ",
        " SELECT  *  FROM /pos  WHERE status like '_nac%ive' or pkid like '1%' ",
        " SELECT  *  FROM /pos  WHERE status like 'in_ctive' or pkid like '1%' ",
        " SELECT  *  FROM /pos  WHERE status like 'in_ctive' or pkid like '1_' ",
        " SELECT  status, pkid FROM /pos  WHERE status like '%ctive' and pkid like '1%' ",
        " SELECT  *  FROM /pos  WHERE NOT (status like 'active' or pkid like '1') ",
        " SELECT  *  FROM /pos  WHERE NOT (status like 'active' or pkid like '1%') ",
        " SELECT  *  FROM /pos  WHERE NOT (status like 'acti%' and pkid like '1%') ",
        " SELECT  *  FROM /pos  WHERE NOT (status like 'acti%' or pkid like '1%') ",
        " SELECT  *  FROM /pos  WHERE NOT (status like 'active' and pkid like '1%') ",
        " SELECT  *  FROM /pos  WHERE NOT (status like 'active' or pkid like '1%') ",
        " SELECT  *  FROM /pos  WHERE NOT (status like 'active' or pkid like '11' or pkid like '1') ",
        " SELECT  *  FROM /pos  WHERE status like '%' and pkid like '1%' ",
        " SELECT  *  FROM /pos  WHERE pkid like '_'" ,
        " SELECT  *  FROM /pos  WHERE status like '.*tive' ",
        " SELECT  *  FROM /pos  WHERE pkid like '1+' ",
        " SELECT  *  FROM /pos  WHERE unicodeṤtring like 'ṤṶẐ' ",
        " SELECT  *  FROM /pos  WHERE unicodeṤtring like 'ṤṶ%' ",
        " SELECT  *  FROM /pos p, p.positions.values v  WHERE v.secId like 'I%' ",
        " SELECT  *  FROM /pos p, p.positions.values v  WHERE v.secId like '%L' ",
        " SELECT  *  FROM /pos p, p.positions.values v  WHERE v.secId like 'A%L' ",
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];

    for (int i = 0; i < 20; i++) {
      Portfolio pf = new Portfolio(i);
      region.put(new Integer(i), pf);
    }

    for (int i = 0; i < queries.length; i++) {
      q = qs.newQuery(queries[i]); 
      sr[i][0] = (SelectResults)q.execute();
    }
    
    // Create index.
    qs.createIndex("pkidIndex", "pkid", "/pos");
    qs.createIndex("statusIndex", "status", "/pos");
    qs.createIndex("unicodeṤtringIndex", "unicodeṤtring", "/pos");
    qs.createIndex("secIdIndex", "v.secId", "/pos p, p.positions.values v");

    for (int i = 0; i < queries.length; i++) {
      q = qs.newQuery(queries[i]); 
      try {
        sr[i][1] = (SelectResults)q.execute();
      } catch (Exception e) {
        fail("Query execution failed for: " + queries[i]);
      }
    }
    
    // compare results.
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);

    SelectResults results = null;
    
    q = qs.newQuery("SELECT  status  FROM /pos  WHERE status like 'inactive' and pkid like '1' ");
    results = (SelectResults)q.execute();
    if (results.size() != 1) {
      fail("Unexpected result. expected :" + 1 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  pkid  FROM /pos  WHERE status like 'active' or pkid like '1' ");
    results = (SelectResults)q.execute();
    if (results.size() != 11) {
      fail("Unexpected result. expected :" + 11 + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like 'in%' and pkid like '1' ");
    results = (SelectResults)q.execute();
    if (results.size() != 1) {
      fail("Unexpected result. expected :" + 1 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like 'in%' or pkid like '1' ");
    results = (SelectResults)q.execute();
    if (results.size() != 10) {
      fail("Unexpected result. expected :" + 10 + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like 'inact%' and pkid like '1%' ");
    results = (SelectResults)q.execute();
    if (results.size() != 6) {
      fail("Unexpected result. expected :" + 6 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like 'inact%' or pkid like '1%' ");
    results = (SelectResults)q.execute();
    if (results.size() != 15) {
      fail("Unexpected result. expected :" + 15 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like 'inactiv_' or pkid like '1%' ");
    results = (SelectResults)q.execute();
    if (results.size() != 15) {
      fail("Unexpected result. expected :" + 15 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like '_nactive' or pkid like '1%' ");
    results = (SelectResults)q.execute();
    if (results.size() != 15) {
      fail("Unexpected result. expected :" + 15 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like '_nac%ive' or pkid like '1%' ");
    results = (SelectResults)q.execute();
    if (results.size() != 15) {
      fail("Unexpected result. expected :" + 15 + " found : " + results.size());    
    }
   
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like 'in_ctive' or pkid like '1%' ");
    results = (SelectResults)q.execute();
    if (results.size() != 15) {
      fail("Unexpected result. expected :" + 15 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like 'in_ctive' or pkid like '1_' ");
    results = (SelectResults)q.execute();
    if (results.size() != 15) {
      fail("Unexpected result. expected :" + 15 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like '%ctive' and pkid like '1%' ");
    results = (SelectResults)q.execute();
    if (results.size() != 11) {
      fail("Unexpected result. expected :" + 11 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE NOT (status like 'active' or pkid like '1') ");
    results = (SelectResults)q.execute();
    if (results.size() != 9) {
      fail("Unexpected result. expected :" + 9 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE NOT (status like 'active' or pkid like '1%') ");
    results = (SelectResults)q.execute();
    if (results.size() != 4) {
      fail("Unexpected result. expected :" + 4 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE NOT (status like 'acti%' and pkid like '1%') ");
    results = (SelectResults)q.execute();
    if (results.size() != 15) {
      fail("Unexpected result. expected :" + 15 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE NOT (status like 'acti%' or pkid like '1%') ");
    results = (SelectResults)q.execute();
    if (results.size() != 4) {
      fail("Unexpected result. expected :" + 4 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE NOT (status like 'active' and pkid like '1%') ");
    results = (SelectResults)q.execute();
    if (results.size() != 15) {
      fail("Unexpected result. expected :" + 15 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE NOT (status like 'active' or pkid like '1%') ");
    results = (SelectResults)q.execute();
    if (results.size() != 4) {
      fail("Unexpected result. expected :" + 4 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE NOT (status like 'active' or pkid like '11' or pkid like '1') ");
    results = (SelectResults)q.execute();
    if (results.size() != 8) {
      fail("Unexpected result. expected :" + 8 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE status like '%' and pkid like '1%' ");
    results = (SelectResults)q.execute();
    if (results.size() != 11) {
      fail("Unexpected result. expected :" + 11 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE pkid like '_' ");
    results = (SelectResults)q.execute();
    if (results.size() != 10) {
      fail("Unexpected result. expected :" + 10 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE pkid like '.*tive' ");
    results = (SelectResults)q.execute();
    if (results.size() != 0) {
      fail("Unexpected result. expected :" + 0 + " found : " + results.size());    
    }
    
    q = qs.newQuery(" SELECT  *  FROM /pos  WHERE pkid like '1+' ");
    results = (SelectResults)q.execute();
    if (results.size() != 0) {
      fail("Unexpected result. expected :" + 0 + " found : " + results.size());    
    }

  }
  
  @Test
  public void testLikePredicateOnNullValues() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);

    char ch = 'd';
    String base = "abc";
    for (int i = 1; i < 6; i++) {
      Portfolio pf = new Portfolio(i);
      pf.pkid = "abc";
      if (i % 2 == 0) {
        pf.pkid = null;
        pf.status = "like";
      } else if (i == 3) {
        pf.status = null;
      } 
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();
    Query q;
    SelectResults results;
    
    String query[] = new String[] {
        "SELECT distinct *  FROM /pos ps WHERE ps.pkid like '%b%'",
        "SELECT * FROM /pos ps WHERE ps.pkid like '%b%' and ps.status like '%ctiv%'",
        "SELECT * FROM /pos ps WHERE ps.pkid like '_bc'",        
        "SELECT pkid FROM /pos ps WHERE ps.pkid like 'abc%'",     
        "SELECT pkid FROM /pos ps WHERE ps.pkid = 'abc'",     
        "SELECT pkid FROM /pos ps WHERE ps.pkid like '%b%' and ps.status = 'like'",
        "SELECT pkid FROM /pos ps WHERE ps.pkid like '%b%' and ps.status like '%ike'",
        "SELECT pkid FROM /pos ps WHERE ps.pkid like '%b%' and ps.pkid like '_bc'",
        "SELECT pkid FROM /pos ps WHERE ps.pkid like 'ml%' or ps.status = 'like'",
    };
    
    // null check
    for (int i=0; i < query.length; i++) {   
      q = qs.newQuery(query[i]);
      results = (SelectResults)q.execute(); // No NPE.
    }
    
    // validate results
    q = qs.newQuery(query[0]);
    results = (SelectResults)q.execute(); 
    assertEquals("Result size is not as expected", 3, results.size());
    
    q = qs.newQuery(query[1]);
    results = (SelectResults)q.execute(); 
    assertEquals("Result size is not as expected", 2, results.size());
    
    // Create index.
    qs.createIndex("pkid",IndexType.FUNCTIONAL, "ps.pkid", "/pos ps");
    qs.createIndex("status",IndexType.FUNCTIONAL, "ps.status", "/pos ps");

    for (int i=0; i < query.length; i++) {   
      q = qs.newQuery(query[i]);
      results = (SelectResults)q.execute(); // No NPE.
    }

    ResultsBag bag = new ResultsBag(null);
    for (int i = 1; i < 6; ++i) {
      bag.add(region.get(new Integer(i)));
    }
  }

  /**
   * Tests with combination of % and _  and NOT with LIKE
   * Supported from 6.6
   * @throws Exception
   */
  private void enhancedNotLike(boolean useBindPrms, boolean useIndex) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    
    String[] values = new String[] {
        "active",
        "inactive",
    };

    String[] likePredicates1 = new String[] {
        "active",
        "act%%ve",
        "a%e",
        "activ_",
        "ac_ive",
        "act__e",
        "a%iv_",
        "a_tiv%",
        "ac%",
    };

    String[] likePredicates2 = new String[] {
        "%ctiv%",
        "%c%iv%",
        "%ctive",
        "%%ti%",
        "%",
    };

    String[] likePredicates3 = new String[] {
        
        "___ctive",
        "___c_iv_",
        "___ctiv%",
        "____tive",
    };
    for (int i = 0; i < values.length; i++) {
      region.put(new Integer(i), values[i]);
    }

    QueryService qs = cache.getQueryService();
    
    if(useIndex){
      qs.createIndex("p", IndexType.FUNCTIONAL, "p", "/pos.values p");
    }
    Query q;
    SelectResults results;
    
    for (int i = 0; i < likePredicates1.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos p where NOT (p like '" + likePredicates1[i] + "')");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos p where NOT (p like $1)");
        results = (SelectResults)q.execute(new Object[] {likePredicates1[i]});
      }
      List r = results.asList();
      if (r.size() !=1 || !r.get(0).equals(values[1])) {
        fail("Unexprected result. expected :" + values[1] + " for the like predicate1: " + likePredicates1[i] + 
            " found : " + (r.size() == 1 ? r.get(0) : "Result size not equal to 1"));
      }
    }

    for (int i = 0; i < likePredicates2.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos p where NOT (p like '" + likePredicates2[i] + "')");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos p where NOT (p like $1)");
        results = (SelectResults)q.execute(new Object[] {likePredicates2[i]});
      }
      List r = results.asList();
      if (r.size() !=0) {
        fail("Unexprected result. expected nothing for the like predicate2: " + likePredicates2[i] + 
            " found : " + (r.size() != 0 ? r.get(0) + " Result size not equal to 0" : ""));
      }
    }

    for (int i = 0; i < likePredicates3.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos p where NOT (p like '" + likePredicates3[i] + "')");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos p where NOT (p like $1)");
        results = (SelectResults)q.execute(new Object[] {likePredicates3[i]});
      }
      List r = results.asList();
      if (r.size() !=1 || !r.get(0).equals(values[0])) {
        fail("Unexprected result. expected :" + values[0] + " for the like predicate3: " + likePredicates3[i] + 
            " found : " + (r.size() == 1 ? r.get(0) : "Result size not equal to 1"));
      }
    }
  }

  @Test
  public void testEnhancedNotLikeWithoutBindPrms() throws Exception {
    enhancedNotLike(false,false);
  }
  
  @Test
  public void testEnhancedNotLikeWithBindPrms() throws Exception {
    enhancedNotLike(true,false);
  }

  @Test
  public void testEnhancedNotLikeWithoutBindPrmsWithIndex() throws Exception {
    enhancedNotLike(true,true);
  }
  
  @Test
  public void testEnhancedNotLike2WithoutBindPrms() throws Exception {
    enhancedNotLike2(false, false);
  }
  
  @Test
  public void testEnhancedNotLike2WithBindPrms() throws Exception {
    enhancedNotLike2(true, false);
  }
  
  @Test
  public void testEnhancedNotLike2WithoutBindPrmsWithIndex() throws Exception {
    enhancedNotLike2(false, true);
  }
  
  
  private void enhancedNotLike2(boolean useBindPrms, boolean useIndex) throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    
    String[] values = new String[] {
        "ac\\tive",
        "X\\\\X",
        "Y%Y",
        "Z\\%Z",
        "pass\\ive",
        "inact\\%+ive",
        "1inact\\_+ive",
    };

    String[] likePredicates = new String[] {
        "ac\\\\tive",
        "ac\\\\%",
        "ac_tive",
        "Y\\%Y",
        "X__X",
        "X%X",
        "Z\\\\\\%Z",
        "inact\\\\%+ive",
        "1inact\\\\_+ive",
    };
    
    String[] result = new String[] {
        "ac\\tive",
        "ac\\tive",
        "ac\\tive",
        "Y%Y",
        "X\\\\X",
        "X\\\\X",
        "Z\\%Z",
        "inact\\%+ive",
        "1inact\\_+ive",
    };
    

    for (int i = 0; i < values.length; i++) {
      region.put(new Integer(i), values[i]);
    }
    
    QueryService qs = cache.getQueryService();
    Query q;
    SelectResults results;
    if(useIndex){
      qs.createIndex("p", IndexType.FUNCTIONAL, "p", "/pos.values p");
    }
    for (int i = 0; i < likePredicates.length; i++) {
      if(!useBindPrms) {
        q = qs.newQuery("select p from /pos.values p where NOT (p like '" + likePredicates[i] + "')");      
        results = (SelectResults)q.execute();
      } else {
        q = qs.newQuery("select p from /pos.values p where NOT (p like $1)");
        results = (SelectResults)q.execute(new Object[] {likePredicates[i]});
      }
      List r = results.asList();
      if (r.size() !=6 ) {
        fail("Unexpected result size: " + r.size() + " for query: " + q.getQueryString());
      }
    }
  }

  /**
   * Query with index on other fields.
   * @throws Exception
   */
  @Test
  public void testNotLikeWithOtherIndexedField2() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    int size = 10;
    
    String base = "abc";
    for (int i = 1; i <= size; i++) {
      Portfolio pf = new Portfolio(i);
      pf.pkid = "1";
      if ((i%4) == 0) {
        pf.status = base;
      } else if ((i <= 2)) {
        pf.pkid = "2";
      }
      region.put(new Integer(i), pf);
    }

    QueryService qs = cache.getQueryService();
    
    Query q;
    SelectResults results;
    SelectResults expectedResults;
    
    int expectedResultSize = 2;
    
    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '%b%')");
    results = (SelectResults)q.execute();
    if (results.size() != expectedResultSize * 4) {
      fail("Unexpected result. expected :" + expectedResultSize * 4 + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '%b%' or ps.pkid = '2')");
    results = (SelectResults)q.execute();
    if (results.size() != (expectedResultSize * 3)) {
      fail("Unexpected result. expected :" + (expectedResultSize * 3) + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '%b%' or ps.ID > 0 )");
    results = (SelectResults)q.execute();
    if (results.size() != 0) {
      fail("Unexpected result. expected :" + 0 + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '%b%') or ps.ID > 4 ");
    results = (SelectResults)q.execute();
    if (results.size() != 9) {
      fail("Unexpected result. expected :" + 9 + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '%b%' and ps.ID > 3 )");
    results = (SelectResults)q.execute();
    if (results.size() != 8) {
      fail("Unexpected result. expected :" + 5 + " found : " + results.size());    
    }

    // Query to be compared with indexed results.
    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '%b%' and ps.pkid = '1' )");
    expectedResults = (SelectResults)q.execute();
    if (expectedResults.size() != 8) {
      fail("Unexpected result. expected :" + size + " found : " + expectedResults.size());    
    }

    // Create Index
    qs.createIndex("pkid", IndexType.FUNCTIONAL, "ps.pkid", "/pos ps");
    qs.createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    qs.createIndex("id", IndexType.FUNCTIONAL, "ps.ID", "/pos ps");
    
    
    results = (SelectResults)q.execute();
    SelectResults rs[][] = new SelectResults[][] { { results, expectedResults } };

    if (results.size() != expectedResults.size()) {
      fail("Unexpected result. expected :" + expectedResults.size() + " found : " + results.size());    
    }
    // compare results.
    CacheUtils.compareResultsOfWithAndWithoutIndex(rs, this);

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '%b%' and ps.ID > 3 )");
    results = (SelectResults)q.execute();
    if (results.size() != 8) {
      fail("Unexpected result. expected :" + 5 + " found : " + results.size());    
    }

    // Index will only be used in OR junctions if NOT is used with LIKE.
    QueryObserver old = QueryObserverHolder
    .setInstance(new QueryObserverAdapter() {
      private boolean indexCalled = false;
      public void afterIndexLookup(Collection results) {
        indexCalled = true;
      }
      public void endQuery() {
        assertTrue(indexCalled);
      }
    });

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '_b_' or ps.pkid = '2')");
    results = (SelectResults)q.execute();
    if (results.size() != (expectedResultSize * 3)) {
      fail("Unexpected result. expected :" + (expectedResultSize * 3) + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '%b%' or ps.ID > 0 )");
    results = (SelectResults)q.execute();
    if (results.size() != 0) {
      fail("Unexpected result. expected :" + 0 + " found : " + results.size());    
    }

    q = qs.newQuery(" SELECT  *  FROM /pos ps WHERE NOT (ps.status like '%b%') or ps.ID > 4");
    results = (SelectResults)q.execute();
    if (results.size() != 9) {
      fail("Unexpected result. expected :" + 9 + " found : " + results.size());    
    }

    QueryObserverHolder.setInstance(old);

  }
  
  @Test
  public void testLikeWithNewLine() throws Exception {
    likeWithNewLine(false);
  }
  
  @Test
  public void testLikeWithNewLineWithIndex() throws Exception {
    likeWithNewLine(true);
  }
  
  public void likeWithNewLine(boolean useIndex) throws Exception {
    // setup
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion("pos", regionAttributes);

    region.put("abc", "abc\ndef");
    region.put("opq", "\nopq");
    region.put("mns", "opq\n");
    QueryService qs = cache.getQueryService();
    if(useIndex){
      qs.createIndex("p", IndexType.FUNCTIONAL, "p", "/pos p");
    }
    Query query = qs.newQuery("select * from /pos a where a like '%bc%'");
    SelectResults sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 1);
    query = qs.newQuery("select * from /pos a where a like '%bc\n%'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 1);
    query = qs.newQuery("select * from /pos a where a like '%bcde%'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 0); // no results as newline is required
                                       // whitespace
    query = qs.newQuery("select * from /pos a where a like '%bc\nde%'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 1);
    query = qs.newQuery("select * from /pos a where a like '%de%'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 1);
    query = qs.newQuery("select * from /pos a where a like '%\nde%'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 1);
    query = qs.newQuery("select * from /pos a where a like '%bc%de%'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 1);
    query = qs.newQuery("select * from /pos a where a like '%zyx%'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 0);
    query = qs.newQuery("select * from /pos a where a like '%\n%'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 3); // find newline anywhere in string
    query = qs.newQuery("select * from /pos a where a like '\n%'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 1);
    query = qs.newQuery("select * from /pos a where a like '%\n'");
    sr = (SelectResults) query.execute();
    Assert.assertEquals(sr.size(), 1);
  }
  
  
  /**
   * Test to verify that query executes using index even when the index is being
   * removed during the execution
   * 
   * @throws Exception
   */

  @Test
  public void testRemoveIndexDuringQueryinRR() throws Exception {
    removeIndexDuringQuery(false);
  }

  @Test
  public void testRemoveIndexDuringQueryinPR() throws Exception {
    removeIndexDuringQuery(true);
  }

  public void removeIndexDuringQuery(boolean isPr) throws Exception {
    String regionName = "exampleRegion";
    String name = "/" + regionName;

    Cache cache = CacheUtils.getCache();
    Region r1 = null;
    if (isPr) {
      r1 = cache.createRegionFactory(RegionShortcut.PARTITION).create(
          regionName);
    } else {
      r1 = cache.createRegionFactory(RegionShortcut.REPLICATE).create(
          regionName);
    }

    QueryService qs = cache.getQueryService();
    qs.createIndex("status", "status", name);
    assertEquals(cache.getQueryService().getIndexes().size(), 1);

    QueryObserver old = QueryObserverHolder
        .setInstance(new QueryObserverAdapter() {
          private boolean indexCalled = false;

          public void afterIndexLookup(Collection results) {
            indexCalled = true;
          }

          public void endQuery() {
            assertTrue(indexCalled);
          }
        });

    // set the test hook
    IndexManager.testHook = new LikeQueryIndexTestHook();

    for (int i = 0; i < 10; i++) {
      r1.put("key-" + i, new Portfolio(i));
    }

    SelectResults rs[][] = new SelectResults[1][2];
    String query = "select distinct * from " + name
        + " where status like 'act%'";
    rs[0][0] = (SelectResults) cache.getQueryService().newQuery(query)
        .execute();
    assertEquals(5, rs[0][0].size());

    // wait for remove to complete
    ThreadUtils.join(LikeQueryIndexTestHook.th, 60 * 1000);

    // The index should have been removed by now
    assertEquals(0, cache.getQueryService().getIndexes().size());

    // remove the test hook
    IndexManager.testHook = null;

    // create the same index again and execute the query
    qs.createIndex("status", "status", name);

    rs[0][1] = (SelectResults) cache.getQueryService().newQuery(query)
        .execute();
    assertEquals(5, rs[0][1].size());

    CacheUtils.compareResultsOfWithAndWithoutIndex(rs);
    QueryObserverHolder.setInstance(old);

  }

  @Test
  public void testQueryExecutionMultipleTimes() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    Region region2 = cache.createRegion("pos2", regionAttributes);
    
    // Create Index
    Index i1 = cache.getQueryService().createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    Index i2 = cache.getQueryService().createIndex("description", IndexType.FUNCTIONAL, "ps.description", "/pos ps");
    Index i3 = cache.getQueryService().createIndex("description2", IndexType.FUNCTIONAL, "ps2.description", "/pos2 ps2");
    
    for (int i = 0; i < 10; i++) {
      Portfolio p = new Portfolio(i);
      region.put("key-" + i, p);
      region2.put("key-" + i, p);
    }
    
    executeQueryMultipleTimes("SELECT distinct *  FROM /pos ps WHERE ps.status like 'in%ve'", false);
    assertEquals(2, i1.getStatistics().getTotalUses());
    assertEquals(0, i2.getStatistics().getTotalUses());
    assertEquals(0, i3.getStatistics().getTotalUses());

    executeQueryMultipleTimes("SELECT distinct *  FROM /pos ps WHERE ps.status like 'in%ve' or ps.description like 'X%X'", false);
    assertEquals(4, i1.getStatistics().getTotalUses());
    assertEquals(2, i2.getStatistics().getTotalUses());
    assertEquals(0, i3.getStatistics().getTotalUses());


    executeQueryMultipleTimes("SELECT distinct *  FROM /pos ps WHERE ps.ID >= 0 and ps.status like 'in%ve'", false);
    assertEquals(6, i1.getStatistics().getTotalUses());
    assertEquals(2, i2.getStatistics().getTotalUses());
    assertEquals(0, i3.getStatistics().getTotalUses());


    executeQueryMultipleTimes("SELECT distinct *  FROM /pos ps1, /pos2 ps2 WHERE ps1.status like 'in%ve' or ps2.description like 'X%X'", false);
    assertEquals(8, i1.getStatistics().getTotalUses());
    assertEquals(2, i2.getStatistics().getTotalUses());
    assertEquals(2, i3.getStatistics().getTotalUses());
    
  }
  
  @Test
  public void testQueryExecutionMultipleTimesWithBindParams() throws Exception {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    Region region2 = cache.createRegion("pos2", regionAttributes);
    
    // Create Index
    Index i1 = cache.getQueryService().createIndex("status", IndexType.FUNCTIONAL, "ps.status", "/pos ps");
    Index i2 = cache.getQueryService().createIndex("description", IndexType.FUNCTIONAL, "ps.description", "/pos ps");
    Index i3 = cache.getQueryService().createIndex("description2", IndexType.FUNCTIONAL, "ps2.description", "/pos2 ps2");
    
    for (int i = 0; i < 10; i++) {
      Portfolio p = new Portfolio(i);
      region.put("key-" + i, p);
      region2.put("key-" + i, p);
    }
    
    executeQueryMultipleTimes("SELECT distinct *  FROM /pos ps WHERE ps.status like $1", true);
    assertEquals(2, i1.getStatistics().getTotalUses());
    assertEquals(0, i2.getStatistics().getTotalUses());
    assertEquals(0, i3.getStatistics().getTotalUses());


    executeQueryMultipleTimes("SELECT distinct *  FROM /pos ps WHERE ps.status like $1 or ps.description like $2", true);
    assertEquals(4, i1.getStatistics().getTotalUses());
    assertEquals(2, i2.getStatistics().getTotalUses());
    assertEquals(0, i3.getStatistics().getTotalUses());


    executeQueryMultipleTimes("SELECT distinct *  FROM /pos ps WHERE ps.ID >= 0 and ps.status like $1", true);
    assertEquals(6, i1.getStatistics().getTotalUses());
    assertEquals(2, i2.getStatistics().getTotalUses());
    assertEquals(0, i3.getStatistics().getTotalUses());


    executeQueryMultipleTimes("SELECT distinct *  FROM /pos ps1, /pos2 ps2 WHERE ps1.status like $1 or ps2.description like $2", true);
    assertEquals(8, i1.getStatistics().getTotalUses());
    assertEquals(2, i2.getStatistics().getTotalUses());
    assertEquals(2, i3.getStatistics().getTotalUses());
    
  }
  
  private void executeQueryMultipleTimes(String  queryStr, boolean useBindParam) throws Exception {
    
    QueryService qs = CacheUtils.getCache().getQueryService();
    Region region = CacheUtils.getCache().getRegion("pos");

    Query q = qs.newQuery(queryStr);
    SelectResults results;
    SelectResults expectedResults;
   
    if(useBindParam) {
      results = (SelectResults)q.execute(new Object[]{"in%ve","X%X"});
    }else {
      results = (SelectResults)q.execute();
    }
 
    //Execute the query again to verify if it is still using
    //the index
    if(useBindParam) {
      results = (SelectResults)q.execute(new Object[]{"in%ve","X%X"});
    }else {
      results = (SelectResults)q.execute();
    }
  }

  @Test
  public void testInvalidQuery() throws Exception {
    executeInvalidQueryWithLike(false);
  }
  
  @Test
  public void testInvalidQueryWithBindParams() throws Exception {
    executeInvalidQueryWithLike(true);
  }
  
  public void executeInvalidQueryWithLike(boolean useBindParam) {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    for(int i = 0 ; i < 10; i++) {
      region.put("key-"+i, new Portfolio(i));
    }
    region.put("key-"+11, new PortfolioModifiedStatus(11));
    QueryService qs = cache.getQueryService();

    Query[] q = new Query[4];
    SelectResults[][] results = new SelectResults[2][2];
    String predicate = "";
    String predicate2 = "";
    if(useBindParam) {
      predicate = "$1";
      predicate2 = "$1";
    } else {
      predicate = " '%nactive'";
      predicate2 = " 'inactive'";
    }
    
    boolean exceptionThrown = false;
    q[0] = qs.newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status like "+ predicate);
    q[1] = qs.newQuery("SELECT distinct *  FROM /pos ps WHERE ps.status = "+ predicate2);
    q[2] = qs.newQuery("SELECT distinct *  FROM /pos ps WHERE NOT (ps.status like "+ predicate +")");
    q[3] = qs.newQuery("SELECT distinct *  FROM /pos ps WHERE NOT (ps.status = "+ predicate2 + ")");
    try {
      if (useBindParam) {
        results[0][0] = (SelectResults) q[0].execute(new Object[] { "%nactive" });
        results[0][1] =  (SelectResults) q[1].execute(new Object[] { "inactive" });
        results[1][0] = (SelectResults) q[2].execute(new Object[] { "%nactive" });
        results[1][1] =  (SelectResults) q[3].execute(new Object[] { "inactive" });
      } else {
        results[0][0] = (SelectResults) q[0].execute();
        results[0][1] =  (SelectResults) q[1].execute();
	results[1][0] = (SelectResults) q[2].execute();
        results[1][1] =  (SelectResults) q[3].execute();
      }
    } catch(Exception e) {
      exceptionThrown = true;
      fail("Query execution failed " + e);
    }
    assertTrue(results[0][0].size() > 0);
    assertTrue(CacheUtils.compareResultsOfWithAndWithoutIndex(results));
    assertFalse("Query exception should not have been thrown", exceptionThrown);
    
    try {
      qs.createIndex("IDindex", "ps.ID", "/pos ps");
    } catch (Exception e) {
      fail("Index creation failed");
    } 
    
    exceptionThrown = false;
    try {
      if (useBindParam) {
        results[0][0] = (SelectResults) q[0].execute(new Object[] { "%nactive" });
        results[0][1] =  (SelectResults) q[1].execute(new Object[] { "inactive" });
        results[1][0] = (SelectResults) q[2].execute(new Object[] { "%nactive" });
        results[1][1] =  (SelectResults) q[3].execute(new Object[] { "inactive" });
      } else {
        results[0][0] = (SelectResults) q[0].execute();
        results[0][1] =  (SelectResults) q[1].execute();
        results[1][0] = (SelectResults) q[2].execute();
        results[1][1] =  (SelectResults) q[3].execute();
      }
    } catch(Exception e) {
      exceptionThrown = true;
      fail("Query execution failed " + e);
    }
    assertTrue(results[0][0].size() > 0);
    assertTrue(CacheUtils.compareResultsOfWithAndWithoutIndex(results));
    assertFalse("Query exception should not have been thrown", exceptionThrown);
  }

  @Test
  public void testUndefined() throws Exception {
    String[] queries = {
        "select * from /pos where status.toUpperCase() like '%ACT%'",
        "select * from /pos where status.toUpperCase() like 'ACT%'",
        "select * from /pos where status.toUpperCase() like '%IVE'",
        "select * from /pos where status.toUpperCase() like 'ACT_VE'",
        "select * from /pos where status like '%CT%'",
        "select * from /pos where status like 'ACT%'",
        "select * from /pos where status like 'ACT_VE'",
        "select * from /pos where position1.secId like '%B%'",
        "select * from /pos where position1.secId like 'IB%'",
        "select * from /pos where position1.secId like 'I_M'",
        "select * from /pos p, p.positions.values pos where pos.secId like '%B%'",
        "select * from /pos p, p.positions.values pos where pos.secId like 'IB%'",
        "select * from /pos p, p.positions.values pos where pos.secId like 'I_M'",
        };
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    RegionAttributes regionAttributes = attributesFactory.create();

    Region region = cache.createRegion("pos", regionAttributes);
    
    for(int i = 0 ; i < 10; i++) {
      Portfolio p = new Portfolio(i);
      if(i % 2 == 0) {
        p.status = null;
        p.position1 = null;
        p.positions = null;
      } else {
        p.position1 = new Position("IBM", 0);
        p.status = "ACTIVE";
      }
      region.put("key-"+i, p);
    }
    
    SelectResults[][] res = new SelectResults[queries.length][2];
    
    QueryService qs = CacheUtils.getCache().getQueryService();
    SelectResults results = null;

    for (int i = 0; i < queries.length; i++) {
      Query q = qs.newQuery(queries[i]);
      try {
          results = (SelectResults) q.execute();
          res[i][0] = results;
      } catch (Exception e) {
        fail("Query execution failed for query " + queries[i] + " with exception: " + e);
      }
      if(i < 10) {
        assertEquals("Query " + queries[i] + " should return 5 results and not " + results.size(), 5, results.size());
      } else {
        assertEquals("Query " + queries[i] + " should return 1 results and not " + results.size(), 1, results.size());
      }
    }
    
    try {
      qs.createIndex("status", "status", region.getFullPath());
      qs.createIndex("position1secId", "pos1.secId", region.getFullPath());
      qs.createIndex("secId", "pos.secId", region.getFullPath() + " pos, pos.secId");
    } catch (Exception e) {
      fail("Index creation failed");
    }
    
    for (int i = 0; i < queries.length; i++) {
      Query q = qs.newQuery(queries[i]);
      try {
          results = (SelectResults) q.execute();
          res[i][1] = results;
      } catch (Exception e) {
        fail("Query execution failed for query " + queries[i] + " with exception: " + e);
      }
      if(i < 10) {
        assertEquals("Query " + queries[i] + " should return 5 results and not " + results.size(), 5, results.size());
      } else {
        assertEquals("Query " + queries[i] + " should return 1 results and not " + results.size(), 1, results.size());
      }
    }
     
    CacheUtils.compareResultsOfWithAndWithoutIndex(res);
  }

  private static class LikeQueryIndexTestHook implements TestHook {
    public static Thread th;

    @Override
    public void hook(int spot) throws RuntimeException {
      if (spot == 12) {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            Cache cache = CacheUtils.getCache();
            cache.getLogger().fine("Removing Index in LikeQueryIndexTestHook");
            QueryService qs = cache.getQueryService();
            qs.removeIndex(qs.getIndex(cache.getRegion("exampleRegion"),
                "status"));
          }
        };
        th = new Thread(r);
        th.start();
      }
    }
  }
  private class PortfolioModifiedStatus {
    public PortfolioModifiedStatus(int id) {
      String[] status = {"inactive", "active"};
    }
  }

}
