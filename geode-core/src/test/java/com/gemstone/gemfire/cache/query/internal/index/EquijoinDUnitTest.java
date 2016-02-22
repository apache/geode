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
package com.gemstone.gemfire.cache.query.internal.index;

import java.io.Serializable;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.functional.StructSetOrResultsSet;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class EquijoinDUnitTest extends TestCase {
  QueryService qs;
  Region region1, region2, region3, region4;
  
  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    qs = CacheUtils.getQueryService();
  }
  
  @After
  public void tearDown() {
    region2.destroyRegion();
    region1.destroyRegion();
  }
  
  protected void createRegions() throws Exception {
    region1 = createReplicatedRegion("region1");
    region2 = createReplicatedRegion("region2");
  }
  
  protected void createAdditionalRegions() throws Exception {
    region3 = createReplicatedRegion("region3");
    region4 = createReplicatedRegion("region4");
  }
  
  protected void destroyAdditionalRegions() throws Exception {
    if (region3 != null) {
      region3.destroyRegion();
    }
    if (region4 != null) {
      region4.destroyRegion();
    }
  }

  @Test
  public void testSingleFilterWithSingleEquijoinOneToOneMapping() throws Exception {
    createRegions();

    String[] queries = new String[]{
        "<trace>select * from /region1 c, /region2 s where c.pkid=1 and c.pkid = s.pkid",
        "<trace>select * from /region1 c, /region2 s where c.pkid=1 and s.pkid = c.pkid",
        "<trace>select * from /region1 c, /region2 s where c.pkid = s.pkid and c.pkid=1",
        "<trace>select * from /region1 c, /region2 s where s.pkid = c.pkid and c.pkid=1",
    };
    
    for (int i = 0; i < 1000; i++) {
      region1.put( i, new Customer(i, i));
      region2.put( i, new Customer(i, i));
    }
    
    executeQueriesWithIndexCombinations(queries);
  }
  
  @Test
  public void testSingleFilterWithSingleEquijoinOneToOneMappingWithAdditionalJoins() throws Exception {
    createRegions();
    try {
      createAdditionalRegions();
      
      String[] queries = new String[]{
          "<trace>select * from /region1 c, /region2 s, /region3 d where c.pkid=1 and c.pkid = s.pkid and d.pkid = s.pkid",  //this should derive d after deriving s from c
          "<trace>select * from /region1 c, /region2 s, /region3 d, /region4 f where c.pkid=1 and c.pkid = s.pkid and d.pkid = s.pkid and f.pkid = d.pkid",  //this should f from d from s from c
          "<trace>select * from /region1 c, /region2 s, /region3 d where c.pkid=1 and c.pkid = s.pkid and d.pkid = c.pkid",  //this should derive d and s from c 
          "<trace>select * from /region1 c, /region2 s, /region3 d where c.pkid=1 and c.pkid = s.pkid and s.pkid = d.pkid",  //this should derive d after deriving s from c (order is just switched in the query)
      };
      
      for (int i = 0; i < 30; i++) {
        region1.put( i, new Customer(i, i));
        region2.put( i, new Customer(i, i));
        region3.put( i, new Customer(i, i));
        region4.put( i, new Customer(i, i));
      }
      
      executeQueriesWithIndexCombinations(queries);
    }
    finally {
      destroyAdditionalRegions();
    }
  }

  
  /**
   * We do not want to test this with Primary Key on the many side or else only 1 result will be returned
   */
  @Test
  public void testSingleFilterWithSingleEquijoinOneToManyMapping() throws Exception {
    createRegions();

    String[] queries = new String[]{
        "select * from /region1 c, /region2 s where c.pkid=1 and c.pkid = s.pkid",
        "select * from /region1 c, /region2 s where c.pkid=1 and s.pkid = c.pkid",
        "select * from /region1 c, /region2 s where c.pkid = s.pkid and c.pkid=1",
        "select * from /region1 c, /region2 s where s.pkid = c.pkid and c.pkid=1",
    };
    
    for (int i = 0; i < 1000; i++) {
      region1.put( i, new Customer(i, i));
      region2.put( i, new Customer(i % 100, i));
    }
    
    executeQueriesWithIndexCombinations(queries, new DefaultIndexCreatorCallback(qs) {
      protected String[] createIndexTypesForRegion2() {
        return new String[] { "Compact", "Hash"};
      }
    }, false);
  }

  @Test
  public void testSingleFilterWithSingleEquijoinMultipleFiltersOnSameRegionOnSameIteratorMapping() throws Exception {
    createRegions();

    String[] queries = new String[]{
        "select * from /region1 c, /region2 s where c.pkid=1 and c.pkid = s.pkid and c.id = 1",
        "select * from /region1 c, /region2 s where c.id = 1 and c.pkid=1 and s.pkid = c.pkid",
        
    };
    
    for (int i = 0; i < 1000; i++) {
      region1.put( i, new Customer(i, i % 10));
      region2.put( i, new Customer(i, i));
    }
    
    executeQueriesWithIndexCombinations(queries, new DefaultIndexCreatorCallback(qs) {
      Index secondaryIndex;
      
      @Override
      public void createIndexForRegion1(int indexTypeId) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
        secondaryIndex = qs.createIndex("region1 id", "p.id", "/region1 p");
        super.createIndexForRegion1(indexTypeId);
      }

      @Override
      public void destroyIndexForRegion1(int indexTypeId) {
        qs.removeIndex(secondaryIndex);
        super.destroyIndexForRegion1(indexTypeId);
      }
      
    }, false /*want to compare actual results and not size only*/);
  }

  @Test  
  public void testSingleFilterWithSingleEquijoinWithRangeFilters() throws Exception {
    createRegions();

    String[] queries = new String[]{
        "<trace>select * from /region1 c, /region2 s where c.pkid = 1 and c.id > 1 and c.id < 10 and c.pkid = s.pkid",
        "<trace>select * from /region1 c, /region2 s where c.pkid >= 0 and c.pkid < 10 and c.id < 10 and c.pkid = s.pkid"
    };
    
    //just need enough so that there are 1-10 ids per pkid
    for (int i = 0; i < 1000; i++) {
      region1.put(i, new Customer(i % 5, i % 10));
      region2.put(i, new Customer(i, i));
    }
    
    executeQueriesWithIndexCombinations(queries, new DefaultIndexCreatorCallback(qs) {
      protected String[] createIndexTypesForRegion1() {
        return new String[] { "Compact", "Hash"};
      }
    }, false /*want to compare actual results and not size only*/);
  }

  @Test 
  public void testSingleFilterWithSingleEquijoinLimit() throws Exception {
    //In this test we are hoping the index being used will properly use the limit while taking into consideration the filters of c.id and c.pkid
    //This test is set up so that if the pkid index is used and limit applied, if id is not taken into consideration until later stages, it will lead to incorrect results (0)
    createRegions();

    String[] queries = new String[]{
        "select * from /region1 c, /region2 s where c.id = 3 and c.pkid > 2  and c.pkid = s.pkid limit 1",
    };
    
    for (int i = 0; i < 1000; i++) {
      region1.put( i, new Customer(i, i % 10));
      region2.put( i, new Customer(i, i));
    }
    
    executeQueriesWithIndexCombinations(queries, new DefaultIndexCreatorCallback(qs) {
      Index secondaryIndex;
      
      @Override
      public void createIndexForRegion1(int indexTypeId) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
        secondaryIndex = qs.createIndex("region1 id", "p.id", "/region1 p");
        super.createIndexForRegion1(indexTypeId);
      }

      @Override
      public void destroyIndexForRegion1(int indexTypeId) {
        qs.removeIndex(secondaryIndex);
        super.destroyIndexForRegion1(indexTypeId);
      }
      
    }, true);
  }

  @Test
  public void testSingleFilterWithSingleEquijoinNestedQuery() throws Exception {
    createRegions();

    String[] queries = new String[]{
        "select * from /region1 c, /region2 s where c.pkid=1 and c.pkid = s.pkid and c.pkid in (select t.pkid from /region1 t,/region2 s where s.pkid=t.pkid and s.pkid = 1)",
        "select * from /region1 c, /region2 s where c.pkid=1 and c.pkid = s.pkid or c.pkid in set (1,2,3,4)",
    };
    
    for (int i = 0; i < 1000; i++) {
      region1.put( i, new Customer(i, i));
      region2.put( i, new Customer(i, i));
    }
    
    executeQueriesWithIndexCombinations(queries);
  }

  public static class Customer implements Serializable {
    public int pkid;
    public int id;
    public String name;
    public Map<String, Customer> nested = new HashMap<String, Customer>();

    public Customer(int pkid, int id) {
      this.pkid = pkid;
      this.id = id;
      this.name = "name" + pkid;
    }

    public String toString() {
      return "Customer pkid = " + pkid + ", id: " + id + " name:" + name;
    }
  }

  private Region createReplicatedRegion(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }

  protected void executeQueriesWithIndexCombinations(String[] queries) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException, QueryInvocationTargetException, NameResolutionException, TypeMismatchException, FunctionDomainException {
    executeQueriesWithIndexCombinations(queries, new DefaultIndexCreatorCallback(qs), false);
  }
  
  protected void executeQueriesWithIndexCombinations(String[] queries, IndexCreatorCallback indexCreator, boolean sizeOnly) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException, QueryInvocationTargetException, NameResolutionException, TypeMismatchException, FunctionDomainException {
    Object[] nonIndexedResults = executeQueries(queries);
    
    for (int r1Index = 0; r1Index < indexCreator.getNumIndexTypesForRegion1(); r1Index++) {
      indexCreator.createIndexForRegion1(r1Index);
      for (int r2Index = 0; r2Index < indexCreator.getNumIndexTypesForRegion2(); r2Index++) {
        indexCreator.createIndexForRegion2(r2Index);
        Object[] indexedResults = executeQueries(queries);
        compareResults(nonIndexedResults, indexedResults, queries, sizeOnly);
        indexCreator.destroyIndexForRegion2(r2Index);
      }
      indexCreator.destroyIndexForRegion1(r1Index);
    }
  }
  
  protected Object[] executeQueries(String[] queries) throws QueryInvocationTargetException, NameResolutionException, TypeMismatchException, FunctionDomainException {
    Object[] results = new SelectResults[queries.length];
    for (int i = 0; i < queries.length; i++) {
      results[i] = qs.newQuery(queries[i]).execute();
    }
    return results;
  }
  
  interface IndexCreatorCallback {
    int getNumIndexTypesForRegion1();
    int getNumIndexTypesForRegion2();
    void createIndexForRegion1(int indexTypeId) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException;
    void createIndexForRegion2(int indexTypeId) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException;
    void destroyIndexForRegion1(int indexTypeId) ;
    void destroyIndexForRegion2(int indexTypeId) ;
  }
  
  static class DefaultIndexCreatorCallback implements IndexCreatorCallback {
    protected String[] indexTypesForRegion1 = createIndexTypesForRegion1();
    protected String[] indexTypesForRegion2 = createIndexTypesForRegion2();
    protected Index indexOnR1, indexOnR2;
    protected QueryService qs;
    
    DefaultIndexCreatorCallback(QueryService qs) {
      this.qs = qs;
    }
    protected String[] createIndexTypesForRegion1() {
      return new String[] { "Compact", "Hash", "PrimaryKey"};
    }
    
    protected String[] createIndexTypesForRegion2() {
      return new String[] { "Compact", "Hash", "PrimaryKey"};
    }
    
    public int getNumIndexTypesForRegion1() {
      return indexTypesForRegion1.length; 
    }
    
    public int getNumIndexTypesForRegion2() {
      return indexTypesForRegion2.length;
    }
    
    public void createIndexForRegion1(int indexTypeId) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
      indexOnR1 = createIndex(indexTypesForRegion1[indexTypeId], "region1", "pkid");

    }
    
    public void createIndexForRegion2(int indexTypeId) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
      indexOnR2 = createIndex(indexTypesForRegion2[indexTypeId], "region2", "pkid");
    }

    //Type id is not used here but at some future time we could store a map of indexes or find a use for this id?
    public void destroyIndexForRegion1(int indexTypeId) {
      qs.removeIndex(indexOnR1);
    }
    
    public void destroyIndexForRegion2(int indexTypeId) {
      qs.removeIndex(indexOnR2);
    }
    
    
    private Index createIndex(String type, String regionName, String field) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
      Index index = null;
      switch (type) {
      case "Compact":
        index = createCompactRangeIndex(regionName, field);
        break;
      case "Range":
        index = createRangeIndexOnFirstIterator(regionName, field);
        break;
      case "Hash":
        index = createHashIndex(regionName, field);
        break;
      case "PrimaryKey":
        index = createPrimaryKeyIndex(regionName, field);
        break;
      }
      return index;
    }
    
    private Index createCompactRangeIndex(String regionName, String fieldName) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
      String fromClause = "/" + regionName + " r";
      String indexedExpression = "r." + fieldName;
      return qs.createIndex("Compact " + fromClause + ":" + indexedExpression, indexedExpression, fromClause);
    }
    
    private Index createHashIndex(String regionName, String fieldName) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
      String fromClause = "/" + regionName + " r";
      String indexedExpression = "r." + fieldName;
      return qs.createHashIndex("Hash " + fromClause + ":" + indexedExpression, indexedExpression, fromClause);
    }
    
    private Index createPrimaryKeyIndex(String regionName, String fieldName) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
      String fromClause = "/" + regionName + " r";
      String indexedExpression = "r." + fieldName;
      return qs.createKeyIndex("PrimaryKey " + fromClause + ":" + indexedExpression, indexedExpression, fromClause);
    }
    
    private Index createRangeIndexOnFirstIterator(String regionName, String fieldName) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
      String fromClause = "/" + regionName + " r, r.nested.values v";
      String indexedExpression = "r." + fieldName;
      return qs.createIndex("Range " + fromClause + ":" + indexedExpression, indexedExpression, fromClause);
    }
    
    private Index createRangeIndexOnSecondIterator(String regionName, String fieldName) throws RegionNotFoundException, IndexExistsException, IndexNameConflictException {
      String fromClause = "/" + regionName + " r, r.nested.values v";
      String indexedExpression = "v." + fieldName;
      return qs.createIndex("Range " + fromClause + ":" + indexedExpression, indexedExpression, fromClause);
    }
  }
  
  private void compareResults(Object[] nonIndexedResults, Object[] indexedResults, String[] queries, boolean sizeOnly) {
    if (sizeOnly) {
      for (int i = 0; i < queries.length; i++) {
        assertTrue(((SelectResults)nonIndexedResults[i]).size() == ((SelectResults)indexedResults[i]).size());
        assertTrue(((SelectResults)nonIndexedResults[i]).size() > 0);
      }
    }
    else {
      StructSetOrResultsSet util = new StructSetOrResultsSet();
      for (int i = 0; i < queries.length; i++) {
        Object[][] resultsToCompare = new Object[1][2];
        resultsToCompare[0][0] = nonIndexedResults[i];
        resultsToCompare[0][1] = indexedResults[i];
        util.CompareQueryResultsWithoutAndWithIndexes(resultsToCompare, 1, new String[]{queries[i]});
        assertTrue(((SelectResults)nonIndexedResults[i]).size() > 0);
      }
    }
  }
}
