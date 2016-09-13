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
package org.apache.geode.cache.query.functional;

import org.apache.geode.cache.*;
import org.apache.geode.cache.query.*;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.text.ParseException;
import java.util.Collection;
import java.util.Iterator;

@Category(IntegrationTest.class)
public class IndexOnEntrySetJUnitTest {

  private static String testRegionName = "regionName";
  private static Region testRegion;
  private static int numElem = 100;
  private String newValue = "NEW VALUE";

   @Before
   public void setUp() throws Exception {
     System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

   @After
  public void tearDown() throws Exception {
    // Destroy current Region for other tests    
    IndexManager.testHook = null;
    if (testRegion != null) {
      testRegion.destroyRegion();
    }
    CacheUtils.closeCache();    
  }

  private String[] getQueriesOnRegion(String regionName) {
    return new String[] {"SELECT DISTINCT entry.value, entry.key FROM /" + regionName + ".entrySet entry WHERE entry.key.PartitionID > 0 AND " + "entry.key.Index > 1 ORDER BY entry.key.Index ASC LIMIT 2",
    "SELECT DISTINCT entry.value, entry.key FROM /" + regionName + ".entrySet entry WHERE entry.key.Index > 1 ORDER BY entry.key.Index ASC LIMIT 2",
    "SELECT DISTINCT * FROM /" + regionName + ".entrySet entry WHERE entry.key.PartitionID > 0 AND " + "entry.key.Index > 1 ORDER BY entry.key.Index ASC LIMIT 2",
    "SELECT DISTINCT entry.value, entry.key FROM /" + regionName + ".entrySet entry WHERE entry.key.PartitionID > 0 AND " + "entry.key.Index > 1 LIMIT 2",
    "SELECT DISTINCT entry.value, entry.key FROM /" + regionName + ".entrySet entry WHERE entry.key.PartitionID > 0 AND " + "entry.key.Index > 1 ORDER BY entry.key.Index ASC",};
  }
  
  private String[] getQueriesOnRegionForPut(String regionName) {
    return new String[] {"SELECT DISTINCT entry.value, entry.key FROM /" + regionName + ".entrySet entry WHERE entry.key.PartitionID = 50 AND " + "entry.key.Index > 1 ORDER BY entry.key.Index ASC LIMIT 2",
        "SELECT DISTINCT entry.value, entry.key FROM /" + regionName + ".entrySet entry WHERE entry.value = 50 AND " + "entry.key.Index > 1 ORDER BY entry.key.Index ASC LIMIT 2"};
  }

  /**
   * Test queries with index on replicated regions and concurrent PUT, DESTORY, INVALIDATE operations.
   * Make sure there is no UNDEFINED in the query result.
   */
  @Test
  public void testQueriesOnReplicatedRegion() throws Exception {
    testRegion = createReplicatedRegion(testRegionName);
    String regionPath = "/" + testRegionName + ".entrySet entry";
    executeQueryTest(getQueriesOnRegion(testRegionName), "entry.key.Index", regionPath);
  }

  /**
   * Test queries with index on partitioned regions and concurrent PUT, DESTORY, INVALIDATE operations.
   * Make sure there is no UNDEFINED in the query result.
   */
  @Test
  public void testQueriesOnPartitionedRegion() throws Exception {
    testRegion = createPartitionedRegion(testRegionName);
    String regionPath = "/" + testRegionName + ".entrySet entry";
    executeQueryTest(getQueriesOnRegion(testRegionName), "entry.key.Index", regionPath);
  }

  private Region createReplicatedRegion(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }

  private Region createPartitionedRegion(String regionName) throws ParseException {
    Cache cache = CacheUtils.getCache();
    PartitionAttributesFactory prAttFactory = new PartitionAttributesFactory();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(prAttFactory.create());
    RegionAttributes regionAttributes = attributesFactory.create();
    return cache.createRegion(regionName, regionAttributes);
  }

  private void populateRegion(Region region) throws Exception {
    for (int i = 1; i <= numElem; i++) {
      putData(i, region);
    }
  }

  private void putData(int id, Region region) throws ParseException {
    region.put(new SomeKey(id, id), id);
  }
  
  private void clearData(Region region) {
    Iterator it = region.entrySet().iterator();
    while (it.hasNext()) {
      Region.Entry entry = (Region.Entry) it.next();
      region.destroy(entry.getKey());
    }
  }

  /**** Query Execution Helpers ****/

  private void executeQueryTest(String[] queries, String indexedExpression, String regionPath) throws Exception {
    Cache cache = CacheUtils.getCache();
    boolean[] booleanVals = {true, false};
    for (String query : queries) {
      for (boolean isDestroy : booleanVals) {
        clearData(testRegion);
        populateRegion(testRegion);
        Assert.assertNotNull(cache.getRegion(testRegionName));
        Assert.assertEquals(numElem, cache.getRegion(testRegionName).size());
        if (isDestroy) {
          helpTestFunctionalIndexForQuery(query, indexedExpression, regionPath, new DestroyEntryTestHook(testRegion));
        }
        else {
          helpTestFunctionalIndexForQuery(query, indexedExpression, regionPath, new InvalidateEntryTestHook(testRegion));
        }
      }
    }
    
    queries = getQueriesOnRegionForPut(testRegionName);
    for (String query : queries) {
      clearData(testRegion);
      populateRegion(testRegion);
      Assert.assertNotNull(cache.getRegion(testRegionName));
      Assert.assertEquals(numElem, cache.getRegion(testRegionName).size());
      helpTestFunctionalIndexForQuery(query, indexedExpression, regionPath, new PutEntryTestHook(testRegion));
    }
  }

  /**
   *  helper method to test against a functional index
   *  make sure there is no UNDEFINED result
   * 
   * @param query
   * 
   * @throws Exception */
  private SelectResults helpTestFunctionalIndexForQuery(String query, String indexedExpression, String regionPath, AbstractTestHook testHook) throws Exception {
    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserverHolder.setInstance(observer);
    IndexManager.testHook = testHook;
    QueryService qs = CacheUtils.getQueryService();
    Index index = qs.createIndex("testIndex", indexedExpression, regionPath);
    SelectResults indexedResults = (SelectResults) qs.newQuery(query).execute();
    Iterator iterator = indexedResults.iterator();
    while (iterator.hasNext()) {
      Object row = iterator.next();
      if (row instanceof Struct) {
        Object[] fields = ((Struct) row).getFieldValues();
        for (Object field: fields) {
          Assert.assertTrue(field != QueryService.UNDEFINED);
          if (field instanceof String) {
            Assert.assertTrue(((String) field).compareTo(newValue) != 0);
          }
        }
      }
      else {
        Assert.assertTrue(row != QueryService.UNDEFINED);
        if (row instanceof String) {
          Assert.assertTrue(((String) row).compareTo(newValue) != 0);
        }
      }
    }
    Assert.assertTrue(indexedResults.size() > 0);
    Assert.assertTrue(observer.indexUsed);
    Assert.assertTrue(((AbstractTestHook) IndexManager.testHook).isTestHookCalled());
    ((AbstractTestHook) IndexManager.testHook).reset();
    qs.removeIndex(index);
   
    return indexedResults;
  }

  class MyQueryObserverAdapter extends QueryObserverAdapter {
    public boolean indexUsed = false;

    public void afterIndexLookup(Collection results) {
      super.afterIndexLookup(results);
      indexUsed = true;
    }
  }

  class SomeKey {
    public int Index = 1;
    public int PartitionID = 1;

    public SomeKey(int index, int partitionId) {
      this.Index = index;
      this.PartitionID = partitionId;
    }

    public boolean equals(Object other) {
      if (other instanceof SomeKey) {
        SomeKey otherKey = (SomeKey) other;
        return this.Index == otherKey.Index && this.PartitionID == otherKey.PartitionID;
      }
      return false;
    }

    public String toString() {
      return "somekey:" + Index + "," + PartitionID;

    }
  }

  /**
   * Test hook
   */
  abstract class AbstractTestHook implements IndexManager.TestHook {
    boolean isTestHookCalled = false;
    Object waitObj = new Object();
    Region r;

    public void reset() {
      isTestHookCalled = false;
    }
    
    public boolean isTestHookCalled() {
      return isTestHookCalled;
    }
    
    /**
     * Subclass override with different operation
     */
    public abstract void doOp();
    
    @Override
    public void hook(int spot) throws RuntimeException {
      if (spot == 200) {
        if (!isTestHookCalled) {
          isTestHookCalled = true;
          try {
            new Thread(new Runnable() {
              public void run() {
                doOp();
                synchronized (waitObj) {
                  waitObj.notifyAll();
                }
              }
            }).start();
            synchronized (waitObj) {
              waitObj.wait();
            }
          } catch (InterruptedException e) {
            throw new Error(e);
          }
        }
      }

    }

  }
  
  class DestroyEntryTestHook extends AbstractTestHook {
    
    DestroyEntryTestHook(Region r) {
      this.r = r;
    }
    
    public void doOp() {
      Iterator it = r.entrySet().iterator();
      while (it.hasNext()) {
        Region.Entry entry = (Region.Entry) it.next();
          r.destroy(entry.getKey());
        }      
    }
  }
  
  class InvalidateEntryTestHook extends AbstractTestHook {
    
    InvalidateEntryTestHook(Region r) {
      this.r = r;
    }
    
    public void doOp() {
      Iterator it = r.entrySet().iterator();
      while (it.hasNext()) {
        Region.Entry entry = (Region.Entry) it.next();
          r.invalidate(entry.getKey());
      }      
    }
  }
  
  class PutEntryTestHook extends AbstractTestHook {
    
    PutEntryTestHook(Region r) {
      this.r = r;
    }
    
    public void doOp() {
      Iterator it = r.entrySet().iterator();
      while (it.hasNext()) {
        Region.Entry entry = (Region.Entry) it.next();
        r.put(entry.getKey(), newValue);
      }      
    }
  }
}
