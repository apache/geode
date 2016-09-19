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
//
//  IndexTrackingQueryObserver.java
//  gemfire
//
package org.apache.geode.cache.query.internal;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.internal.index.CompactMapRangeIndex;
import org.apache.geode.cache.query.internal.index.MapRangeIndex;
import org.apache.geode.internal.cache.PartitionedRegionQueryEvaluator.TestHook;

/**
 * Verbose Index information
 *
 * @see DefaultQuery
 */
public class IndexTrackingQueryObserver extends QueryObserverAdapter {
  
  private static final ThreadLocal indexInfo = new ThreadLocal();
  private static final ThreadLocal lastIndexUsed = new ThreadLocal();
  private volatile TestHook th;
  
  public void beforeIndexLookup(Index index, int oper, Object key) {
    Map<String, IndexInfo> indexMap = (Map)this.indexInfo.get();
    if (indexMap == null) {
      indexMap = new HashMap<String, IndexInfo>();
      this.indexInfo.set(indexMap);
    }
    IndexInfo iInfo;
    String indexName;
    //Dont create new IndexInfo if one is already there in map for aggregation
    //of results later for whole partition region on this node.
    if((index instanceof MapRangeIndex || index instanceof CompactMapRangeIndex) && key instanceof Object[]){
      indexName = index.getName()+ "-"+((Object[])key)[1];
    } else {
      indexName = index.getName();
    }
    if(indexMap.containsKey(indexName)){
      iInfo = indexMap.get(indexName);
    } else {
      iInfo = new IndexInfo();
    }
    iInfo.addRegionId(index.getRegion().getFullPath());
    indexMap.put(indexName, iInfo);
    this.lastIndexUsed.set(index);
    if(th != null){
      th.hook(1);
    }
  }
  
  public void beforeIndexLookup(Index index, int lowerBoundOperator,
                                Object lowerBoundKey, int upperBoundOperator, Object upperBoundKey,
                                Set NotEqualKeys) {
    Map<String, IndexInfo> indexMap = (Map)this.indexInfo.get();
    if (indexMap == null) {
      indexMap = new HashMap<String, IndexInfo>();
      this.indexInfo.set(indexMap);
    }
    IndexInfo iInfo;
    //Dont create new IndexInfo if one is already there in map for aggregation
    //of results later for whole partition region on this node.
    if(indexMap.containsKey(index.getName())){
      iInfo = indexMap.get(index.getName());
    } else {
      iInfo = new IndexInfo();
    }
    iInfo.addRegionId(index.getRegion().getFullPath());
    indexMap.put(index.getName(), iInfo);
    this.lastIndexUsed.set(index);
    if(th != null){
      th.hook(2);
    }
  }
  
  /**
   * appends the size of the lookup to the last index name in the list
   */
  public void afterIndexLookup(Collection results) {
    if (results == null) {
      // according to javadocs in QueryObserver, can be null if there
      // is an exception
      return;
    }
    
    // append the size of the lookup results (and bucket id if its an Index on bucket)
    // to IndexInfo results Map.
    Map indexMap = (Map)this.indexInfo.get();
    if (lastIndexUsed.get() != null) {
      IndexInfo indexInfo = (IndexInfo) indexMap
          .get(((Index) this.lastIndexUsed.get()).getName());
      if (indexInfo != null) {
      indexInfo.getResults().put(
          ((Index)this.lastIndexUsed.get()).getRegion().getFullPath(),
          new Integer(results.size()));
      }
    }
    this.lastIndexUsed.set(null);
    if(th != null){
      th.hook(3);
    }
  }    
  
  /**
   * This should be called only when one query execution on one gemfire node is done.
   * NOT for each buckets.
   */
  public void reset() {
    if(th != null){
      th.hook(4);
    }
    this.indexInfo.set(null);
  }

  public void setIndexInfo(Map indexInfoMap) {
    indexInfo.set(indexInfoMap);
  }

  public Map getUsedIndexes() {
    Map map = (Map)this.indexInfo.get();
    if (map == null) {
      return Collections.EMPTY_MAP;
    }
    return map;
  }

  public void setTestHook(TestHook testHook) {
    th = testHook;
  }
  /**
   * This class contains information related to buckets and results found in
   * the index on those buckets.
   *
   */
  public class IndexInfo{
    // A {RegionFullPath, results} map for an Index lookup on a Region.
    private Map<String, Integer> results = new Object2ObjectOpenHashMap();
    
    public Map getResults() {
      return results;
    }
    /**
     * Adds a results map (mostly a bucket index lookup results)
     * to the "this" IndexInfo.
     * @param rslts
     */
    public void addResults(Map rslts) {
      for(Object obj : rslts.entrySet()){
        Entry<String, Integer> ent = (Entry)obj;
        this.results.put(ent.getKey(), ent.getValue());
      }
    }
    public Set getRegionIds() {
      return results.keySet();
    }
    public void addRegionId(String regionId) {
      this.results.put(regionId, 0);
    }
    
    @Override
    public String toString() {
      int total =0;
      for (Integer i: results.values()){
        total+=i.intValue();
      }
      return "(Results: "+ total +")";
    }
    public void merge(IndexInfo src) {
      this.addResults(src.getResults());
    }
  }

  public Map getUsedIndexes(String fullPath) {
    Map map = (Map)this.indexInfo.get();
    if (map == null) {
      return Collections.EMPTY_MAP;
    }
    Map newMap = new HashMap();
    for(Object obj: (Set)map.entrySet()){
      Map.Entry<String, IndexInfo> entry = (Map.Entry<String, IndexInfo>)obj;
      if(entry != null && entry.getValue().getRegionIds().contains(fullPath)){
        newMap.put(entry.getKey(), entry.getValue().getResults().get(fullPath));
      }
    }
    return newMap;
  }

  public TestHook getTestHook() {
    return th;
  }
}
