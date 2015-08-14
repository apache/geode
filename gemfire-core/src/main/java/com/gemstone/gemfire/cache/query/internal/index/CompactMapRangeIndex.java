/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.index.AbstractIndex.InternalIndexStatistics;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;

public class CompactMapRangeIndex extends AbstractMapIndex
{
  private final Map<Object, Map> entryToMapKeyIndexKeyMap;
  private IndexCreationHelper ich;

  CompactMapRangeIndex(String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes,
      String origFromClause, String origIndxExpr, String[] defintions,
      boolean isAllKeys, String[] multiIndexingKeysPattern, Object[] mapKeys, IndexStatistics stats) {
    super(indexName, region, fromClause, indexedExpression,
        projectionAttributes, origFromClause, origIndxExpr, defintions, 
        isAllKeys, multiIndexingKeysPattern, mapKeys, stats);
    RegionAttributes ra = region.getAttributes();
    this.entryToMapKeyIndexKeyMap = new java.util.concurrent.ConcurrentHashMap(ra.getInitialCapacity(),ra.getLoadFactor(), ra.getConcurrencyLevel());
  }

  @Override
  void instantiateEvaluator(IndexCreationHelper ich)
  {
    this.evaluator = new IMQEvaluator(ich);
    this.ich = ich;
  }

  @Override
  public boolean containsEntry(RegionEntry entry)
  {
    return this.entryToMapKeyIndexKeyMap.containsKey(entry);
  }
  
  @Override
  void recreateIndexData() throws IMQException
  {
    this.entryToMapKeyIndexKeyMap.clear();
    this.mapKeyToValueIndex.clear();
    this.initializeIndex(true);
  }

  protected void removeMapping(RegionEntry entry, int opCode) throws IMQException {
    // this implementation has a reverse map, so it doesn't handle
    // BEFORE_UPDATE_OP
    if (opCode == BEFORE_UPDATE_OP) {
      return;
    }

    //Object values = this.entryToMapKeysMap.remove(entry);
    Map mapKeyToIndexKey = this.entryToMapKeyIndexKeyMap.remove(entry);
    //Values in reverse coould be null if map in region value does not
    //contain any key which matches to index expression keys.
    if (mapKeyToIndexKey == null ) {
      return;
    }
    
    Iterator<Map.Entry<?, ?>> mapKeyIterator = mapKeyToIndexKey.entrySet().iterator();
    while (mapKeyIterator.hasNext()) {
      Map.Entry<?, ?> mapEntry = mapKeyIterator.next();
      Object mapKey = mapEntry.getKey();
      Object indexKey = mapEntry.getValue();
      CompactRangeIndex ri = (CompactRangeIndex)this.mapKeyToValueIndex.get(mapKey);
      long start = System.nanoTime();
      this.internalIndexStats.incUpdatesInProgress(1);
      ri.removeMapping(indexKey, entry);
      this.internalIndexStats.incUpdatesInProgress(-1);
      long end = System.nanoTime() - start;
      this.internalIndexStats.incUpdateTime(end);
      this.internalIndexStats.incNumUpdates();
    }
  }
  
  @Override
  void saveMapping(Object key, Object value, RegionEntry entry)
      throws IMQException
  {
    if(key == QueryService.UNDEFINED || !(key instanceof Map)) {
      return;
    }
    if (this.isAllKeys) {
      Iterator<Map.Entry<?, ?>> entries = ((Map)key).entrySet().iterator();
      while (entries.hasNext()) {
        Map.Entry<?, ?> mapEntry = entries.next();
        Object mapKey = mapEntry.getKey();
        Object indexKey = mapEntry.getValue();
        this.saveIndexAddition(mapKey, indexKey, value, entry);
      }
    }
    else {
      for (Object mapKey : mapKeys) {
        Object indexKey = ((Map)key).get(mapKey);
        if (indexKey != null) {
          //Do not convert to IndexManager.NULL.  We are only interested in specific keys
          this.saveIndexAddition(mapKey, indexKey, value, entry);
        }
      }
    }
  }
  
  protected void doIndexAddition(Object mapKey, Object indexKey, Object value,
      RegionEntry entry) throws IMQException
  {
    boolean isPr = this.region instanceof BucketRegion;
    // Get RangeIndex for it or create it if absent
    CompactRangeIndex rg = (CompactRangeIndex)this.mapKeyToValueIndex.get(mapKey);
    if (rg == null) {
      // use previously created MapRangeIndexStatistics
      IndexStatistics stats = this.internalIndexStats;
      PartitionedIndex prIndex = null;
      if (isPr) {
        prIndex = (PartitionedIndex) this.getPRIndex();
        prIndex.incNumMapKeysStats(mapKey);
      }
      rg = new CompactRangeIndex(indexName+"-"+mapKey, region, fromClause, indexedExpression,
          projectionAttributes, this.originalFromClause,
          this.originalIndexedExpression, this.canonicalizedDefinitions, stats);
      rg.instantiateEvaluator(this.ich, ((AbstractIndex.IMQEvaluator)this.evaluator).getIndexResultSetType());
      this.mapKeyToValueIndex.put(mapKey, rg);
      if(!isPr) {
        this.internalIndexStats.incNumMapIndexKeys(1);
      }
    }
    long start = System.nanoTime();
    rg.addMapping(indexKey, value, entry);
    //This call is skipped when addMapping is called from MapRangeIndex
    //rg.internalIndexStats.incNumUpdates();
    this.internalIndexStats.incUpdatesInProgress(-1);
    long end = System.nanoTime() - start;
    this.internalIndexStats.incUpdateTime(end);
    this.internalIndexStats.incNumUpdates();
  //add to mapkey to indexkey map
    Map mapKeyToIndexKey = this.entryToMapKeyIndexKeyMap.get(entry);
    if (mapKeyToIndexKey == null) {
      mapKeyToIndexKey = new HashMap();
      entryToMapKeyIndexKeyMap.put(entry, mapKeyToIndexKey);
    }
    mapKeyToIndexKey.put(mapKey, indexKey);
  }

  protected void saveIndexAddition(Object mapKey, Object indexKey, Object value,
      RegionEntry entry) throws IMQException
  {
    if (indexKey == null) {
      indexKey = IndexManager.NULL;
    }
    boolean isPr = this.region instanceof BucketRegion;
    // Get RangeIndex for it or create it if absent
    CompactRangeIndex rg = (CompactRangeIndex) this.mapKeyToValueIndex.get(mapKey);
    if (rg == null) {
      // use previously created MapRangeIndexStatistics
      IndexStatistics stats = this.internalIndexStats;
      PartitionedIndex prIndex = null;
      if (isPr) {
        prIndex = (PartitionedIndex) this.getPRIndex();
        prIndex.incNumMapKeysStats(mapKey);
      }
      rg = new CompactRangeIndex(indexName+"-"+mapKey, region, fromClause, indexedExpression,
          projectionAttributes, this.originalFromClause,
          this.originalIndexedExpression, this.canonicalizedDefinitions, stats);
      rg.instantiateEvaluator(this.ich, ((AbstractIndex.IMQEvaluator)this.evaluator).getIndexResultSetType());
      this.mapKeyToValueIndex.put(mapKey, rg);
      if(!isPr) {
        this.internalIndexStats.incNumMapIndexKeys(1);
      }
    }
    this.internalIndexStats.incUpdatesInProgress(1);
    long start = System.nanoTime();
    
    //add to mapkey to indexkey map
    Map mapKeyToIndexKey = this.entryToMapKeyIndexKeyMap.get(entry);
    if (mapKeyToIndexKey == null) {
      mapKeyToIndexKey = new HashMap();
      entryToMapKeyIndexKeyMap.put(entry, mapKeyToIndexKey);
    }
    //Due to the way indexes are stored, we are actually doing an "update" here 
    //and removing old keys that no longer exist for this region entry
    Object oldKey = mapKeyToIndexKey.get(mapKey);
    if (oldKey == null) {
      rg.addMapping(indexKey, value, entry);
    }
    else if ((oldKey != null && !oldKey.equals(indexKey))) {     
      rg.addMapping(indexKey, value, entry);
      rg.removeMapping(oldKey, entry);
    }
    this.internalIndexStats.incUpdatesInProgress(-1);
    long end = System.nanoTime() - start;
    this.internalIndexStats.incUpdateTime(end);
    this.internalIndexStats.incNumUpdates();
    mapKeyToIndexKey.put(mapKey, indexKey);
  }
}
