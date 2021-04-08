/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query.internal.index;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class CompactMapRangeIndex extends AbstractMapIndex {
  private final Map<Object, Map> entryToMapKeyIndexKeyMap;
  private IndexCreationHelper ich;

  CompactMapRangeIndex(InternalCache cache, String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes, String origFromClause,
      String origIndxExpr, String[] defintions, boolean isAllKeys,
      String[] multiIndexingKeysPattern, Object[] mapKeys, IndexStatistics stats) {
    super(cache, indexName, region, fromClause, indexedExpression, projectionAttributes,
        origFromClause, origIndxExpr, defintions, isAllKeys, multiIndexingKeysPattern, mapKeys,
        stats);
    RegionAttributes ra = region.getAttributes();
    this.entryToMapKeyIndexKeyMap = new java.util.concurrent.ConcurrentHashMap(
        ra.getInitialCapacity(), ra.getLoadFactor(), ra.getConcurrencyLevel());
  }

  @Override
  void instantiateEvaluator(IndexCreationHelper indexCreationHelper) {
    this.evaluator = new IMQEvaluator(indexCreationHelper);
    this.ich = indexCreationHelper;
  }

  @Override
  public boolean containsEntry(RegionEntry entry) {
    return this.entryToMapKeyIndexKeyMap.containsKey(entry);
  }

  @Override
  void recreateIndexData() throws IMQException {
    this.entryToMapKeyIndexKeyMap.clear();
    this.mapKeyToValueIndex.clear();
    this.initializeIndex(true);
  }

  @Override
  protected void removeMapping(RegionEntry entry, int opCode) throws IMQException {
    // this implementation has a reverse map, so it doesn't handle
    // BEFORE_UPDATE_OP
    if (opCode == BEFORE_UPDATE_OP || opCode == CLEAN_UP_THREAD_LOCALS) {
      return;
    }

    // Object values = this.entryToMapKeysMap.remove(entry);
    Map mapKeyToIndexKey = this.entryToMapKeyIndexKeyMap.remove(entry);
    // Values in reverse coould be null if map in region value does not
    // contain any key which matches to index expression keys.
    if (mapKeyToIndexKey == null) {
      return;
    }

    Iterator<Map.Entry<?, ?>> mapKeyIterator = mapKeyToIndexKey.entrySet().iterator();
    while (mapKeyIterator.hasNext()) {
      Map.Entry<?, ?> mapEntry = mapKeyIterator.next();
      Object mapKey = mapEntry.getKey();
      Object indexKey = mapEntry.getValue();
      CompactRangeIndex ri = (CompactRangeIndex) this.mapKeyToValueIndex.get(mapKey);
      long start = System.nanoTime();
      this.internalIndexStats.incUpdatesInProgress(1);
      ri.removeMapping(indexKey, entry);
      this.internalIndexStats.incUpdatesInProgress(-1);
      long end = System.nanoTime() - start;
      this.internalIndexStats.incUpdateTime(end);
      this.internalIndexStats.incNumUpdates();
    }
  }

  private static final Logger logger = LogService.getLogger();

  @Override
  void saveMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    if (key == QueryService.UNDEFINED || (key != null && !(key instanceof Map))) {
      return;
    }
    if (this.isAllKeys) {
      // If the key is null or it has no elements then we cannot associate it
      // to any index key (it would apply to all). That is why
      // this type of index does not support !=
      // queries or queries comparing with null.
      if (key == null) {
        return;
      }
      Iterator<Map.Entry<?, ?>> entries = ((Map) key).entrySet().iterator();
      while (entries.hasNext()) {
        Map.Entry<?, ?> mapEntry = entries.next();
        Object mapKey = mapEntry.getKey();
        Object indexKey = mapEntry.getValue();
        this.saveIndexAddition(mapKey, indexKey, value, entry);
      }
      removeOldMappings(((Map) key).keySet(), entry);
    } else {
      logger.info("toberal saveMaping. mapKeys: {}", mapKeys);
      for (Object mapKey : mapKeys) {
        Object indexKey;
        if (key == null) {
          indexKey = QueryService.UNDEFINED;
        } else {
          indexKey = ((Map) key).get(mapKey);
        }
        this.saveIndexAddition(mapKey, indexKey, value, entry);
      }
    }
  }

  private void removeOldMappings(Collection presentKeys, RegionEntry entry) throws IMQException {
    Map oldKeysAndValuesForEntry = entryToMapKeyIndexKeyMap.get(entry);
    if (oldKeysAndValuesForEntry == null) {
      oldKeysAndValuesForEntry = Collections.EMPTY_MAP;
    }
    Set<Entry> removedKeyValueEntries = oldKeysAndValuesForEntry != null
        ? oldKeysAndValuesForEntry.entrySet() : Collections.EMPTY_SET;
    Iterator<Entry> iterator = removedKeyValueEntries.iterator();
    while (iterator.hasNext()) {
      Entry keyValue = iterator.next();
      Object indexKey = keyValue.getKey() == null ? IndexManager.NULL : keyValue.getKey();
      if (!presentKeys.contains(indexKey)) {
        CompactRangeIndex rg = (CompactRangeIndex) this.mapKeyToValueIndex.get(keyValue.getKey());
        rg.removeMapping(keyValue.getValue(), entry);
        iterator.remove();
      }
    }
  }


  @Override
  protected void doIndexAddition(Object mapKey, Object indexKey, Object value, RegionEntry entry)
      throws IMQException {
    if (indexKey == null) {
      indexKey = IndexManager.NULL;
    }
    if (mapKey == null) {
      mapKey = IndexManager.NULL;
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
      rg = new CompactRangeIndex(cache, indexName + "-" + mapKey, region, fromClause,
          indexedExpression, projectionAttributes, this.originalFromClause,
          this.originalIndexedExpression, this.canonicalizedDefinitions, stats);
      rg.instantiateEvaluator(this.ich,
          ((AbstractIndex.IMQEvaluator) this.evaluator).getIndexResultSetType());
      this.mapKeyToValueIndex.put(mapKey, rg);
      if (!isPr) {
        this.internalIndexStats.incNumMapIndexKeys(1);
      }
    }
    long start = System.nanoTime();
    rg.addMapping(indexKey, value, entry);
    // This call is skipped when addMapping is called from MapRangeIndex
    // rg.internalIndexStats.incNumUpdates();
    this.internalIndexStats.incUpdatesInProgress(-1);
    long end = System.nanoTime() - start;
    this.internalIndexStats.incUpdateTime(end);
    this.internalIndexStats.incNumUpdates();
    // add to mapkey to indexkey map
    Map mapKeyToIndexKey = this.entryToMapKeyIndexKeyMap.get(entry);
    if (mapKeyToIndexKey == null) {
      mapKeyToIndexKey = new HashMap();
      entryToMapKeyIndexKeyMap.put(entry, mapKeyToIndexKey);
    }
    mapKeyToIndexKey.put(mapKey, indexKey);
  }

  @Override
  protected void saveIndexAddition(Object mapKey, Object indexKey, Object value, RegionEntry entry)
      throws IMQException {
    if (indexKey == null) {
      indexKey = IndexManager.NULL;
    }
    if (mapKey == null) {
      mapKey = IndexManager.NULL;
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
      rg = new CompactRangeIndex(cache, indexName + "-" + mapKey, region, fromClause,
          indexedExpression, projectionAttributes, this.originalFromClause,
          this.originalIndexedExpression, this.canonicalizedDefinitions, stats);
      rg.instantiateEvaluator(this.ich,
          ((AbstractIndex.IMQEvaluator) this.evaluator).getIndexResultSetType());
      this.mapKeyToValueIndex.put(mapKey, rg);
      if (!isPr) {
        this.internalIndexStats.incNumMapIndexKeys(1);
      }
    }
    this.internalIndexStats.incUpdatesInProgress(1);
    long start = System.nanoTime();

    // add to mapkey to indexkey map
    Map mapKeyToIndexKey = this.entryToMapKeyIndexKeyMap.get(entry);
    if (mapKeyToIndexKey == null) {
      mapKeyToIndexKey = new HashMap();
      entryToMapKeyIndexKeyMap.put(entry, mapKeyToIndexKey);
    }
    // Due to the way indexes are stored, we are actually doing an "update" here
    // and removing old keys that no longer exist for this region entry
    Object oldKey = mapKeyToIndexKey.get(mapKey);
    if (oldKey == null) {
      rg.addMapping(indexKey, value, entry);
    } else if (!oldKey.equals(indexKey)) {
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
