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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.MapIndexable;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionEntry;

public abstract class AbstractMapIndex extends AbstractIndex {
  final boolean isAllKeys;

  final String[] patternStr;

  protected final Map<Object, AbstractIndex> mapKeyToValueIndex;

  protected final Object[] mapKeys;

  AbstractMapIndex(InternalCache cache, String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes, String origFromClause,
      String origIndxExpr, String[] defintions, boolean isAllKeys,
      String[] multiIndexingKeysPattern, Object[] mapKeys, IndexStatistics stats) {
    super(cache, indexName, region, fromClause, indexedExpression, projectionAttributes,
        origFromClause, origIndxExpr, defintions, stats);
    this.mapKeyToValueIndex = new ConcurrentHashMap<Object, AbstractIndex>(2, 0.75f, 1);
    RegionAttributes ra = region.getAttributes();
    this.isAllKeys = isAllKeys;
    this.mapKeys = mapKeys;
    if (this.isAllKeys) {
      this.patternStr = new String[] {new StringBuilder(indexedExpression)
          .deleteCharAt(indexedExpression.length() - 2).toString()};

    } else {
      this.patternStr = multiIndexingKeysPattern;
    }
  }

  @Override
  void addMapping(RegionEntry entry) throws IMQException {
    this.evaluator.evaluate(entry, true);
  }

  @Override
  protected InternalIndexStatistics createStats(String indexName) {
    // PartitionedIndexStatistics are used for PR
    if (!(this.region instanceof BucketRegion)) {
      return new MapIndexStatistics(indexName);
    } else {
      return new InternalIndexStatistics() {};
    }
  }

  public boolean getIsAllKeys() {
    return isAllKeys;
  }

  class MapIndexStatistics extends InternalIndexStatistics {
    private IndexStats vsdStats;

    public MapIndexStatistics(String indexName) {
      this.vsdStats = new IndexStats(getRegion().getCache().getDistributedSystem(), indexName);
    }

    /**
     * Return the total number of times this index has been updated
     */
    @Override
    public long getNumUpdates() {
      return this.vsdStats.getNumUpdates();
    }

    @Override
    public void incNumValues(int delta) {
      this.vsdStats.incNumValues(delta);
    }

    @Override
    public void incNumUpdates() {
      this.vsdStats.incNumUpdates();
    }

    @Override
    public void incNumUpdates(int delta) {
      this.vsdStats.incNumUpdates(delta);
    }

    @Override
    public void updateNumKeys(long numKeys) {
      this.vsdStats.updateNumKeys(numKeys);
    }

    @Override
    public void incNumMapIndexKeys(long numKeys) {
      this.vsdStats.incNumMapIndexKeys(numKeys);
    }

    @Override
    public void incNumKeys(long numKeys) {
      this.vsdStats.incNumKeys(numKeys);
    }

    @Override
    public void incUpdateTime(long delta) {
      this.vsdStats.incUpdateTime(delta);
    }

    @Override
    public void incUpdatesInProgress(int delta) {
      this.vsdStats.incUpdatesInProgress(delta);
    }

    @Override
    public void incNumUses() {
      this.vsdStats.incNumUses();
    }

    @Override
    public void incUseTime(long delta) {
      this.vsdStats.incUseTime(delta);
    }

    @Override
    public void incUsesInProgress(int delta) {
      this.vsdStats.incUsesInProgress(delta);
    }

    @Override
    public void incReadLockCount(int delta) {
      this.vsdStats.incReadLockCount(delta);
    }

    /**
     * Returns the total amount of time (in nanoseconds) spent updating this index.
     */
    @Override
    public long getTotalUpdateTime() {
      return this.vsdStats.getTotalUpdateTime();
    }

    /**
     * Returns the total number of times this index has been accessed by a query.
     */
    @Override
    public long getTotalUses() {
      return this.vsdStats.getTotalUses();
    }

    /**
     * Returns the number of keys in this index at the highest level
     */
    @Override
    public long getNumberOfMapIndexKeys() {
      return this.vsdStats.getNumberOfMapIndexKeys();
    }

    /**
     * Returns the number of keys in this index.
     */
    @Override
    public long getNumberOfKeys() {
      return this.vsdStats.getNumberOfKeys();
    }

    /**
     * Returns the number of values in this index.
     */
    @Override
    public long getNumberOfValues() {
      return this.vsdStats.getNumberOfValues();
    }

    /**
     * Return the number of values for the specified key in this index.
     */
    @Override
    public long getNumberOfValues(Object key) {
      long numValues = 0;
      for (Object ind : mapKeyToValueIndex.values()) {
        numValues += ((Index) ind).getStatistics().getNumberOfValues(key);
      }
      return numValues;
    }

    /**
     * Return the number of read locks taken on this index
     */
    @Override
    public int getReadLockCount() {
      return this.vsdStats.getReadLockCount();
    }

    @Override
    public void close() {
      this.vsdStats.close();
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("No Keys = ").append(getNumberOfKeys()).append("\n");
      sb.append("No Map Index Keys = ").append(getNumberOfMapIndexKeys()).append("\n");
      sb.append("No Values = ").append(getNumberOfValues()).append("\n");
      sb.append("No Uses = ").append(getTotalUses()).append("\n");
      sb.append("No Updates = ").append(getNumUpdates()).append("\n");
      sb.append("Total Update time = ").append(getTotalUpdateTime()).append("\n");
      return sb.toString();
    }
  }

  @Override
  public ObjectType getResultSetType() {
    return this.evaluator.getIndexResultSetType();
  }

  @Override
  void instantiateEvaluator(IndexCreationHelper indexCreationHelper) {
    this.evaluator = new IMQEvaluator(indexCreationHelper);
  }

  @Override
  public void initializeIndex(boolean loadEntries) throws IMQException {
    evaluator.initializeIndex(loadEntries);
  }

  @Override
  void lockedQuery(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection) throws TypeMismatchException,
      FunctionDomainException, NameResolutionException, QueryInvocationTargetException {
    Object[] mapKeyAndVal = (Object[]) key;
    AbstractIndex ri = this.mapKeyToValueIndex.get(mapKeyAndVal[1]);
    if (ri != null) {
      ri.lockedQuery(mapKeyAndVal[0], operator, results, iterOps, runtimeItr, context, projAttrib,
          intermediateResults, isIntersection);
    }
  }

  @Override
  void lockedQuery(Object lowerBoundKey, int lowerBoundOperator, Object upperBoundKey,
      int upperBoundOperator, Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    throw new UnsupportedOperationException(
        "Range grouping for MapIndex condition is not supported");

  }

  @Override
  void lockedQuery(Object key, int operator, Collection results, Set keysToRemove,
      ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    Object[] mapKeyAndVal = (Object[]) key;
    AbstractIndex ri = this.mapKeyToValueIndex.get(mapKeyAndVal[1]);
    if (ri != null) {
      ri.lockedQuery(mapKeyAndVal[0], operator, results, keysToRemove, context);
    }
  }

  @Override
  abstract void recreateIndexData() throws IMQException;

  @Override
  protected abstract void removeMapping(RegionEntry entry, int opCode) throws IMQException;

  @Override
  public boolean clear() throws QueryException {
    throw new UnsupportedOperationException("MapType Index method not supported");
  }

  @Override
  public int getSizeEstimate(Object key, int op, int matchLevel) throws TypeMismatchException {
    Object[] mapKeyAndVal = (Object[]) key;
    Object mapKey = mapKeyAndVal[1];
    AbstractIndex ri = this.mapKeyToValueIndex.get(mapKey);
    if (ri != null) {
      return ri.getSizeEstimate(mapKeyAndVal[0], op, matchLevel);
    } else {
      return 0;
    }
  }

  @Override
  protected boolean isCompactRangeIndex() {
    return false;
  }

  @Override
  public IndexType getType() {
    return IndexType.FUNCTIONAL;
  }

  @Override
  public boolean isMapType() {
    return true;
  }

  @Override
  void addMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    addOrSaveMapping(key, value, entry, true);
  }

  @Override
  void saveMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    addOrSaveMapping(key, value, entry, false);
  }

  void addOrSaveMapping(Object key, Object value, RegionEntry entry, boolean isAdd)
      throws IMQException {
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
        if (isAdd) {
          this.doIndexAddition(mapKey, indexKey, value, entry);
        } else {
          this.saveIndexAddition(mapKey, indexKey, value, entry);
        }
      }
    } else {
      for (Object mapKey : mapKeys) {
        Object indexKey;
        if (key == null) {
          indexKey = QueryService.UNDEFINED;
        } else {
          indexKey = ((Map) key).get(mapKey);
        }
        if (isAdd) {
          this.doIndexAddition(mapKey, indexKey, value, entry);
        } else {
          this.saveIndexAddition(mapKey, indexKey, value, entry);
        }
      }
    }
  }

  protected abstract void doIndexAddition(Object mapKey, Object indexKey, Object value,
      RegionEntry entry) throws IMQException;

  protected abstract void saveIndexAddition(Object mapKey, Object indexKey, Object value,
      RegionEntry entry) throws IMQException;

  public Map<Object, AbstractIndex> getRangeIndexHolderForTesting() {
    return Collections.unmodifiableMap(this.mapKeyToValueIndex);
  }

  public String[] getPatternsForTesting() {
    return this.patternStr;
  }

  public Object[] getMapKeysForTesting() {
    return this.mapKeys;
  }

  @Override
  public abstract boolean containsEntry(RegionEntry entry);

  @Override
  public boolean isMatchingWithIndexExpression(CompiledValue condnExpr, String conditionExprStr,
      ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    if (this.isAllKeys) {
      // check if the conditionExps is of type MapIndexable.If yes then check
      // the canonicalized string
      // stripped of the index arg & see if it matches.
      if (condnExpr instanceof MapIndexable) {
        MapIndexable mi = (MapIndexable) condnExpr;
        CompiledValue recvr = mi.getReceiverSansIndexArgs();
        StringBuilder sb = new StringBuilder();
        recvr.generateCanonicalizedExpression(sb, context);
        sb.append('[').append(']');
        return sb.toString().equals(this.patternStr[0]);

      } else {
        return false;
      }
    } else {
      for (String expr : this.patternStr) {
        if (expr.equals(conditionExprStr)) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean isEmpty() {
    return mapKeyToValueIndex.size() == 0 ? true : false;
  }

}
