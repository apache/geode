/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.ExecutionContext;
import com.gemstone.gemfire.cache.query.internal.MapIndexable;
import com.gemstone.gemfire.cache.query.internal.RuntimeIterator;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;

public abstract class AbstractMapIndex extends AbstractIndex
{
  final protected boolean isAllKeys;

  final String[] patternStr;

  protected final Map<Object, AbstractIndex> mapKeyToValueIndex;

  protected final Object[] mapKeys;
    
  AbstractMapIndex(String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes,
      String origFromClause, String origIndxExpr, String[] defintions,
      boolean isAllKeys, String[] multiIndexingKeysPattern, Object[] mapKeys, IndexStatistics stats) {
    super(indexName, region, fromClause, indexedExpression,
        projectionAttributes, origFromClause, origIndxExpr, defintions, stats);
    this.mapKeyToValueIndex = new ConcurrentHashMap<Object, AbstractIndex>(2, 0.75f, 1);
    RegionAttributes ra = region.getAttributes();
    this.isAllKeys = isAllKeys;
    this.mapKeys = mapKeys;
    if (this.isAllKeys) {
      this.patternStr = new String[] { new StringBuilder(indexedExpression)
          .deleteCharAt(indexedExpression.length() - 2).toString() };

    }
    else {
      this.patternStr = multiIndexingKeysPattern;
    }
  }

  @Override
  void addMapping(RegionEntry entry) throws IMQException
  {
    this.evaluator.evaluate(entry, true);
  }

  protected InternalIndexStatistics createStats(String indexName) {
    // PartitionedIndexStatistics are used for PR
    if (!(this.region instanceof BucketRegion)) {
      return new MapIndexStatistics(indexName);
    } else {
      return new InternalIndexStatistics() {
      };
    }
  }

  class MapIndexStatistics extends InternalIndexStatistics {
    private IndexStats vsdStats;

    public MapIndexStatistics(String indexName) {
      this.vsdStats = new IndexStats(getRegion().getCache()
          .getDistributedSystem(), indexName);
    }

    /**
     * Return the total number of times this index has been updated
     */
    public long getNumUpdates() {
      return this.vsdStats.getNumUpdates();
    }

    public void incNumValues(int delta) {
      this.vsdStats.incNumValues(delta);
    }

    public void incNumUpdates() {
      this.vsdStats.incNumUpdates();
    }

    public void incNumUpdates(int delta) {
      this.vsdStats.incNumUpdates(delta);
    }

    public void updateNumKeys(long numKeys) {
      this.vsdStats.updateNumKeys(numKeys);
    }

    public void incNumMapIndexKeys(long numKeys) {
      this.vsdStats.incNumMapIndexKeys(numKeys);
    }

    public void incNumKeys(long numKeys) {
      this.vsdStats.incNumKeys(numKeys);
    }

    public void incUpdateTime(long delta) {
      this.vsdStats.incUpdateTime(delta);
    }

    public void incUpdatesInProgress(int delta) {
      this.vsdStats.incUpdatesInProgress(delta);
    }

    public void incNumUses() {
      this.vsdStats.incNumUses();
    }

    public void incUseTime(long delta) {
      this.vsdStats.incUseTime(delta);
    }

    public void incUsesInProgress(int delta) {
      this.vsdStats.incUsesInProgress(delta);
    }

    public void incReadLockCount(int delta) {
      this.vsdStats.incReadLockCount(delta);
    }

    /**
     * Returns the total amount of time (in nanoseconds) spent updating this
     * index.
     */
    public long getTotalUpdateTime() {
      return this.vsdStats.getTotalUpdateTime();
    }

    /**
     * Returns the total number of times this index has been accessed by a
     * query.
     */
    public long getTotalUses() {
      return this.vsdStats.getTotalUses();
    }

    /**
     * Returns the number of keys in this index
     * at the highest level
     */
    public long getNumberOfMapIndexKeys() {
      return this.vsdStats.getNumberOfMapIndexKeys();
    }

    /**
     * Returns the number of keys in this index.
     */
    public long getNumberOfKeys() {
      return this.vsdStats.getNumberOfKeys();
    }

    /**
     * Returns the number of values in this index.
     */
    public long getNumberOfValues() {
      return this.vsdStats.getNumberOfValues();
    }

    /**
     * Return the number of values for the specified key in this index.
     */
    public long getNumberOfValues(Object key) {
      long numValues = 0;
      for (Object ind : mapKeyToValueIndex.values()) { 
        numValues += ((AbstractIndex)ind).getStatistics().getNumberOfValues(key);
      }
      return numValues;
    }

    /**
     * Return the number of read locks taken on this index
     */
    public int getReadLockCount() {
      return this.vsdStats.getReadLockCount();
    }
    

    public void close() {
      this.vsdStats.close();
    }

    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("No Keys = ").append(getNumberOfKeys()).append("\n");
      sb.append("No Map Index Keys = ").append(getNumberOfMapIndexKeys()).append("\n");
      sb.append("No Values = ").append(getNumberOfValues()).append("\n");
      sb.append("No Uses = ").append(getTotalUses()).append("\n");
      sb.append("No Updates = ").append(getNumUpdates()).append("\n");
      sb.append("Total Update time = ").append(getTotalUpdateTime())
          .append("\n");
      return sb.toString();
    }
  } 

  @Override
  public ObjectType getResultSetType()
  {
    return this.evaluator.getIndexResultSetType();
  }

  @Override
  void instantiateEvaluator(IndexCreationHelper ich)
  {
    this.evaluator = new IMQEvaluator(ich);
  }

  @Override
  public void initializeIndex(boolean loadEntries) throws IMQException
  {
    evaluator.initializeIndex(loadEntries);
  }

  @Override
  void lockedQuery(Object key, int operator, Collection results,
      CompiledValue iterOps, RuntimeIterator runtimeItr,
      ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection)
      throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException
  {
    Object[] mapKeyAndVal = (Object[])key;
    AbstractIndex ri = this.mapKeyToValueIndex.get(mapKeyAndVal[1]);
    if (ri != null) {
      ri.lockedQuery(mapKeyAndVal[0], operator, results, iterOps, runtimeItr,
          context, projAttrib, intermediateResults, isIntersection);
    }
  }

  @Override
  void lockedQuery(Object lowerBoundKey, int lowerBoundOperator,
      Object upperBoundKey, int upperBoundOperator, Collection results,
      Set keysToRemove, ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException
  {
    throw new UnsupportedOperationException(
        "Range grouping for MapIndex condition is not supported");

  }

  @Override
  void lockedQuery(Object key, int operator, Collection results,
      Set keysToRemove, ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException
  {
    Object[] mapKeyAndVal = (Object[])key;
    AbstractIndex ri = this.mapKeyToValueIndex.get(mapKeyAndVal[1]);
    if (ri != null) {
      ri.lockedQuery(mapKeyAndVal[0], operator, results, keysToRemove, context);
    }
  }

  abstract void recreateIndexData() throws IMQException;

  protected abstract void removeMapping(RegionEntry entry, int opCode) throws IMQException;

  public boolean clear() throws QueryException
  {
    throw new UnsupportedOperationException(
        "MapType Index method not supported");
  }

  public int getSizeEstimate(Object key, int op, int matchLevel)
      throws TypeMismatchException
  {
    Object[] mapKeyAndVal = (Object[])key;
    Object mapKey = mapKeyAndVal[1];
    AbstractIndex ri = this.mapKeyToValueIndex.get(mapKey);
    if (ri != null) {
      return ri.getSizeEstimate(mapKeyAndVal[0], op, matchLevel);
    }
    else {
      return 0;
    }
  }

  @Override
  protected boolean isCompactRangeIndex() {
    return false;
  }
  
  public IndexType getType()
  {
    return IndexType.FUNCTIONAL;
  }

  @Override
  public boolean isMapType()
  {
    return true;
  }

  @Override
  void addMapping(Object key, Object value, RegionEntry entry)
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
        this.doIndexAddition(mapKey, indexKey, value, entry);
      }
    }
    else {
      for (Object mapKey : mapKeys) {
        Object indexKey = ((Map)key).get(mapKey);
        if (indexKey != null) {
          this.doIndexAddition(mapKey, indexKey, value, entry);
        }
      }
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
          this.saveIndexAddition(mapKey, indexKey, value, entry);
        }
      }
    }
  }

  protected abstract void doIndexAddition(Object mapKey, Object indexKey, Object value,
      RegionEntry entry) throws IMQException;

  protected abstract void saveIndexAddition(Object mapKey, Object indexKey, Object value,
      RegionEntry entry) throws IMQException;

  public Map<Object, AbstractIndex> getRangeIndexHolderForTesting()
  {
    return Collections.unmodifiableMap(this.mapKeyToValueIndex);
  }

  public String[] getPatternsForTesting()
  {
    return this.patternStr;
  }

  public Object[] getMapKeysForTesting()
  {
    return this.mapKeys;
  }

  public abstract boolean containsEntry(RegionEntry entry);

  @Override
  public boolean isMatchingWithIndexExpression(CompiledValue condnExpr,
      String conditionExprStr, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException,
      NameResolutionException
  {
    if (this.isAllKeys) {
      // check if the conditionExps is of type MapIndexable.If yes then check
      // the canonicalized string
      // stripped of the index arg & see if it matches.
      if (condnExpr instanceof MapIndexable) {
        MapIndexable mi = (MapIndexable)condnExpr;
        CompiledValue recvr = mi.getRecieverSansIndexArgs();
        StringBuffer sb = new StringBuffer();
        recvr.generateCanonicalizedExpression(sb, context);
        sb.append('[').append(']');
        return sb.toString().equals(this.patternStr[0]);

      }
      else {
        return false;
      }
    }
    else {
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
