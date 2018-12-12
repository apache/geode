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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledID;
import org.apache.geode.cache.query.internal.CompiledIndexOperation;
import org.apache.geode.cache.query.internal.CompiledIteratorDef;
import org.apache.geode.cache.query.internal.CompiledOperation;
import org.apache.geode.cache.query.internal.CompiledPath;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.CqEntry;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.IndexInfo;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.QueryUtils;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.internal.StructFields;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.Support;
import org.apache.geode.cache.query.internal.index.IndexStore.IndexStoreEntry;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxString;

/**
 * This class implements abstract algorithms common to all indexes, such as index creation, use of a
 * path evaluator object, etc. It serves as the factory for a path evaluator object and maintains
 * the path evaluator object to use for index creation and index maintenance. It also maintains a
 * reference to the root collection on which the index is created. This class also implements the
 * abstract methods to add and remove entries to an underlying storage structure (e.g. a btree), and
 * as part of this algorithm, maintains a map of entries that map to null at the end of the index
 * path, and entries that cannot be traversed to the end of the index path (traversal is undefined).
 */
public abstract class AbstractIndex implements IndexProtocol {
  private static final Logger logger = LogService.getLogger();

  // package-private to avoid synthetic accessor
  static final AtomicIntegerFieldUpdater<RegionEntryToValuesMap> atomicUpdater =
      AtomicIntegerFieldUpdater.newUpdater(RegionEntryToValuesMap.class, "numValues");

  final InternalCache cache;

  final String indexName;

  final Region region;

  final String indexedExpression;

  final String fromClause;

  final String projectionAttributes;

  final String originalIndexedExpression;

  final String originalFromClause;

  private final String originalProjectionAttributes;

  final String[] canonicalizedDefinitions;

  private boolean isValid;

  protected IndexedExpressionEvaluator evaluator;

  InternalIndexStatistics internalIndexStats;

  /** For PartitionedIndex for now */
  protected Index prIndex;

  /**
   * Flag to indicate if index map has keys as PdxString All the keys in the index map should be
   * either Strings or PdxStrings
   */
  private Boolean isIndexedPdxKeys = false;

  /** Flag to indicate if the flag isIndexedPdxKeys is set */
  Boolean isIndexedPdxKeysFlagSet = false;

  boolean indexOnRegionKeys = false;

  boolean indexOnValues = false;

  private final ReadWriteLock removeIndexLock = new ReentrantReadWriteLock();

  /** Flag to indicate if the index is populated with data */
  volatile boolean isPopulated = false;

  AbstractIndex(InternalCache cache, String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes, String originalFromClause,
      String originalIndexedExpression, String[] defintions, IndexStatistics stats) {
    this.cache = cache;
    this.indexName = indexName;
    this.region = region;
    this.indexedExpression = indexedExpression;
    this.fromClause = fromClause;
    this.originalIndexedExpression = originalIndexedExpression;
    this.originalFromClause = originalFromClause;
    this.canonicalizedDefinitions = defintions;
    if (StringUtils.isEmpty(projectionAttributes)) {
      projectionAttributes = "*";
    }
    this.projectionAttributes = projectionAttributes;
    this.originalProjectionAttributes = projectionAttributes;
    if (stats != null) {
      this.internalIndexStats = (InternalIndexStatistics) stats;
    } else {
      this.internalIndexStats = createStats(indexName);
    }
  }

  /**
   * Must be implemented by all implementing classes iff they have any forward map for
   * index-key->RE.
   *
   * @return the forward map of respective index.
   */
  public Map getValueToEntriesMap() {
    return null;
  }

  /**
   * Get statistics information for this index.
   */
  @Override
  public IndexStatistics getStatistics() {
    return this.internalIndexStats;
  }

  @Override
  public void destroy() {
    markValid(false);
    if (this.internalIndexStats != null) {
      this.internalIndexStats.updateNumKeys(0);
      this.internalIndexStats.close();
    }
  }

  long updateIndexUpdateStats() {
    long result = System.nanoTime();
    this.internalIndexStats.incUpdatesInProgress(1);
    return result;
  }

  void updateIndexUpdateStats(long start) {
    long end = System.nanoTime();
    this.internalIndexStats.incUpdatesInProgress(-1);
    this.internalIndexStats.incUpdateTime(end - start);
  }

  long updateIndexUseStats() {
    return updateIndexUseStats(true);
  }

  long updateIndexUseStats(boolean updateStats) {
    long result = 0;
    if (updateStats) {
      this.internalIndexStats.incUsesInProgress(1);
      result = System.nanoTime();
    }
    return result;
  }

  void updateIndexUseEndStats(long start) {
    updateIndexUseEndStats(start, true);
  }

  void updateIndexUseEndStats(long start, boolean updateStats) {
    if (updateStats) {
      long end = System.nanoTime();
      this.internalIndexStats.incUsesInProgress(-1);
      this.internalIndexStats.incNumUses();
      this.internalIndexStats.incUseTime(end - start);
    }
  }

  public IndexedExpressionEvaluator getEvaluator() {
    return this.evaluator;
  }

  /**
   * The Region this index is on
   *
   * @return the Region for this index
   */
  @Override
  public Region getRegion() {
    return this.region;
  }

  /**
   * Returns the unique name of this index
   */
  @Override
  public String getName() {
    return this.indexName;
  }

  @Override
  public void query(Object key, int operator, Collection results, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {

    // get a read lock when doing a lookup
    if (context.getBucketList() != null && this.region instanceof BucketRegion) {
      PartitionedRegion pr = ((Bucket) this.region).getPartitionedRegion();
      long start = updateIndexUseStats();
      try {
        for (Object bucketId : context.getBucketList()) {
          AbstractIndex bucketIndex =
              PartitionedIndex.getBucketIndex(pr, this.indexName, (Integer) bucketId);
          if (bucketIndex == null) {
            continue;
          }
          bucketIndex.lockedQuery(key, operator, results, null/* No Keys to be removed */, context);

        }
      } finally {
        updateIndexUseEndStats(start);
      }
    } else {
      long start = updateIndexUseStats();
      try {
        lockedQuery(key, operator, results, null/* No Keys to be removed */, context);
      } finally {
        updateIndexUseEndStats(start);
      }
    }
  }

  @Override
  public void query(Object key, int operator, Collection results, @Retained CompiledValue iterOp,
      RuntimeIterator indpndntItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection) throws TypeMismatchException,
      FunctionDomainException, NameResolutionException, QueryInvocationTargetException {

    // get a read lock when doing a lookup
    if (context.getBucketList() != null && this.region instanceof BucketRegion) {
      PartitionedRegion pr = ((Bucket) region).getPartitionedRegion();
      long start = updateIndexUseStats();
      try {
        for (Object bucketId : context.getBucketList()) {
          AbstractIndex bucketIndex =
              PartitionedIndex.getBucketIndex(pr, this.indexName, (Integer) bucketId);
          if (bucketIndex == null) {
            continue;
          }
          bucketIndex.lockedQuery(key, operator, results, iterOp, indpndntItr, context, projAttrib,
              intermediateResults, isIntersection);
        }
      } finally {
        updateIndexUseEndStats(start);
      }
    } else {
      long start = updateIndexUseStats();
      try {
        lockedQuery(key, operator, results, iterOp, indpndntItr, context, projAttrib,
            intermediateResults, isIntersection);
      } finally {
        updateIndexUseEndStats(start);
      }
    }
  }

  @Override
  public void query(Object key, int operator, Collection results, Set keysToRemove,
      ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {

    // get a read lock when doing a lookup
    if (context.getBucketList() != null && this.region instanceof BucketRegion) {
      PartitionedRegion pr = ((Bucket) region).getPartitionedRegion();
      long start = updateIndexUseStats();
      try {
        for (Object bucketId : context.getBucketList()) {
          AbstractIndex bucketIndex =
              PartitionedIndex.getBucketIndex(pr, this.indexName, (Integer) bucketId);
          if (bucketIndex == null) {
            continue;
          }
          bucketIndex.lockedQuery(key, operator, results, keysToRemove, context);
        }
      } finally {
        updateIndexUseEndStats(start);
      }
    } else {
      long start = updateIndexUseStats();
      try {
        lockedQuery(key, operator, results, keysToRemove, context);
      } finally {
        updateIndexUseEndStats(start);
      }
    }
  }

  @Override
  public void query(Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {

    Iterator iterator = keysToRemove.iterator();
    Object temp = iterator.next();
    iterator.remove();
    if (context.getBucketList() != null && this.region instanceof BucketRegion) {
      long start = updateIndexUseStats();
      try {
        PartitionedRegion partitionedRegion = ((Bucket) this.region).getPartitionedRegion();
        for (Object bucketId : context.getBucketList()) {
          AbstractIndex bucketIndex = PartitionedIndex.getBucketIndex(partitionedRegion,
              this.indexName, (Integer) bucketId);
          if (bucketIndex == null) {
            continue;
          }
          bucketIndex.lockedQuery(temp, OQLLexerTokenTypes.TOK_NE, results,
              iterator.hasNext() ? keysToRemove : null, context);
        }
      } finally {
        updateIndexUseEndStats(start);
      }
    } else {
      long start = updateIndexUseStats();
      try {
        lockedQuery(temp, OQLLexerTokenTypes.TOK_NE, results,
            iterator.hasNext() ? keysToRemove : null, context);
      } finally {
        updateIndexUseEndStats(start);
      }
    }
  }

  @Override
  public void query(Object lowerBoundKey, int lowerBoundOperator, Object upperBoundKey,
      int upperBoundOperator, Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {

    if (context.getBucketList() != null) {
      if (this.region instanceof BucketRegion) {
        PartitionedRegion partitionedRegion = ((Bucket) this.region).getPartitionedRegion();
        long start = updateIndexUseStats();
        try {
          for (Object bucketId : context.getBucketList()) {
            AbstractIndex bucketIndex = PartitionedIndex.getBucketIndex(partitionedRegion,
                this.indexName, (Integer) bucketId);
            if (bucketIndex == null) {
              continue;
            }
            bucketIndex.lockedQuery(lowerBoundKey, lowerBoundOperator, upperBoundKey,
                upperBoundOperator, results, keysToRemove, context);
          }
        } finally {
          updateIndexUseEndStats(start);
        }
      }
    } else {
      long start = updateIndexUseStats();
      try {
        lockedQuery(lowerBoundKey, lowerBoundOperator, upperBoundKey, upperBoundOperator, results,
            keysToRemove, context);
      } finally {
        updateIndexUseEndStats(start);
      }
    }
  }

  @Override
  public List queryEquijoinCondition(IndexProtocol index, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {

    Support.assertionFailed(
        " This function should have never got invoked as its meaningful implementation is present only in RangeIndex class");
    return null;
  }

  /**
   * Get the projectionAttributes for this expression.
   *
   * @return the projectionAttributes, or "*" if there were none specified at index creation.
   */
  @Override
  public String getProjectionAttributes() {
    return this.originalProjectionAttributes;
  }

  /**
   * Get the projectionAttributes for this expression.
   *
   * @return the projectionAttributes, or "*" if there were none specified at index creation.
   */
  @Override
  public String getCanonicalizedProjectionAttributes() {
    return this.projectionAttributes;
  }

  /**
   * Get the Original indexedExpression for this index.
   */
  @Override
  public String getIndexedExpression() {
    return this.originalIndexedExpression;
  }

  /**
   * Get the Canonicalized indexedExpression for this index.
   */
  @Override
  public String getCanonicalizedIndexedExpression() {
    return this.indexedExpression;
  }

  /**
   * Get the original fromClause for this index.
   */
  @Override
  public String getFromClause() {
    return this.originalFromClause;
  }

  /**
   * Get the canonicalized fromClause for this index.
   */
  @Override
  public String getCanonicalizedFromClause() {
    return this.fromClause;
  }

  public boolean isMapType() {
    return false;
  }

  @Override
  public boolean addIndexMapping(RegionEntry entry) throws IMQException {
    addMapping(entry);

    // if no exception, then success
    return true;
  }

  @Override
  public boolean addAllIndexMappings(Collection<RegionEntry> c) throws IMQException {
    for (RegionEntry regionEntry : c) {
      addMapping(regionEntry);
    }
    // if no exception, then success
    return true;
  }

  /**
   * @param opCode one of OTHER_OP, BEFORE_UPDATE_OP, AFTER_UPDATE_OP.
   */
  @Override
  public boolean removeIndexMapping(RegionEntry entry, int opCode) throws IMQException {
    removeMapping(entry, opCode);
    // if no exception, then success
    return true;
  }

  @Override
  public boolean removeAllIndexMappings(Collection<RegionEntry> c) throws IMQException {
    for (RegionEntry regionEntry : c) {
      removeMapping(regionEntry, OTHER_OP);
    }
    // if no exception, then success
    return true;
  }


  public boolean isValid() {
    return this.isValid;
  }

  @Override
  public void markValid(boolean b) {
    this.isValid = b;
  }

  @Override
  public boolean isMatchingWithIndexExpression(CompiledValue condnExpr, String condnExprStr,
      ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    return this.indexedExpression.equals(condnExprStr);
  }

  // package-private to avoid synthetic accessor
  Object verifyAndGetPdxDomainObject(Object value) {
    if (value instanceof StructImpl) {
      // Doing hasPdx check first, since its cheaper.
      if (((StructImpl) value).isHasPdx()
          && !((InternalCache) this.region.getCache()).getPdxReadSerializedByAnyGemFireServices()) {
        // Set the pdx values for the struct object.
        StructImpl v = (StructImpl) value;
        Object[] fieldValues = v.getPdxFieldValues();
        return new StructImpl((StructTypeImpl) v.getStructType(), fieldValues);
      }
    } else if (value instanceof PdxInstance
        && !((InternalCache) this.region.getCache()).getPdxReadSerializedByAnyGemFireServices()) {
      return ((PdxInstance) value).getObject();
    }
    return value;
  }

  private void addToResultsWithUnionOrIntersection(Collection results,
      SelectResults intermediateResults, boolean isIntersection, Object value) {
    value = verifyAndGetPdxDomainObject(value);

    if (intermediateResults == null) {
      results.add(value);
    } else {
      if (isIntersection) {
        int numOcc = intermediateResults.occurrences(value);
        if (numOcc > 0) {
          results.add(value);
          intermediateResults.remove(value);
        }
      } else {
        results.add(value);
      }
    }
  }

  private void addToStructsWithUnionOrIntersection(Collection results,
      SelectResults intermediateResults, boolean isIntersection, Object[] values) {

    for (int i = 0; i < values.length; i++) {
      values[i] = verifyAndGetPdxDomainObject(values[i]);
    }

    if (intermediateResults == null) {
      if (results instanceof StructFields) {
        ((StructFields) results).addFieldValues(values);
      } else {
        // The results could be LinkedStructSet or SortedResultsBag or StructSet
        SelectResults selectResults = (SelectResults) results;
        StructImpl structImpl = new StructImpl(
            (StructTypeImpl) selectResults.getCollectionType().getElementType(), values);
        selectResults.add(structImpl);
      }

    } else {
      if (isIntersection) {
        if (results instanceof StructFields) {
          int occurrences = intermediateResults.occurrences(values);
          if (occurrences > 0) {
            ((StructFields) results).addFieldValues(values);
            ((StructFields) intermediateResults).removeFieldValues(values);
          }

        } else {
          // could be LinkedStructSet or SortedResultsBag
          SelectResults selectResults = (SelectResults) results;
          StructImpl structImpl = new StructImpl(
              (StructTypeImpl) selectResults.getCollectionType().getElementType(), values);
          if (intermediateResults.remove(structImpl)) {
            selectResults.add(structImpl);
          }
        }

      } else {
        if (results instanceof StructFields) {
          ((StructFields) results).addFieldValues(values);
        } else {
          // could be LinkedStructSet or SortedResultsBag
          SelectResults selectResults = (SelectResults) results;
          StructImpl structImpl = new StructImpl(
              (StructTypeImpl) selectResults.getCollectionType().getElementType(), values);
          if (intermediateResults.remove(structImpl)) {
            selectResults.add(structImpl);
          }
        }
      }
    }
  }

  void applyCqOrProjection(List projAttrib, ExecutionContext context, Collection result,
      Object iterValue, SelectResults intermediateResults, boolean isIntersection, Object key)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    if (context != null && context.isCqQueryContext()) {
      result.add(new CqEntry(key, iterValue));
    } else {
      applyProjection(projAttrib, context, result, iterValue, intermediateResults, isIntersection);
    }
  }

  void applyProjection(List projAttrib, ExecutionContext context, Collection result,
      Object iterValue, SelectResults intermediateResults, boolean isIntersection)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {

    if (projAttrib == null) {
      iterValue = deserializePdxForLocalDistinctQuery(context, iterValue);
      this.addToResultsWithUnionOrIntersection(result, intermediateResults, isIntersection,
          iterValue);

    } else {
      boolean isStruct = result instanceof SelectResults
          && ((SelectResults) result).getCollectionType().getElementType() != null
          && ((SelectResults) result).getCollectionType().getElementType().isStructType();

      if (isStruct) {
        int projCount = projAttrib.size();
        Object[] values = new Object[projCount];
        Iterator projIter = projAttrib.iterator();
        int i = 0;
        while (projIter.hasNext()) {
          Object[] projDef = (Object[]) projIter.next();
          values[i] = deserializePdxForLocalDistinctQuery(context,
              ((CompiledValue) projDef[1]).evaluate(context));
          i++;
        }
        this.addToStructsWithUnionOrIntersection(result, intermediateResults, isIntersection,
            values);
      } else {
        Object[] temp = (Object[]) projAttrib.get(0);
        Object val = deserializePdxForLocalDistinctQuery(context,
            ((CompiledValue) temp[1]).evaluate(context));
        this.addToResultsWithUnionOrIntersection(result, intermediateResults, isIntersection, val);
      }
    }
  }

  /**
   * For local queries with distinct, deserialize all PdxInstances as we do not have a way to
   * compare Pdx and non Pdx objects in case the cache has a mix of pdx and non pdx objects. We
   * still have to honor the cache level readSerialized flag in case of all Pdx objects in cache.
   * Also always convert PdxString to String before adding to resultSet for remote queries
   */
  private Object deserializePdxForLocalDistinctQuery(ExecutionContext context, Object value)
      throws QueryInvocationTargetException {

    if (!((DefaultQuery) context.getQuery()).isRemoteQuery()) {
      if (context.isDistinct() && value instanceof PdxInstance
          && !this.region.getCache().getPdxReadSerialized()) {
        try {
          value = ((PdxInstance) value).getObject();
        } catch (Exception ex) {
          throw new QueryInvocationTargetException(
              "Unable to retrieve domain object from PdxInstance while building the ResultSet. "
                  + ex.getMessage());
        }
      } else if (value instanceof PdxString) {
        value = value.toString();
      }
    }
    return value;
  }

  private void removeFromResultsWithUnionOrIntersection(Collection results,
      SelectResults intermediateResults, boolean isIntersection, Object value) {

    if (intermediateResults == null) {
      results.remove(value);
    } else {
      if (isIntersection) {
        int numOcc = ((SelectResults) results).occurrences(value);
        if (numOcc > 0) {
          results.remove(value);
          intermediateResults.add(value);
        }
      } else {
        results.remove(value);
      }
    }
  }

  private void removeFromStructsWithUnionOrIntersection(Collection results,
      SelectResults intermediateResults, boolean isIntersection, Object[] values) {

    if (intermediateResults == null) {
      ((StructFields) results).removeFieldValues(values);
    } else {
      if (isIntersection) {
        int numOcc = ((SelectResults) results).occurrences(values);
        if (numOcc > 0) {
          ((StructFields) results).removeFieldValues(values);
          ((StructFields) intermediateResults).addFieldValues(values);

        }
      } else {
        ((StructFields) results).removeFieldValues(values);
      }
    }
  }

  private void removeProjection(List projAttrib, ExecutionContext context, Collection result,
      Object iterValue, SelectResults intermediateResults, boolean isIntersection)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {

    if (projAttrib == null) {
      this.removeFromResultsWithUnionOrIntersection(result, intermediateResults, isIntersection,
          iterValue);
    } else {
      if (result instanceof StructFields) {
        int projCount = projAttrib.size();
        Object[] values = new Object[projCount];
        Iterator projIter = projAttrib.iterator();
        int i = 0;
        while (projIter.hasNext()) {
          Object projDef[] = (Object[]) projIter.next();
          values[i++] = ((CompiledValue) projDef[1]).evaluate(context);
        }
        this.removeFromStructsWithUnionOrIntersection(result, intermediateResults, isIntersection,
            values);
      } else {
        Object[] temp = (Object[]) projAttrib.get(0);
        Object val = ((CompiledValue) temp[1]).evaluate(context);
        this.removeFromResultsWithUnionOrIntersection(result, intermediateResults, isIntersection,
            val);
      }
    }

  }

  /**
   * This function returns the canonicalized definitions of the from clauses used in Index creation
   */
  @Override
  public String[] getCanonicalizedIteratorDefinitions() {
    return this.canonicalizedDefinitions;
  }

  /**
   * This implementation is for PrimaryKeyIndex. RangeIndex has its own implementation. For
   * PrimaryKeyIndex , this method should not be used
   * <p>
   * TODO: check if an Exception should be thrown if the function implementation of this class gets
   * invoked
   */
  @Override
  public boolean containsEntry(RegionEntry entry) {
    return false;
  }

  abstract void instantiateEvaluator(IndexCreationHelper indexCreationHelper);

  @Override
  public void initializeIndex(boolean loadEntries) throws IMQException {
    // implement me
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Index [");
    sb.append(" Name=").append(getName());
    sb.append(" Type =").append(getType());
    sb.append(" IdxExp=").append(getIndexedExpression());
    sb.append(" From=").append(getFromClause());
    sb.append(" Proj=").append(getProjectionAttributes());
    sb.append(']');
    return sb.toString();
  }

  public abstract boolean isEmpty();

  protected abstract boolean isCompactRangeIndex();

  protected abstract InternalIndexStatistics createStats(String indexName);

  @Override
  public abstract ObjectType getResultSetType();

  abstract void recreateIndexData() throws IMQException;

  abstract void addMapping(RegionEntry entry) throws IMQException;

  abstract void removeMapping(RegionEntry entry, int opCode) throws IMQException;

  abstract void addMapping(Object key, Object value, RegionEntry entry) throws IMQException;

  /**
   * This is used to buffer the index entries evaluated from a RegionEntry which is getting updated
   * at present. These buffered index entries are replaced into the index later all together to
   * avoid remove-add sequence.
   */
  abstract void saveMapping(Object key, Object value, RegionEntry entry) throws IMQException;

  /** Lookup method used when appropriate lock is held */
  abstract void lockedQuery(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator indpndntItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection) throws TypeMismatchException,
      FunctionDomainException, NameResolutionException, QueryInvocationTargetException;

  abstract void lockedQuery(Object lowerBoundKey, int lowerBoundOperator, Object upperBoundKey,
      int upperBoundOperator, Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException;

  abstract void lockedQuery(Object key, int operator, Collection results, Set keysToRemove,
      ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException;

  public Index getPRIndex() {
    return this.prIndex;
  }

  void setPRIndex(Index parIndex) {
    this.prIndex = parIndex;
  }

  /**
   * Dummy implementation that subclasses can override.
   */
  protected abstract static class InternalIndexStatistics implements IndexStatistics {
    @Override
    public long getNumUpdates() {
      return 0L;
    }

    @Override
    public long getTotalUpdateTime() {
      return 0L;
    }

    @Override
    public long getTotalUses() {
      return 0L;
    }

    @Override
    public long getNumberOfKeys() {
      return 0L;
    }

    @Override
    public long getNumberOfValues() {
      return 0L;
    }

    @Override
    public long getNumberOfValues(Object key) {
      return 0L;
    }

    @Override
    public int getReadLockCount() {
      return 0;
    }

    @Override
    public long getNumberOfMapIndexKeys() {
      return 0;
    }

    @Override
    public int getNumberOfBucketIndexes() {
      return 0;
    }

    public void close() {}

    public void incNumValues(int delta) {}

    public void incNumUpdates() {}

    public void incNumUpdates(int delta) {}

    public void incUpdatesInProgress(int delta) {}

    public void incUsesInProgress(int delta) {}

    public void updateNumKeys(long count) {}

    public void incNumKeys(long count) {}

    public void incNumMapIndexKeys(long numKeys) {}

    public void incUpdateTime(long delta) {}

    public void incNumUses() {}

    public void incUseTime(long delta) {}

    public void incReadLockCount(int delta) {}

    public void incNumBucketIndexes(int delta) {}
  }

  class IMQEvaluator implements IndexedExpressionEvaluator {
    private final InternalCache cache;

    private List fromIterators = null;

    private CompiledValue indexedExpr = null;

    private final String[] canonicalIterNames;

    private ObjectType indexResultSetType = null;

    private Map dependencyGraph = null;

    /**
     * The boolean if true indicates that the 0th iterator is on entries . If the 0th iterator is on
     * collection of Region.Entry objects, then the RegionEntry object used in Index data objects is
     * obtained directly from its corresponding Region.Entry object. However if the 0th iterator is
     * not on entries then the boolean is false. In this case the additional projection attribute
     * gives us the original value of the iterator while the Region.Entry object is obtained from
     * 0th iterator. It is possible to have index being created on a Region Entry itself , instead
     * of a Region. A Map operator( Compiled Index Operator) used with Region enables, us to create
     * such indexes. In such case the 0th iterator, even if it represents a collection of Objects
     * which are not Region.Entry objects, still the boolean remains true, as the Entry object can
     * be easily obtained from the 0th iterator. In this case, the additional projection attribute s
     * not null as it is used to evaluate the Entry object from the 0th iterator.
     */
    private boolean isFirstItrOnEntry = false;

    /** The boolean if true indicates that the 0th iterator is on keys. */
    private boolean isFirstItrOnKey = false;

    /**
     * List of modified iterators, not null only when the boolean isFirstItrOnEntry is false.
     */
    private List indexInitIterators = null;

    /**
     * The additional Projection attribute representing the value of the original 0th iterator. If
     * the isFirstItrOnEntry is false, then it is not null. However if the isFirstItrOnEntry is true
     * but & still this attribute is not null, this indicates that the 0th iterator is derived using
     * an individual entry thru Map operator on the Region.
     */
    private CompiledValue additionalProj = null;

    /** This is not null iff the boolean isFirstItrOnEntry is false. */
    private CompiledValue modifiedIndexExpr = null;

    private ObjectType addnlProjType = null;

    private int initEntriesUpdated = 0;

    private boolean hasInitOccurredOnce = false;

    private ExecutionContext initContext = null;

    private int iteratorSize = -1;

    private Region rgn = null;

    /** Creates a new instance of IMQEvaluator */
    IMQEvaluator(IndexCreationHelper helper) {
      this.cache = helper.getCache();
      this.fromIterators = helper.getIterators();
      this.indexedExpr = helper.getCompiledIndexedExpression();
      this.rgn = helper.getRegion();
      // The modified iterators for optimizing Index creation
      this.isFirstItrOnEntry = ((FunctionalIndexCreationHelper) helper).isFirstIteratorRegionEntry;
      this.isFirstItrOnKey = ((FunctionalIndexCreationHelper) helper).isFirstIteratorRegionKey;
      this.additionalProj = ((FunctionalIndexCreationHelper) helper).additionalProj;
      Object[] params1 = {new QRegion(this.rgn, false)};
      this.initContext = new ExecutionContext(params1, this.cache);
      this.canonicalIterNames = ((FunctionalIndexCreationHelper) helper).canonicalizedIteratorNames;
      if (this.isFirstItrOnEntry) {
        this.indexInitIterators = this.fromIterators;
      } else {
        this.indexInitIterators = ((FunctionalIndexCreationHelper) helper).indexInitIterators;
        this.modifiedIndexExpr = ((FunctionalIndexCreationHelper) helper).modifiedIndexExpr;
        this.addnlProjType = ((FunctionalIndexCreationHelper) helper).addnlProjType;
      }
      this.iteratorSize = this.indexInitIterators.size();
    }

    @Override
    public String getIndexedExpression() {
      return AbstractIndex.this.getCanonicalizedIndexedExpression();
    }

    @Override
    public String getProjectionAttributes() {
      return AbstractIndex.this.getCanonicalizedProjectionAttributes();
    }

    @Override
    public String getFromClause() {
      return AbstractIndex.this.getCanonicalizedFromClause();
    }

    @Override
    public void expansion(List expandedResults, Object lowerBoundKey, Object upperBoundKey,
        int lowerBoundOperator, int upperBoundOperator, Object value) throws IMQException {
      // no-op
    }

    @Override
    public void evaluate(RegionEntry target, boolean add) throws IMQException {
      assert add; // ignored, but should be true here
      DummyQRegion dQRegion = new DummyQRegion(this.rgn);
      dQRegion.setEntry(target);
      Object[] params = {dQRegion};
      ExecutionContext context = new ExecutionContext(params, this.cache);
      context.newScope(IndexCreationHelper.INDEX_QUERY_SCOPE_ID);

      try {
        boolean computeDependency = true;
        if (this.dependencyGraph != null) {
          context.setDependencyGraph(this.dependencyGraph);
          computeDependency = false;
        }

        for (int i = 0; i < this.iteratorSize; i++) {
          CompiledIteratorDef iterDef = (CompiledIteratorDef) this.fromIterators.get(i);
          // Compute the dependency only once. The call to methods of this
          // class are thread safe as for update lock on Index is taken .
          if (computeDependency) {
            iterDef.computeDependencies(context);
          }
          RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
          context.addToIndependentRuntimeItrMapForIndexCreation(iterDef);
          context.bindIterator(rIter);
        }

        // Save the dependency graph for future updates.
        if (this.dependencyGraph == null) {
          this.dependencyGraph = context.getDependencyGraph();
        }

        Support.Assert(this.indexResultSetType != null,
            "IMQEvaluator::evaluate:The StrcutType should have been initialized during index creation");

        doNestedIterations(0, context);
      } catch (IMQException imqe) {
        throw imqe;
      } catch (Exception e) {
        throw new IMQException(e);
      } finally {
        context.popScope();
      }
    }

    /**
     * This function is used for creating Index data at the start
     */
    @Override
    public void initializeIndex(boolean loadEntries) throws IMQException {
      this.initEntriesUpdated = 0;
      try {
        // Since an index initialization can happen multiple times for a given region, due to clear
        // operation, we are using hardcoded scope ID of 1 , as otherwise if obtained from
        // ExecutionContext object, it will get incremented on very index initialization
        this.initContext.newScope(1);
        for (int i = 0; i < this.iteratorSize; i++) {
          CompiledIteratorDef iterDef = (CompiledIteratorDef) this.indexInitIterators.get(i);
          RuntimeIterator rIter = null;
          if (!this.hasInitOccurredOnce) {
            iterDef.computeDependencies(this.initContext);
            rIter = iterDef.getRuntimeIterator(this.initContext);
            this.initContext.addToIndependentRuntimeItrMapForIndexCreation(iterDef);
          }
          if (rIter == null) {
            rIter = iterDef.getRuntimeIterator(this.initContext);
          }
          this.initContext.bindIterator(rIter);
        }
        this.hasInitOccurredOnce = true;
        if (this.indexResultSetType == null) {
          this.indexResultSetType = createIndexResultSetType();
        }
        if (loadEntries) {
          doNestedIterationsForIndexInit(0, this.initContext.getCurrentIterators());
        }
      } catch (IMQException imqe) {
        throw imqe;
      } catch (Exception e) {
        throw new IMQException(e);
      } finally {
        this.initContext.popScope();
      }
    }

    private void doNestedIterationsForIndexInit(int level, List runtimeIterators)
        throws TypeMismatchException, AmbiguousNameException, FunctionDomainException,
        NameResolutionException, QueryInvocationTargetException, IMQException {
      if (level == 1) {
        ++this.initEntriesUpdated;
      }
      if (level == this.iteratorSize) {
        applyProjectionForIndexInit(runtimeIterators);
      } else {
        RuntimeIterator rIter = (RuntimeIterator) runtimeIterators.get(level);
        Collection collection = rIter.evaluateCollection(this.initContext);
        if (collection == null) {
          return;
        }
        for (Object aCollection : collection) {
          rIter.setCurrent(aCollection);
          doNestedIterationsForIndexInit(level + 1, runtimeIterators);
        }
      }
    }

    /**
     * This function is used to obtain Index data at the time of index creation. Each element of the
     * List is an Object Array of size 3. The 0th element of Object Array stores the value of Index
     * Expression. The 1st element of ObjectArray contains the RegionEntry object ( If the boolean
     * isFirstItrOnEntry is false, then the 0th iterator will give us the Region.Entry object which
     * can be used to obtain the underlying RegionEntry object. If the boolean is true & additional
     * projection attribute is not null, then the Region.Entry object can be obtained by evaluating
     * the additional projection attribute. If the boolean isFirstItrOnEntry is true & additional
     * projection attribute is null, then the 0th iterator itself will evaluate to Region.Entry
     * Object.
     * <p>
     * The 2nd element of Object Array contains the Struct object ( tuple) created. If the boolean
     * isFirstItrOnEntry is false, then the first attribute of the Struct object is obtained by
     * evaluating the additional projection attribute.
     */
    private void applyProjectionForIndexInit(List currrentRuntimeIters)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException, IMQException {

      if (QueryMonitor.isLowMemory()) {
        throw new IMQException(
            "Index creation canceled due to low memory");
      }

      LocalRegion.NonTXEntry temp;

      // Evaluate NonTXEntry for index on entries or additional projections
      // on Entry or just entry value.
      if (this.isFirstItrOnEntry && this.additionalProj != null) {
        temp = (LocalRegion.NonTXEntry) this.additionalProj.evaluate(this.initContext);
      } else {
        temp = (LocalRegion.NonTXEntry) ((RuntimeIterator) currrentRuntimeIters.get(0))
            .evaluate(this.initContext);
      }

      RegionEntry re = temp.getRegionEntry();
      Object indxResultSet;

      if (this.iteratorSize == 1) {
        indxResultSet = this.isFirstItrOnEntry
            ? this.additionalProj == null ? temp
                : ((RuntimeIterator) currrentRuntimeIters.get(0)).evaluate(this.initContext)
            : this.additionalProj.evaluate(this.initContext);
      } else {
        Object[] tuple = new Object[this.iteratorSize];
        int i = this.isFirstItrOnEntry ? 0 : 1;
        for (; i < this.iteratorSize; i++) {
          RuntimeIterator iter = (RuntimeIterator) currrentRuntimeIters.get(i);
          tuple[i] = iter.evaluate(this.initContext);
        }
        if (!this.isFirstItrOnEntry) {
          tuple[0] = this.additionalProj.evaluate(this.initContext);
        }
        Support.Assert(this.indexResultSetType instanceof StructTypeImpl,
            "The Index ResultType should have been an instance of StructTypeImpl rather than ObjectTypeImpl. The indxeResultType is "
                + this.indexResultSetType);
        indxResultSet = new StructImpl((StructTypeImpl) this.indexResultSetType, tuple);
      }

      // Key must be evaluated after indexResultSet evaluation is done as Entry might be getting
      // destroyed and so if value is UNDEFINED, key will definitely will be UNDEFINED.
      Object indexKey = this.isFirstItrOnEntry ? this.indexedExpr.evaluate(this.initContext)
          : this.modifiedIndexExpr.evaluate(this.initContext);
      // based on the first key convert the rest to PdxString or String
      if (!AbstractIndex.this.isIndexedPdxKeysFlagSet) {
        setPdxStringFlag(indexKey);
      }
      indexKey = getPdxStringForIndexedPdxKeys(indexKey);
      addMapping(indexKey, indxResultSet, re);
    }

    private void doNestedIterations(int level, ExecutionContext context)
        throws TypeMismatchException, AmbiguousNameException, FunctionDomainException,
        NameResolutionException, QueryInvocationTargetException, IMQException {

      List iterList = context.getCurrentIterators();
      if (level == this.iteratorSize) {
        applyProjection(context);
      } else {
        RuntimeIterator rIter = (RuntimeIterator) iterList.get(level);
        Collection collection = rIter.evaluateCollection(context);
        if (collection == null) {
          return;
        }
        for (Object aCollection : collection) {
          rIter.setCurrent(aCollection);
          doNestedIterations(level + 1, context);
        }
      }
    }

    private void applyProjection(ExecutionContext context)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException, IMQException {

      List currrentRuntimeIters = context.getCurrentIterators();
      Object indexKey = this.indexedExpr.evaluate(context);
      // based on the first key convert the rest to PdxString or String
      if (!AbstractIndex.this.isIndexedPdxKeysFlagSet) {
        setPdxStringFlag(indexKey);
      }
      indexKey = getPdxStringForIndexedPdxKeys(indexKey);
      Object indxResultSet;

      if (this.iteratorSize == 1) {
        RuntimeIterator iter = (RuntimeIterator) currrentRuntimeIters.get(0);
        indxResultSet = iter.evaluate(context);
      } else {
        Object tuple[] = new Object[this.iteratorSize];
        for (int i = 0; i < this.iteratorSize; i++) {
          RuntimeIterator iter = (RuntimeIterator) currrentRuntimeIters.get(i);
          tuple[i] = iter.evaluate(context);
        }
        Support.Assert(this.indexResultSetType instanceof StructTypeImpl,
            "The Index ResultType should have been an instance of StructTypeImpl rather than ObjectTypeImpl. The indxeResultType is "
                + this.indexResultSetType);
        indxResultSet = new StructImpl((StructTypeImpl) this.indexResultSetType, tuple);
      }

      // Keep Entry value in fly until all keys are evaluated
      RegionEntry entry = ((DummyQRegion) context.getBindArgument(1)).getEntry();
      saveMapping(indexKey, indxResultSet, entry);
    }

    /**
     * The struct type calculation is modified if the 0th iterator is modified to make it dependent
     * on Entry
     */
    private ObjectType createIndexResultSetType() {
      List currentIterators = this.initContext.getCurrentIterators();
      int len = currentIterators.size();
      ObjectType[] fieldTypes = new ObjectType[len];
      int start = this.isFirstItrOnEntry ? 0 : 1;
      for (; start < len; start++) {
        RuntimeIterator iter = (RuntimeIterator) currentIterators.get(start);
        fieldTypes[start] = iter.getElementType();
      }
      if (!this.isFirstItrOnEntry) {
        fieldTypes[0] = this.addnlProjType;
      }
      return len == 1 ? fieldTypes[0] : new StructTypeImpl(this.canonicalIterNames, fieldTypes);
    }

    @Override
    public ObjectType getIndexResultSetType() {
      return this.indexResultSetType;
    }

    boolean isFirstItrOnEntry() {
      return this.isFirstItrOnEntry;
    }

    boolean isFirstItrOnKey() {
      return this.isFirstItrOnKey;
    }

    @Override
    public List getAllDependentIterators() {
      return this.fromIterators;
    }
  }

  /**
   * Checks the limit for the resultset for distinct and non-distinct queries separately. In case of
   * non-distinct distinct elements size of result-set is matched against limit passed in as an
   * argument.
   *
   * @return true if limit is satisfied.
   */
  boolean verifyLimit(Collection result, int limit) {
    return limit > 0 && result.size() == limit;
  }

  /**
   * This will verify the consistency between RegionEntry and IndexEntry. RangeIndex has following
   * entry structure,
   *
   * IndexKey --> [RegionEntry, [Iterator1, Iterator2....., IteratorN]]
   *
   * Where Iterator1 to IteratorN are iterators defined in index from clause.
   *
   * For example: "/portfolio p, p.positions.values pos" from clause has two iterators where p is
   * independent iterator and pos is dependent iterator.
   *
   * Query iterators can be a subset, superset or exact match of index iterators. But we take query
   * iterators which are matching with index iterators to evaluate RegionEntry for new value and
   * compare it with index value which could be a plain object or a Struct.
   *
   * Note: Struct evaluated from RegionEntry can NOT have more field values than Index Value Struct
   * as we filter out iterators in query context before evaluating Struct from RegionEntry.
   *
   * @return True if Region and Index entries are consistent.
   */
  // package-private to avoid synthetic accessor
  boolean verifyEntryAndIndexValue(RegionEntry re, Object value, ExecutionContext context) {
    IMQEvaluator evaluator = (IMQEvaluator) getEvaluator();
    List valuesInRegion = null;
    Object valueInIndex = null;

    try {
      // In a RegionEntry key and Entry itself can not be modified else RegionEntry itself will
      // change. So no need to verify anything just return true.
      if (evaluator.isFirstItrOnKey()) {
        return true;
      } else if (evaluator.isFirstItrOnEntry()) {
        valuesInRegion = evaluateIndexIteratorsFromRE(re, context);
        valueInIndex = verifyAndGetPdxDomainObject(value);
      } else {
        RegionEntryContext regionEntryContext = context.getPartitionedRegion() != null
            ? context.getPartitionedRegion() : (RegionEntryContext) region;
        Object val = re.getValueInVM(regionEntryContext);
        if (val instanceof CachedDeserializable) {
          val = ((CachedDeserializable) val).getDeserializedValue(getRegion(), re);
        }
        val = verifyAndGetPdxDomainObject(val);
        valueInIndex = verifyAndGetPdxDomainObject(value);
        valuesInRegion = evaluateIndexIteratorsFromRE(val, context);
      }
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Exception occurred while verifying a Region Entry value during a Query when the Region Entry is under update operation",
            e);
      }
    }

    // We could have many index keys available in one Region entry or just one.
    if (!valuesInRegion.isEmpty()) {
      for (Object valueInRegion : valuesInRegion) {
        if (compareStructWithNonStruct(valueInRegion, valueInIndex)) {
          return true;
        }
      }
      return false;
    } else {
      // Seems like value has been invalidated.
      return false;
    }
  }

  /**
   * This method compares two objects in which one could be StructType and other ObjectType. Fur
   * conditions are possible, Object1 -> Struct Object2-> Struct Object1 -> Struct Object2-> Object
   * Object1 -> Object Object2-> Struct Object1 -> Object Object2-> Object
   *
   * @return true if valueInRegion's all objects are part of valueInIndex.
   */
  private boolean compareStructWithNonStruct(Object valueInRegion, Object valueInIndex) {
    if (valueInRegion instanceof Struct && valueInIndex instanceof Struct) {
      Object[] regFields = ((StructImpl) valueInRegion).getFieldValues();
      List indFields = Arrays.asList(((StructImpl) valueInIndex).getFieldValues());
      for (Object regField : regFields) {
        if (!indFields.contains(regField)) {
          return false;
        }
      }
      return true;

    } else if (valueInRegion instanceof Struct) {
      Object[] fields = ((StructImpl) valueInRegion).getFieldValues();
      for (Object field : fields) {
        if (field.equals(valueInIndex)) {
          return true;
        }
      }

    } else if (valueInIndex instanceof Struct) {
      Object[] fields = ((StructImpl) valueInIndex).getFieldValues();
      for (Object field : fields) {
        if (field.equals(valueInRegion)) {
          return true;
        }
      }
    } else {
      return valueInRegion.equals(valueInIndex);
    }
    return false;
  }

  /**
   * Returns evaluated collection for dependent runtime iterator for this index from clause and
   * given RegionEntry.
   *
   * @param context passed here is query context.
   * @return Evaluated second level collection.
   */
  private List evaluateIndexIteratorsFromRE(Object value, ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {

    // We need NonTxEntry to call getValue() on it. RegionEntry does
    // NOT have public getValue() method.
    if (value instanceof RegionEntry) {
      value = ((LocalRegion) this.getRegion()).new NonTXEntry((RegionEntry) value);
    }
    // Get all Independent and dependent iterators for this Index.
    List itrs = getAllDependentRuntimeIterators(context);

    return evaluateLastColl(value, context, itrs, 0);
  }

  private List evaluateLastColl(Object value, ExecutionContext context, List itrs, int level)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {

    // A tuple is a value generated from RegionEntry value which could be a StructType (Multiple
    // Dependent Iterators) or ObjectType (Single Iterator) value.
    List tuples = new ArrayList(1);

    RuntimeIterator currItrator = (RuntimeIterator) itrs.get(level);
    currItrator.setCurrent(value);

    // If its last iterator then just evaluate final struct.
    if (itrs.size() - 1 == level) {
      if (itrs.size() > 1) {
        Object[] tuple = new Object[itrs.size()];
        for (int i = 0; i < itrs.size(); i++) {
          RuntimeIterator iter = (RuntimeIterator) itrs.get(i);
          tuple[i] = iter.evaluate(context);
        }
        // Its ok to pass type as null as we are only interested in values.
        tuples.add(new StructImpl(new StructTypeImpl(), tuple));
      } else {
        tuples.add(currItrator.evaluate(context));
      }
    } else {
      // Not the last iterator.
      RuntimeIterator nextItr = (RuntimeIterator) itrs.get(level + 1);
      Collection nextLevelValues = nextItr.evaluateCollection(context);

      // If value is null or INVALID then the evaluated collection would be Null.
      if (nextLevelValues != null) {
        for (Object nextLevelValue : nextLevelValues) {
          tuples.addAll(evaluateLastColl(nextLevelValue, context, itrs, level + 1));
        }
      }
    }
    return tuples;
  }

  /**
   * Matches the Collection reference in given context for this index's from-clause in all current
   * independent collection references associated to the context. For example, if a join Query has
   * "/region1 p, region2 e" from clause context contains two region references for p and e and
   * Index could be used for any of those of both of those regions.
   *
   * Note: This Index contains its own from clause definition which corresponds to a region
   * collection reference in given context and must be contained at 0th index in
   * {@link AbstractIndex#canonicalizedDefinitions}.
   *
   * @return {@link RuntimeIterator} this should not be null ever.
   */
  RuntimeIterator getRuntimeIteratorForThisIndex(ExecutionContext context) {
    List<RuntimeIterator> indItrs = context.getCurrentIterators();
    Region rgn = this.getRegion();
    if (rgn instanceof BucketRegion) {
      rgn = ((Bucket) rgn).getPartitionedRegion();
    }
    String regionPath = rgn.getFullPath();
    String definition = this.getCanonicalizedIteratorDefinitions()[0];
    for (RuntimeIterator itr : indItrs) {
      if (itr.getDefinition().equals(regionPath) || itr.getDefinition().equals(definition)) {
        return itr;
      }
    }
    return null;
  }

  /**
   * Similar to {@link #getRuntimeIteratorForThisIndex(ExecutionContext)} except that this one also
   * matches the iterator name if present with alias used in the {@link IndexInfo}
   *
   * @return {@link RuntimeIterator}
   */
  RuntimeIterator getRuntimeIteratorForThisIndex(ExecutionContext context, IndexInfo info) {
    List<RuntimeIterator> indItrs = context.getCurrentIterators();
    Region rgn = this.getRegion();
    if (rgn instanceof BucketRegion) {
      rgn = ((Bucket) rgn).getPartitionedRegion();
    }
    String regionPath = rgn.getFullPath();
    String definition = this.getCanonicalizedIteratorDefinitions()[0];
    for (RuntimeIterator itr : indItrs) {
      if (itr.getDefinition().equals(regionPath) || itr.getDefinition().equals(definition)) {
        // if iterator has name alias must be used in the query
        if (itr.getName() != null) {
          CompiledValue path = info._path();
          // match the iterator name with alias
          String pathName = getReceiverNameFromPath(path);
          if (path.getType() == OQLLexerTokenTypes.Identifier || itr.getName().equals(pathName)) {
            return itr;
          }
        } else {
          return itr;
        }
      }
    }
    return null;
  }

  private String getReceiverNameFromPath(CompiledValue path) {
    if (path instanceof CompiledID) {
      return ((CompiledID) path).getId();
    } else if (path instanceof CompiledPath) {
      return getReceiverNameFromPath(path.getReceiver());
    } else if (path instanceof CompiledOperation) {
      return getReceiverNameFromPath(path.getReceiver());
    } else if (path instanceof CompiledIndexOperation) {
      return getReceiverNameFromPath(path.getReceiver());
    }
    return "";
  }

  /**
   * Take all independent iterators from context and remove the one which matches for this Index's
   * independent iterator. Then get all Dependent iterators from given context for this Index's
   * independent iterator.
   *
   * @param context from executing query.
   * @return List of all iterators pertaining to this Index.
   */
  private List getAllDependentRuntimeIterators(ExecutionContext context) {
    List<RuntimeIterator> indItrs = context
        .getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(getRuntimeIteratorForThisIndex(context));

    List<String> definitions = Arrays.asList(this.getCanonicalizedIteratorDefinitions());
    // These are the common iterators between query from clause and index from clause.
    List itrs = new ArrayList();

    for (RuntimeIterator itr : indItrs) {
      if (definitions.contains(itr.getDefinition())) {
        itrs.add(itr);
      }
    }
    return itrs;
  }

  /**
   * This map is not thread-safe. We rely on the fact that every thread which is trying to update
   * this kind of map (In Indexes), must have RegionEntry lock before adding OR removing elements.
   *
   * This map does NOT provide an iterator. To iterate over its element caller has to get inside the
   * map itself through addValuesToCollection() calls.
   */
  class RegionEntryToValuesMap {
    protected Map map;
    private final boolean useList;
    volatile int numValues;

    RegionEntryToValuesMap(boolean useList) {
      this.map = new ConcurrentHashMap(2, 0.75f, 1);
      this.useList = useList;
    }

    RegionEntryToValuesMap(Map map, boolean useList) {
      this.map = map;
      this.useList = useList;
    }

    /**
     * We do NOT use any locks here as every add is for a RegionEntry which is locked before coming
     * here. No two threads can be entering in this method together for a RegionEntry.
     */
    public void add(RegionEntry entry, Object value) {
      assert value != null;
      // Values must NOT be null and ConcurrentHashMap does not support null values.
      if (value == null) {
        return;
      }
      Object object = this.map.get(entry);
      if (object == null) {
        this.map.put(entry, value);
      } else if (object instanceof Collection) {
        Collection coll = (Collection) object;
        // If its a list query might get ConcurrentModificationException.
        // This can only happen for Null mapped or Undefined entries in a
        // RangeIndex. So we are synchronizing on ArrayList.
        if (this.useList) {
          synchronized (coll) {
            coll.add(value);
          }
        } else {
          coll.add(value);
        }
      } else {
        Collection coll = this.useList ? new ArrayList(2) : new IndexConcurrentHashSet(2, 0.75f, 1);
        coll.add(object);
        coll.add(value);
        this.map.put(entry, coll);
      }
      atomicUpdater.incrementAndGet(this);
    }

    public void addAll(RegionEntry entry, Collection values) {
      Object object = this.map.get(entry);
      if (object == null) {
        Collection coll = this.useList ? new ArrayList(values.size())
            : new IndexConcurrentHashSet(values.size(), 0.75f, 1);
        coll.addAll(values);
        this.map.put(entry, coll);
        atomicUpdater.addAndGet(this, values.size());
      } else if (object instanceof Collection) {
        Collection coll = (Collection) object;
        // If its a list query might get ConcurrentModificationException.
        // This can only happen for Null mapped or Undefined entries in a
        // RangeIndex. So we are synchronizing on ArrayList.
        if (this.useList) {
          synchronized (coll) {
            coll.addAll(values);
          }
        } else {
          coll.addAll(values);
        }
      } else {
        Collection coll = this.useList ? new ArrayList(values.size() + 1)
            : new IndexConcurrentHashSet(values.size() + 1, 0.75f, 1);
        coll.addAll(values);
        coll.add(object);
        this.map.put(entry, coll);
      }
      atomicUpdater.addAndGet(this, values.size());
    }

    public Object get(RegionEntry entry) {
      return this.map.get(entry);
    }

    /**
     * We do NOT use any locks here as every remove is for a RegionEntry which is locked before
     * coming here. No two threads can be entering in this method together for a RegionEntry.
     */
    public void remove(RegionEntry entry, Object value) {
      Object object = this.map.get(entry);
      if (object == null)
        return;
      if (object instanceof Collection) {
        Collection coll = (Collection) object;
        boolean removed;
        // If its a list query might get ConcurrentModificationException.
        // This can only happen for Null mapped or Undefined entries in a
        // RangeIndex. So we are synchronizing on ArrayList.
        if (this.useList) {
          synchronized (coll) {
            removed = coll.remove(value);
          }
        } else {
          removed = coll.remove(value);
        }
        if (removed) {
          if (coll.size() == 0) {
            this.map.remove(entry);
          }
          atomicUpdater.decrementAndGet(this);
        }
      } else {
        if (object.equals(value)) {
          this.map.remove(entry);
        }
        atomicUpdater.decrementAndGet(this);
      }
    }

    public Object remove(RegionEntry entry) {
      Object retVal = this.map.remove(entry);
      if (retVal != null) {
        atomicUpdater.addAndGet(this,
            retVal instanceof Collection ? -((Collection) retVal).size() : -1);
      }
      return retVal;
    }

    int getNumValues(RegionEntry entry) {
      Object object = this.map.get(entry);
      if (object == null)
        return 0;
      if (object instanceof Collection) {
        Collection coll = (Collection) object;
        return coll.size();
      } else {
        return 1;
      }
    }

    public int getNumValues() {
      return atomicUpdater.get(this);
    }

    public int getNumEntries() {
      return this.map.keySet().size();
    }

    void addValuesToCollection(Collection result, int limit, ExecutionContext context) {
      for (final Object o : this.map.entrySet()) {
        // Check if query execution on this thread is canceled.
        QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCanceled();
        if (this.verifyLimit(result, limit, context)) {
          return;
        }
        Entry e = (Entry) o;
        Object value = e.getValue();
        assert value != null;

        RegionEntry re = (RegionEntry) e.getKey();
        boolean reUpdateInProgress = re.isUpdateInProgress();
        if (value instanceof Collection) {
          // If its a list query might get ConcurrentModificationException.
          // This can only happen for Null mapped or Undefined entries in a
          // RangeIndex. So we are synchronizing on ArrayList.
          if (this.useList) {
            synchronized (value) {
              for (Object val : (Iterable) value) {
                // Compare the value in index with in RegionEntry.
                if (!reUpdateInProgress || verifyEntryAndIndexValue(re, val, context)) {
                  result.add(val);
                }
                if (limit != -1) {
                  if (result.size() == limit) {
                    return;
                  }
                }
              }
            }
          } else {
            for (Object val : (Iterable) value) {
              // Compare the value in index with in RegionEntry.
              if (!reUpdateInProgress || verifyEntryAndIndexValue(re, val, context)) {
                result.add(val);
              }
              if (limit != -1) {
                if (this.verifyLimit(result, limit, context)) {
                  return;
                }
              }
            }
          }
        } else {
          if (!reUpdateInProgress || verifyEntryAndIndexValue(re, value, context)) {
            if (context.isCqQueryContext()) {
              result.add(new CqEntry(((RegionEntry) e.getKey()).getKey(), value));
            } else {
              result.add(verifyAndGetPdxDomainObject(value));
            }
          }
        }
      }
    }

    void addValuesToCollection(Collection result, CompiledValue iterOp, RuntimeIterator runtimeItr,
        ExecutionContext context, List projAttrib, SelectResults intermediateResults,
        boolean isIntersection, int limit) throws FunctionDomainException, TypeMismatchException,
        NameResolutionException, QueryInvocationTargetException {

      if (this.verifyLimit(result, limit, context)) {
        return;
      }

      for (Object o : this.map.entrySet()) {
        // Check if query execution on this thread is canceled.
        QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCanceled();
        Entry e = (Entry) o;
        Object value = e.getValue();
        // Key is a RegionEntry here.
        RegionEntry entry = (RegionEntry) e.getKey();
        if (value != null) {
          boolean reUpdateInProgress = false;
          if (entry.isUpdateInProgress()) {
            reUpdateInProgress = true;
          }
          if (value instanceof Collection) {
            // If its a list query might get ConcurrentModificationException.
            // This can only happen for Null mapped or Undefined entries in a
            // RangeIndex. So we are synchronizing on ArrayList.
            if (this.useList) {
              synchronized (value) {
                for (Object o1 : ((Iterable) value)) {
                  boolean ok = true;
                  if (reUpdateInProgress) {
                    // Compare the value in index with value in RegionEntry.
                    ok = verifyEntryAndIndexValue(entry, o1, context);
                  }
                  if (ok && runtimeItr != null) {
                    runtimeItr.setCurrent(o1);
                    ok = QueryUtils.applyCondition(iterOp, context);
                  }
                  if (ok) {
                    applyProjection(projAttrib, context, result, o1, intermediateResults,
                        isIntersection);
                    if (limit != -1 && result.size() == limit) {
                      return;
                    }
                  }
                }
              }
            } else {
              for (Object o1 : ((Iterable) value)) {
                boolean ok = true;
                if (reUpdateInProgress) {
                  // Compare the value in index with value in RegionEntry.
                  ok = verifyEntryAndIndexValue(entry, o1, context);
                }
                if (ok && runtimeItr != null) {
                  runtimeItr.setCurrent(o1);
                  ok = QueryUtils.applyCondition(iterOp, context);
                }
                if (ok) {
                  applyProjection(projAttrib, context, result, o1, intermediateResults,
                      isIntersection);
                  if (this.verifyLimit(result, limit, context)) {
                    return;
                  }
                }
              }
            }
          } else {
            boolean ok = true;
            if (reUpdateInProgress) {
              // Compare the value in index with in RegionEntry.
              ok = verifyEntryAndIndexValue(entry, value, context);
            }
            if (ok && runtimeItr != null) {
              runtimeItr.setCurrent(value);
              ok = QueryUtils.applyCondition(iterOp, context);
            }
            if (ok) {
              if (context.isCqQueryContext()) {
                result.add(new CqEntry(((RegionEntry) e.getKey()).getKey(), value));
              } else {
                applyProjection(projAttrib, context, result, value, intermediateResults,
                    isIntersection);
              }
            }
          }
        }
      }
    }

    private boolean verifyLimit(Collection result, int limit, ExecutionContext context) {
      if (limit > 0) {
        if (!context.isDistinct()) {
          return result.size() == limit;
        } else if (result.size() == limit) {
          return true;
        }
      }
      return false;
    }

    public boolean containsEntry(RegionEntry entry) {
      return this.map.containsKey(entry);
    }

    public boolean containsValue(Object value) {
      throw new RuntimeException(
          "Not yet implemented");
    }

    public void clear() {
      this.map.clear();
      atomicUpdater.set(this, 0);
    }

    public Set entrySet() {
      return this.map.entrySet();
    }

    /**
     * This replaces a key's value along with updating the numValues correctly.
     */
    public void replace(RegionEntry entry, Object values) {
      int numOldValues = getNumValues(entry);
      this.map.put(entry, values);
      atomicUpdater.addAndGet(this,
          (values instanceof Collection ? ((Collection) values).size() : 1) - numOldValues);
    }
  }

  /**
   * This will populate resultSet from both type of indexes, {@link CompactRangeIndex} and
   * {@link RangeIndex}.
   */
  void populateListForEquiJoin(List list, Object outerEntries, Object innerEntries,
      ExecutionContext context, Object key) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {

    Assert.assertTrue(outerEntries != null && innerEntries != null,
        "OuterEntries or InnerEntries must not be null");

    Object[][] values = new Object[2][];
    Iterator itr = null;
    int j = 0;

    while (j < 2) {
      boolean isRangeIndex = false;
      if (j == 0) {
        if (outerEntries instanceof RegionEntryToValuesMap) {
          itr = ((RegionEntryToValuesMap) outerEntries).map.entrySet().iterator();
          isRangeIndex = true;
        } else if (outerEntries instanceof CloseableIterator) {
          itr = (Iterator) outerEntries;
        }
      } else {
        if (innerEntries instanceof RegionEntryToValuesMap) {
          itr = ((RegionEntryToValuesMap) innerEntries).map.entrySet().iterator();
          isRangeIndex = true;
        } else if (innerEntries instanceof CloseableIterator) {
          itr = (Iterator) innerEntries;
        }
      }

      // extract the values from the RegionEntries
      List dummy = new ArrayList();
      RegionEntry re = null;
      IndexStoreEntry ie = null;
      Object val = null;
      Object entryVal = null;

      IndexInfo[] indexInfo = (IndexInfo[]) context.cacheGet(CompiledValue.INDEX_INFO);
      IndexInfo indInfo = indexInfo[j];

      while (itr.hasNext()) {
        if (isRangeIndex) {
          Map.Entry entry = (Map.Entry) itr.next();
          val = entry.getValue();
          if (val instanceof Collection) {
            entryVal = ((Iterable) val).iterator().next();
          } else {
            entryVal = val;
          }
          re = (RegionEntry) entry.getKey();
        } else {
          ie = (IndexStoreEntry) itr.next();
        }

        // Bug#41010: We need to verify if Inner and Outer Entries
        // are consistent with index key values.
        boolean ok = true;
        if (isRangeIndex) {
          if (re.isUpdateInProgress()) {
            ok = ((RangeIndex) indInfo._getIndex()).verifyEntryAndIndexValue(re, entryVal, context);
          }
        } else if (ie.isUpdateInProgress()) {
          ok = ((CompactRangeIndex) indInfo._getIndex()).verifyInnerAndOuterEntryValues(ie, context,
              indInfo, key);
        }
        if (ok) {
          if (isRangeIndex) {
            if (val instanceof Collection) {
              dummy.addAll((Collection) val);
            } else {
              dummy.add(val);
            }
          } else {
            if (IndexManager.IS_TEST_EXPANSION) {
              dummy.addAll(((CompactRangeIndex) indInfo._getIndex()).expandValue(context, key, null,
                  OQLLexerTokenTypes.TOK_EQ, -1, ie.getDeserializedValue()));
            } else {
              dummy.add(ie.getDeserializedValue());
            }
          }
        }
      }
      Object[] newValues = new Object[dummy.size()];
      dummy.toArray(newValues);
      values[j++] = newValues;
    }
    list.add(values);
  }

  /**
   * Sets the isIndexedPdxKeys flag indicating if all the keys in the index are Strings or
   * PdxStrings. Also sets another flag isIndexedPdxKeysFlagSet that indicates isIndexedPdxKeys has
   * been set/reset to avoid frequent calculation of map size
   */
  synchronized void setPdxStringFlag(Object key) {
    // For Null and Undefined keys do not set the isIndexedPdxKeysFlagSet flag
    if (isIndexedPdxKeysFlagSet || key == null || key == IndexManager.NULL
        || key == QueryService.UNDEFINED) {
      return;
    }
    if (!this.isIndexedPdxKeys) {
      if (key instanceof PdxString && this.region.getAttributes().getCompressor() == null) {
        this.isIndexedPdxKeys = true;
      }
    }
    this.isIndexedPdxKeysFlagSet = true;
  }

  /**
   * Converts Strings to PdxStrings and vice-versa based on the isIndexedPdxKeys flag
   *
   * @return PdxString or String based on isIndexedPdxKeys flag
   */
  Object getPdxStringForIndexedPdxKeys(Object key) {
    if (this.isIndexedPdxKeys) {
      if (key instanceof String) {
        return new PdxString((String) key);
      }
    } else if (key instanceof PdxString) {
      return key.toString();
    }
    return key;
  }

  boolean acquireIndexReadLockForRemove() {
    boolean success = this.removeIndexLock.readLock().tryLock();
    if (success) {
      this.internalIndexStats.incReadLockCount(1);
      if (logger.isDebugEnabled()) {
        logger.debug("Acquired read lock on index {}", this.getName());
      }
    }
    return success;
  }

  public void releaseIndexReadLockForRemove() {
    this.removeIndexLock.readLock().unlock();
    this.internalIndexStats.incReadLockCount(-1);
    if (logger.isDebugEnabled()) {
      logger.debug("Released read lock on index {}", this.getName());
    }
  }

  /**
   * This makes current thread wait until all query threads are done using it.
   */
  public void acquireIndexWriteLockForRemove() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Acquiring write lock on Index {}", this.getName());
    }
    this.removeIndexLock.writeLock().lock();
    if (isDebugEnabled) {
      logger.debug("Acquired write lock on index {}", this.getName());
    }
  }

  public void releaseIndexWriteLockForRemove() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Releasing write lock on Index {}", this.getName());
    }
    this.removeIndexLock.writeLock().unlock();
    if (isDebugEnabled) {
      logger.debug("Released write lock on Index {}", this.getName());
    }
  }

  public boolean isPopulated() {
    return this.isPopulated;
  }

  public void setPopulated(boolean isPopulated) {
    this.isPopulated = isPopulated;
  }

  boolean isIndexOnPdxKeys() {
    return isIndexedPdxKeys;
  }
}
