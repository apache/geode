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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.Bag;
import com.gemstone.gemfire.cache.query.internal.CompiledID;
import com.gemstone.gemfire.cache.query.internal.CompiledIndexOperation;
import com.gemstone.gemfire.cache.query.internal.CompiledIteratorDef;
import com.gemstone.gemfire.cache.query.internal.CompiledPath;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.CqEntry;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.ExecutionContext;
import com.gemstone.gemfire.cache.query.internal.IndexInfo;
import com.gemstone.gemfire.cache.query.internal.QRegion;
import com.gemstone.gemfire.cache.query.internal.QueryMonitor;
import com.gemstone.gemfire.cache.query.internal.QueryUtils;
import com.gemstone.gemfire.cache.query.internal.RuntimeIterator;
import com.gemstone.gemfire.cache.query.internal.StructFields;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.cache.query.internal.Support;
import com.gemstone.gemfire.cache.query.internal.index.IndexStore.IndexStoreEntry;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.offheap.Chunk;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.internal.PdxString;

/**
 * This class implements abstract algorithms common to all indexes, such as
 * index creation, use of a path evaluator object, etc. It serves as the factory
 * for a path evaluator object and maintains the path evaluator object to use
 * for index creation and index maintenance. It also maintains a reference to
 * the root collection on which the index is created. This class also implements
 * the abstract methods to add and remove entries to an underlying storage
 * structure (e.g. a btree), and as part of this algorithm, maintains a map of
 * entries that map to null at the end of the index path, and entries that
 * cannot be traversed to the end of the index path (traversal is undefined).
 * 
 * @author asif
 * @author vaibhav
 */
public abstract class AbstractIndex implements IndexProtocol
{
  private static final Logger logger = LogService.getLogger();
  
  final String indexName;

  final Region region;

  final String indexedExpression;

  final String fromClause;

  final String projectionAttributes;

  final String originalIndexedExpression;

  final String originalFromClause;

  final String originalProjectionAttributes;

  final String[] canonicalizedDefinitions;
  
  private boolean isValid;
  
  protected IndexedExpressionEvaluator evaluator;
  // Statistics
  protected InternalIndexStatistics internalIndexStats;

  //For PartitionedIndex for now
  protected Index prIndex;
  //Flag to indicate if index map has keys as PdxString
  //All the keys in the index map should be either Strings or PdxStrings
  protected Boolean isIndexedPdxKeys = false;
  
  //Flag to indicate if the flag isIndexedPdxKeys is set
  protected Boolean isIndexedPdxKeysFlagSet = false;

  protected boolean indexOnRegionKeys = false;

  protected boolean indexOnValues = false;
  
  private final ReadWriteLock removeIndexLock = new ReentrantReadWriteLock();

  //Flag to indicate if the index is populated with data
  protected volatile boolean isPopulated = false;

  AbstractIndex(String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes,
      String origFromClause, String origIndxExpr, String[] defintions, IndexStatistics stats) {
    this.indexName = indexName;
    this.region = region;
    this.indexedExpression = indexedExpression;
    this.fromClause = fromClause;
    this.originalIndexedExpression = origIndxExpr;
    this.originalFromClause = origFromClause;
    this.canonicalizedDefinitions = defintions;
    if (projectionAttributes == null || projectionAttributes.length() == 0) {
      projectionAttributes = "*";
    }
    this.projectionAttributes = projectionAttributes;
    this.originalProjectionAttributes = projectionAttributes;
    if (stats != null) {
      this.internalIndexStats = (InternalIndexStatistics)stats;
    } else {
      this.internalIndexStats = createStats(indexName);
    }
  }

  /**
   * Must be implemented by all implementing classes
   * iff they have any forward map for index-key->RE.
   *
   * @return the forward map of respective index.
   */
  public Map getValueToEntriesMap() {
    return null;
  }
  /**
   * Get statistics information for this index.
   */
  public IndexStatistics getStatistics()
  {
    return this.internalIndexStats;
  }

  public void destroy()
  {
    markValid(false);
    if (this.internalIndexStats != null) {
      this.internalIndexStats.updateNumKeys(0);
      this.internalIndexStats.close();
    }
  }

  long updateIndexUpdateStats()
  {
    long result = System.nanoTime();
    this.internalIndexStats.incUpdatesInProgress(1);
    return result;
  }

  void updateIndexUpdateStats(long start)
  {
    long end = System.nanoTime();
    this.internalIndexStats.incUpdatesInProgress(-1);
    this.internalIndexStats.incUpdateTime(end - start);
  }
  
  long updateIndexUseStats() {
    return updateIndexUseStats(true);
  }
  
  long updateIndexUseStats(boolean updateStats)
  {
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
  
  void updateIndexUseEndStats(long start, boolean updateStats)
  {
    if (updateStats) {
      long end = System.nanoTime();
      this.internalIndexStats.incUsesInProgress(-1);
      this.internalIndexStats.incNumUses();
      this.internalIndexStats.incUseTime(end - start);
    }
  }

  public IndexedExpressionEvaluator getEvaluator()
  {
    return evaluator;
  }

  /**
   * The Region this index is on
   * 
   * @return the Region for this index
   */
  public Region getRegion()
  {
    return this.region;
  }

  /**
   * Returns the unique name of this index
   */
  public String getName()
  {
    return this.indexName;
  }

  // ////////// Index default implementation
  public void query(Object key, int operator, Collection results, ExecutionContext context)
  throws TypeMismatchException, FunctionDomainException,
  NameResolutionException, QueryInvocationTargetException {
    // get a read lock when doing a lookup
    if (context.getBucketList() != null && (this.region instanceof BucketRegion)) {
        PartitionedRegion pr = ((BucketRegion)region).getPartitionedRegion();
        long start = updateIndexUseStats();
        try {
          for (Object b :context.getBucketList()) {
            AbstractIndex i = PartitionedIndex.getBucketIndex(pr, this.indexName, (Integer)b);
            if (i == null) {
              continue;
            }
            i.lockedQuery(key, operator, results, null/* No Keys to be removed */, context);
          
          }
        } finally {
          updateIndexUseEndStats(start);
        }
    } else {
      long start = updateIndexUseStats();
      try {
        lockedQuery(key, operator, results, null/* No Keys to be removed */, context);
        return;
      } finally {
        updateIndexUseEndStats(start);
      }
    }
  }

  public void query(Object key, int operator, Collection results,
      @Retained CompiledValue iterOp, RuntimeIterator indpndntIr,
      ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection)
  throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    // get a read lock when doing a lookup
    if (context.getBucketList() != null
        && (this.region instanceof BucketRegion)) {
      PartitionedRegion pr = ((BucketRegion) region).getPartitionedRegion();
      long start = updateIndexUseStats();
      try {
        for (Object b : context.getBucketList()) {
          AbstractIndex i = PartitionedIndex.getBucketIndex(pr, this.indexName,
              (Integer) b);
          if (i == null) {
            continue;
          }
          i.lockedQuery(key, operator, results, iterOp, indpndntIr, context,
              projAttrib, intermediateResults, isIntersection);
        }
      } finally {
        updateIndexUseEndStats(start);
      }
    } else {
      long start = updateIndexUseStats();
      try {
        lockedQuery(key, operator, results, iterOp, indpndntIr, context,
            projAttrib, intermediateResults, isIntersection);
      } finally {
        updateIndexUseEndStats(start);
      }
    }
    return;
  }
  
  public void query(Object key, int operator, Collection results,
      Set keysToRemove, ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    // get a read lock when doing a lookup
    if (context.getBucketList() != null && (this.region instanceof BucketRegion)) {
        PartitionedRegion pr = ((BucketRegion)region).getPartitionedRegion();
        long start = updateIndexUseStats();
        try {
          for (Object b :context.getBucketList()) {
            AbstractIndex i = PartitionedIndex.getBucketIndex(pr, this.indexName, (Integer)b);
            if (i == null) {
              continue;
            }
            i.lockedQuery(key, operator, results, keysToRemove, context);
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
    return;
  }

  public void query(Collection results, Set keysToRemove, ExecutionContext context)
  throws TypeMismatchException, FunctionDomainException,
  NameResolutionException, QueryInvocationTargetException {
    Iterator itr = keysToRemove.iterator();
    Object temp = itr.next();
    itr.remove();
    if (context.getBucketList() != null && (this.region instanceof BucketRegion)) {
      long start = updateIndexUseStats();
      try {
        PartitionedRegion pr = ((BucketRegion)region).getPartitionedRegion();
        for (Object b :context.getBucketList()) {
          AbstractIndex i = PartitionedIndex.getBucketIndex(pr, this.indexName, (Integer)b);
          if (i == null) {
            continue;
          }
          i.lockedQuery(temp, OQLLexerTokenTypes.TOK_NE, results,
            itr.hasNext() ? keysToRemove : null, context);
        } 
      } finally {
        updateIndexUseEndStats(start);
      }
    } else {
      long start = updateIndexUseStats();
      try {
        lockedQuery(temp, OQLLexerTokenTypes.TOK_NE, results,
            itr.hasNext() ? keysToRemove : null, context);
      } finally {
        updateIndexUseEndStats(start);
      }
    }
    return;
  }

  public void query(Object lowerBoundKey, int lowerBoundOperator,
      Object upperBoundKey, int upperBoundOperator, Collection results,
      Set keysToRemove, ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    if (context.getBucketList() != null) {
      if (this.region instanceof BucketRegion) {
        PartitionedRegion pr = ((BucketRegion)region).getPartitionedRegion();
        long start = updateIndexUseStats();
        try {
          for (Object b :context.getBucketList()) {
            AbstractIndex i = PartitionedIndex.getBucketIndex(pr, this.indexName, (Integer)b);
            if (i == null) {
              continue;
            }
            i.lockedQuery(lowerBoundKey, lowerBoundOperator, upperBoundKey,
                upperBoundOperator, results, keysToRemove, context);
         }
        } finally {
          updateIndexUseEndStats(start);
        }
      }
    } else {
      long start = updateIndexUseStats();
      try {
        lockedQuery(lowerBoundKey, lowerBoundOperator, upperBoundKey,
            upperBoundOperator, results, keysToRemove, context);
      } finally {
        updateIndexUseEndStats(start);
      }
    }
    return;
  }

  
  public List queryEquijoinCondition(IndexProtocol index, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException, QueryInvocationTargetException
  {
    Support
        .assertionFailed(" This function should have never got invoked as its meaningful implementation is present only in RangeIndex class");
    return null;
  }

  /**
   * Get the projectionAttributes for this expression.
   * 
   * @return the projectionAttributes, or "*" if there were none specified at
   *         index creation.
   */
  public String getProjectionAttributes()
  {
    return this.originalProjectionAttributes;
  }

  /**
   * Get the projectionAttributes for this expression.
   * 
   * @return the projectionAttributes, or "*" if there were none specified at
   *         index creation.
   */
  public String getCanonicalizedProjectionAttributes()
  {
    return this.projectionAttributes;
  }

  /**
   * Get the Original indexedExpression for this index.
   */
  public String getIndexedExpression()
  {
    return this.originalIndexedExpression;
  }

  /**
   * Get the Canonicalized indexedExpression for this index.
   */
  public String getCanonicalizedIndexedExpression()
  {
    return this.indexedExpression;
  }

  /**
   * Get the original fromClause for this index.
   */
  public String getFromClause()
  {
    return this.originalFromClause;
  }

  /**
   * Get the canonicalized fromClause for this index.
   */
  public String getCanonicalizedFromClause()
  {
    return this.fromClause;
  }

  public boolean isMapType()
  {
    return false;
  }

  // ////////// IndexProtocol default implementation
  public boolean addIndexMapping(RegionEntry entry) throws IMQException
  {
    this.addMapping(entry);
    return true; // if no exception, then success
  }

  public boolean addAllIndexMappings(Collection c) throws IMQException
  {
    Iterator iterator = c.iterator();
    while (iterator.hasNext()) {
      this.addMapping((RegionEntry)iterator.next());
    }
    return true; // if no exception, then success
  }

  /**
   * @param opCode
   *          one of OTHER_OP, BEFORE_UPDATE_OP, AFTER_UPDATE_OP.
   */
  public boolean removeIndexMapping(RegionEntry entry, int opCode)
      throws IMQException
  {
    removeMapping(entry, opCode);
    return true; // if no exception, then success
  }

  public boolean removeAllIndexMappings(Collection c) throws IMQException
  {
    Iterator iterator = c.iterator();
    while (iterator.hasNext()) {
      removeMapping((RegionEntry)iterator.next(), OTHER_OP);
    }
    return true; // if no exception, then success
  }

  public boolean isValid()
  {
    return isValid;
  }

  public void markValid(boolean b)
  {
    isValid = b;
  }

  public boolean isMatchingWithIndexExpression(CompiledValue indexExpr,
      String conditionExprStr, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException,
      NameResolutionException
  {
    return this.indexedExpression.equals(conditionExprStr);
  }

  private Object verifyAndGetPdxDomainObject(Object value) {
    if (value instanceof StructImpl) {
      // Doing hasPdx check first, since its cheaper.
      if (((StructImpl)value).isHasPdx() && !((GemFireCacheImpl)
          this.region.getCache()).getPdxReadSerializedByAnyGemFireServices()) {
        // Set the pdx values for the struct object.
        StructImpl v = (StructImpl)value;
        Object[] fieldValues = v.getPdxFieldValues();
        return new StructImpl((StructTypeImpl)v.getStructType(), fieldValues);
      }
    } else if (value instanceof PdxInstance && !((GemFireCacheImpl) 
        this.region.getCache()).getPdxReadSerializedByAnyGemFireServices()) {
      return ((PdxInstance)value).getObject();
    }
    return value;
  }

  private void addToResultsWithUnionOrIntersection(Collection results,
      SelectResults intermediateResults, boolean isIntersection, Object value)
  {
    value = verifyAndGetPdxDomainObject(value);
    
    if (intermediateResults == null) {
      results.add(value);
    }
    else {
      if (isIntersection) {
        int numOcc = intermediateResults.occurrences(value);
        if (numOcc > 0) {
          results.add(value);
          intermediateResults.remove(value);
        }
      }
      else {
        // intermediateResults.add(value);
        results.add(value);
      }
    }
  }

  private void addToStructsWithUnionOrIntersection(Collection results,
      SelectResults intermediateResults, boolean isIntersection, Object[] values)
  {
    for (int i=0; i < values.length; i++) {
      values[i] = verifyAndGetPdxDomainObject(values[i]);
    }
    
    if (intermediateResults == null) {
      if( results instanceof StructFields) {
        ((StructFields)results).addFieldValues(values);
      }else {
        //The results could be LinkedStructSet or SortedResultsBag or StructSet
        //LinkedStructSet lss = (LinkedStructSet)results;
        SelectResults sr = (SelectResults)results;
        StructImpl structImpl = new StructImpl( (StructTypeImpl)sr.getCollectionType().getElementType(), values);
        //lss.add(structImpl);
        sr.add(structImpl);
      }
    }
    else {
      if (isIntersection) {
        if(results instanceof StructFields) {
          int numOcc = intermediateResults.occurrences(values);
          if (numOcc > 0) {
            ((StructFields)results).addFieldValues(values);
            ((StructFields)intermediateResults).removeFieldValues(values);
          }
        }else {
          //LinkedStructSet lss = (LinkedStructSet)results;
          // could be LinkedStructSet or SortedResultsBag
          SelectResults sr = (SelectResults)results;
          StructImpl structImpl = new StructImpl( (StructTypeImpl)sr.getCollectionType().getElementType(), values);
          if( intermediateResults.remove(structImpl)) {
            sr.add(structImpl);
          }          
        }
      }
      else {
        if( results instanceof StructFields) {
          ((StructFields)results).addFieldValues(values);
        }else {
          // could be LinkedStructSet or SortedResultsBag
          SelectResults sr = (SelectResults)results;
          //LinkedStructSet lss = (LinkedStructSet)results;
          StructImpl structImpl = new StructImpl( (StructTypeImpl)sr.getCollectionType().getElementType(), values);
          if( ((SelectResults)intermediateResults).remove(structImpl)) {
            sr.add(structImpl);
          }
        }
      }
    }
  }

  void applyProjection(List projAttrib, ExecutionContext context,
      Collection result, Object iterValue, SelectResults intermediateResults,
      boolean isIntersection) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException
  {
    if (projAttrib == null) {
      iterValue = deserializePdxForLocalDistinctQuery(context, iterValue);
      this.addToResultsWithUnionOrIntersection(result, intermediateResults,
          isIntersection, iterValue);
    }
    else {
      //TODO : Asif : Optimize this . This condition looks ugly.
     /* if (result instanceof StructBag || result instanceof LinkedStructSet
          || result instanceof LinkedStructBag) {*/
      boolean isStruct = result instanceof SelectResults 
          && ((SelectResults)result).getCollectionType().getElementType() != null
          && ((SelectResults)result).getCollectionType().getElementType().isStructType();
      if (isStruct) {
        int projCount = projAttrib.size();
        Object[] values = new Object[projCount];
        Iterator projIter = projAttrib.iterator();
        int i = 0;
        while (projIter.hasNext()) {
          Object projDef[] = (Object[])projIter.next();
          values[i] = deserializePdxForLocalDistinctQuery(context, ((CompiledValue)projDef[1]).evaluate(context));
          i++;
        }
        this.addToStructsWithUnionOrIntersection(result, intermediateResults,
            isIntersection, values);
      }
      else {
        Object[] temp = (Object[])projAttrib.get(0);
        Object val = deserializePdxForLocalDistinctQuery(context, ((CompiledValue)temp[1]).evaluate(context));
        this.addToResultsWithUnionOrIntersection(result,
            intermediateResults, isIntersection, val);
      }
    }
  }

  // For local queries with distinct, deserialize all PdxInstances
  // as we do not have a way to compare Pdx and non Pdx objects in case
  // the cache has a mix of pdx and non pdx objects.
  // We still have to honor the cache level readserialized flag in 
  // case of all Pdx objects in cache.
  // Also always convert PdxString to String before adding to resultset
  // for remote queries
  private Object deserializePdxForLocalDistinctQuery(ExecutionContext context,
      Object val) throws QueryInvocationTargetException {
    if (!((DefaultQuery) context.getQuery()).isRemoteQuery()) {
      if (context.isDistinct() && val instanceof PdxInstance
          && !this.region.getCache().getPdxReadSerialized()) {
        try {
          val = ((PdxInstance) val).getObject();
        } catch (Exception ex) {
          throw new QueryInvocationTargetException(
              "Unable to retrieve domain object from PdxInstance while building the ResultSet. "
                  + ex.getMessage());
        }
      } else if (val instanceof PdxString) {
        val = ((PdxString) val).toString();
      }
    }
    return val;
  }
  
  private void removeFromResultsWithUnionOrIntersection(Collection results,
      SelectResults intermediateResults, boolean isIntersection, Object value)
  {
    if (intermediateResults == null) {
      results.remove(value);
    }
    else {
      if (isIntersection) {
        int numOcc = ((SelectResults)results).occurrences(value);
        if (numOcc > 0) {
          results.remove(value);
          intermediateResults.add(value);
        }
      }
      else {
        results.remove(value);
      }
    }
  }

  private void removeFromStructsWithUnionOrIntersection(Collection results,
      SelectResults intermediateResults, boolean isIntersection,
      Object values[], ExecutionContext context)
  {
    if (intermediateResults == null) {      
        ((StructFields)results).removeFieldValues(values);      
    }
    else {
      if (isIntersection) {
        int numOcc = ((SelectResults)results).occurrences(values);
        if (numOcc > 0) {
            ((StructFields)results).removeFieldValues(values);
            ((StructFields)intermediateResults).addFieldValues(values);
          
        }
      }
      else {        
        ((StructFields)results).removeFieldValues(values);        
      }
    }
  }

  void removeProjection(List projAttrib, ExecutionContext context,
      Collection result, Object iterValue, SelectResults intermediateResults,
      boolean isIntersection) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException
  {
    if (projAttrib == null) {
      this.removeFromResultsWithUnionOrIntersection(result,
          intermediateResults, isIntersection, iterValue);
    }
    else {
      if (result instanceof StructFields) {
        int projCount = projAttrib.size();
        Object[] values = new Object[projCount];
        Iterator projIter = projAttrib.iterator();
        int i = 0;
        while (projIter.hasNext()) {
          Object projDef[] = (Object[])projIter.next();
          values[i++] = ((CompiledValue)projDef[1]).evaluate(context);
        }
        this.removeFromStructsWithUnionOrIntersection(result,
            intermediateResults, isIntersection, values, context);
      }
      else {
        Object[] temp = (Object[])projAttrib.get(0);
        Object val = ((CompiledValue)temp[1]).evaluate(context);
        this.removeFromResultsWithUnionOrIntersection(result,
            intermediateResults, isIntersection, val);
      }
    }

  }

  /*
   * This function returns the canonicalized defintions of the from clauses used
   * in Index creation TODO:Asif :How to make it final so that it is immutable
   */
  public String[] getCanonicalizedIteratorDefinitions()
  {
    return this.canonicalizedDefinitions;
  }

  // Asif : This implementation is for PrimaryKeyIndex. RangeIndex has its
  // own implementation. For PrimaryKeyIndex , this method should not be used
  // TODO: Asif : Check if an Exception should be thrown if the function
  // implementation of this class gets invoked
  public boolean containsEntry(RegionEntry entry)
  {
    return false;
  }

  void instantiateEvaluator(IndexCreationHelper ich)
  {
  }

  public void initializeIndex(boolean loadEntries) throws IMQException
  {
  }
  
  
  @Override
  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("Index [");
    sb.append(" Name=").append(getName());
    sb.append(" Type =").append(getType());
    sb.append(" IdxExp=").append(getIndexedExpression());
    sb.append(" From=").append(getFromClause());
    sb.append(" Proj=").append(getProjectionAttributes());
    sb.append("]");
    return sb.toString();
  }

  public abstract boolean isEmpty();

  protected abstract boolean isCompactRangeIndex();
  
  protected abstract InternalIndexStatistics createStats(String indexName);

  public abstract ObjectType getResultSetType();

  abstract void recreateIndexData() throws IMQException;

  abstract void addMapping(RegionEntry entry) throws IMQException;

  abstract void removeMapping(RegionEntry entry, int opCode)
      throws IMQException;

  abstract void addMapping(Object key, Object value, RegionEntry entry)
      throws IMQException;

  /**
   * Shobhit: This is used to buffer the index entries evaluated from a
   * RegionEntry which is getting updated at present. These buffered index
   * entries are replaced into the index later all together to avoid
   * remove-add sequence.
   * @param key
   * @param value
   * @param entry
   * @throws IMQException
   */
  abstract void saveMapping(Object key, Object value, RegionEntry entry)
  throws IMQException;

  /** Lookup method used when appropriate lock is held */
  abstract void lockedQuery(Object key, int operator, Collection results,
      CompiledValue iterOps, RuntimeIterator indpndntItr,
      ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection)
      throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException;

  abstract void lockedQuery(Object lowerBoundKey, int lowerBoundOperator,
      Object upperBoundKey, int upperBoundOperator, Collection results,
      Set keysToRemove, ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException;

  abstract void lockedQuery(Object key, int operator, Collection results,
      Set keysToRemove, ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException;
  
  public Index getPRIndex() {
    return prIndex;
  }

  public void setPRIndex(Index parIndex) {
    this.prIndex = parIndex;
  }


  /**
   * Dummy implementation that subclasses can override.
   */
  protected static abstract class InternalIndexStatistics implements
      IndexStatistics
  {
    public long getNumUpdates()
    {
      return 0L;
    }

    public long getTotalUpdateTime()
    {
      return 0L;
    }

    public long getTotalUses()
    {
      return 0L;
    }

    public long getNumberOfKeys()
    {
      return 0L;
    }

    public long getNumberOfValues()
    {
      return 0L;
    }

    public long getNumberOfValues(Object key)
    {
      return 0L;
    }

    public long getUpdateTime() {
      return 0L;
    }
    
    public long getUseTime() {
      return 0L;
    }
   
    public int getReadLockCount() {
      return 0;
    }
    
    public long getNumberOfMapIndexKeys() {
      return 0;
    }

    public int getNumberOfBucketIndexes(){
      return 0;
    }
    
    public void close()
    {
    }

    public void incNumValues(int delta)
    {
    }

    public void incNumUpdates()
    {
    }

    public void incNumUpdates(int delta)
    {
    }

    public void incUpdatesInProgress(int delta)
    {
    }

    public void incUsesInProgress(int delta)
    {
    }

    public void updateNumKeys(long count)
    {
    }

    public void incNumKeys(long count)
    {
    }

    public void incNumMapIndexKeys(long numKeys) {
    }

    public void incUpdateTime(long delta)
    {
    }

    public void incNumUses()
    {
    }

    public void incUseTime(long delta)
    {
    }

    public void incReadLockCount(int delta) 
    {
    }
    
    public void incNumBucketIndexes(int delta) 
    {
    }
  }

  /**
   * 
   * @author vaibhav
   * @author Asif
   */
  class IMQEvaluator implements IndexedExpressionEvaluator
  {
    private Cache cache;

    private List fromIterators = null;

    private CompiledValue indexedExpr = null;

    final private String[] canonicalIterNames;

    private ObjectType indexResultSetType = null;

    private Map dependencyGraph = null;

    /*
     * Asif : The boolean if true indicates that the 0th iterator is on entries
     * . If the 0th iterator is on collection of Region.Entry objects, then the
     * RegionEntry object used in Index data objects is obtained directly from
     * its corresponding Region.Entry object. However if the 0th iterator is not
     * on entries then the boolean is false. In this case the additional
     * projection attribute gives us the original value of the iterator while
     * the Region.Entry object is obtained from 0th iterator. It is possible to
     * have index being created on a Region Entry itself , instead of a Region.
     * A Map operator( Compiled Index Operator) used with Region enables, us to
     * create such indexes. In such case the 0th iterator, even if it represents
     * a collection of Objects which are not Region.Entry objects, still the
     * boolean remains true, as the Entry object can be easily obtained from the
     * 0th iterator. In this case, the additional projection attribute s not
     * null as it is used to evaluate the Entry object from the 0th iterator.
     */
    private boolean isFirstItrOnEntry = false;

    //Shobhit: The boolean if true indicates that the 0th iterator is on keys.
    private boolean isFirstItrOnKey = false;

    // Asif: List of modified iterators, not null only when the booelan
    // isFirstItrOnEntry is false.
    private List indexInitIterators = null;

    // Asif : The additional Projection attribute representing the value of the
    // original 0th iterator.
    // If the isFirstItrOnEntry is false, then it is not null. However if the
    // isFirstItrOnEntry is
    // true but & still this attribute is not null, this indicates that the 0th
    // iterator
    // is derived using an individual entry thru Map operator on the Region.
    private CompiledValue additionalProj = null;

    // Asif : This is not null iff the boolean isFirstItrOnEntry is false.
    private CompiledValue modifiedIndexExpr = null;

    private ObjectType addnlProjType = null;

    private int initEntriesUpdated = 0;

    private boolean hasInitOccuredOnce = false;

    private ExecutionContext initContext = null;

    private int iteratorSize = -1;

    private Region rgn = null;

    /** Creates a new instance of IMQEvaluator */
    IMQEvaluator(IndexCreationHelper helper) {
      this.cache = helper.getCache();
      this.fromIterators = helper.getIterators();
      this.indexedExpr = helper.getCompiledIndexedExpression();
      this.rgn  = helper.getRegion();
      // Asif : The modified iterators for optmizing Index cxreation
      isFirstItrOnEntry = ((FunctionalIndexCreationHelper)helper).isFirstIteratorRegionEntry;
      isFirstItrOnKey = ((FunctionalIndexCreationHelper)helper).isFirstIteratorRegionKey;
      additionalProj = ((FunctionalIndexCreationHelper)helper).additionalProj;
      Object params1[] = { new QRegion(rgn, false) };
      initContext = new ExecutionContext(params1, cache);
      this.canonicalIterNames = ((FunctionalIndexCreationHelper)helper).canonicalizedIteratorNames;
      if (isFirstItrOnEntry) {
        this.indexInitIterators = this.fromIterators;
      }
      else {
        this.indexInitIterators = ((FunctionalIndexCreationHelper)helper).indexInitIterators;
        modifiedIndexExpr = ((FunctionalIndexCreationHelper)helper).modifiedIndexExpr;
        addnlProjType = ((FunctionalIndexCreationHelper)helper).addnlProjType;
      }
      this.iteratorSize = this.indexInitIterators.size();

    }

    public String getIndexedExpression()
    {
      return AbstractIndex.this.getCanonicalizedIndexedExpression();
    }

    public String getProjectionAttributes()
    {
      return AbstractIndex.this.getCanonicalizedProjectionAttributes();
    }

    public String getFromClause()
    {
      return AbstractIndex.this.getCanonicalizedFromClause();
    }
    
    public void expansion(List expandedResults, Object lowerBoundKey, Object upperBoundKey, int lowerBoundOperator, int upperBoundOperator, Object value) throws IMQException {
      //no-op
    }

    public void evaluate(RegionEntry target, boolean add) throws IMQException
    {
      assert add; // ignored, but should be true here
      DummyQRegion dQRegion = new DummyQRegion(rgn);
      dQRegion.setEntry(target);
      Object params[] = { dQRegion };
      ExecutionContext context = new ExecutionContext(params, this.cache);
      context.newScope(IndexCreationHelper.INDEX_QUERY_SCOPE_ID);
      try {
        boolean computeDependency = true;
        if (dependencyGraph != null) {
          context.setDependencyGraph(dependencyGraph);
          computeDependency = false;
        }
        
        for (int i = 0; i < this.iteratorSize; i++) {
          CompiledIteratorDef iterDef = (CompiledIteratorDef) fromIterators.get(i);
          // Asif: Compute the dependency only once. The call to methods of this
          // class are thread safe as for update lock on Index is taken .
          if (computeDependency) {
            iterDef.computeDependencies(context);
          }
          RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
          context.addToIndependentRuntimeItrMapForIndexCreation(iterDef);
          context.bindIterator(rIter);
        }
        // Save the dependency graph for future updates.
        if (dependencyGraph == null) {
          dependencyGraph = context.getDependencyGraph();
        }
        
        Support
            .Assert(
                this.indexResultSetType != null,
                "IMQEvaluator::evaluate:The StrcutType should have been initialized during index creation");

        doNestedIterations(0, context);
      }
      catch (IMQException imqe) {
        throw imqe;
      }
      catch (Exception e) {
        throw new IMQException(e);
      }
      finally {
        context.popScope();
      }
    }

    /**
     * Asif : This function is used for creating Index data at the start
     * 
     */
    public void initializeIndex(boolean loadEntries) throws IMQException
    {
      this.initEntriesUpdated = 0;
      try {
        // Asif: Since an index initialization can happen multiple times
        // for a given region, due to clear operation, we are using harcoded
        // scope ID of 1 , as otherwise if obtained from ExecutionContext
        // object,
        // it will get incremented on very index initialization
        this.initContext.newScope(1);
        for (int i = 0; i < this.iteratorSize; i++) {
          CompiledIteratorDef iterDef = (CompiledIteratorDef)this.indexInitIterators.get(i);
          RuntimeIterator rIter = null;
          if (!this.hasInitOccuredOnce) {
            iterDef.computeDependencies(this.initContext);
            rIter = iterDef.getRuntimeIterator(this.initContext);
            this.initContext.addToIndependentRuntimeItrMapForIndexCreation(iterDef);
          }
          if (rIter == null) {
            rIter = iterDef.getRuntimeIterator(this.initContext);
          }
          this.initContext.bindIterator(rIter);
        }
        this.hasInitOccuredOnce = true;
        if (this.indexResultSetType == null) {
          this.indexResultSetType = createIndexResultSetType();
        }
        if(loadEntries) {
          doNestedIterationsForIndexInit(0, this.initContext
            .getCurrentIterators());
        }
      }
      catch (IMQException imqe) {
        throw imqe;
      }
      catch (Exception e) {
        throw new IMQException(e);
      }
      finally {
        this.initContext.popScope();
      }
    }

    private void doNestedIterationsForIndexInit(int level, List runtimeIterators)
        throws TypeMismatchException, AmbiguousNameException,
        FunctionDomainException, NameResolutionException,
        QueryInvocationTargetException, IMQException
    {
      if (level == 1) {
        ++this.initEntriesUpdated;
      }
      if (level == this.iteratorSize) {
        applyProjectionForIndexInit(runtimeIterators);
      }
      else {
        RuntimeIterator rIter = (RuntimeIterator)runtimeIterators.get(level);
        // System.out.println("Level = "+level+" Iter = "+rIter.getDef());
        Collection c = rIter.evaluateCollection(this.initContext);
        if (c == null)
          return;
        Iterator cIter = c.iterator();
        while (cIter.hasNext()) {
          rIter.setCurrent(cIter.next());
          doNestedIterationsForIndexInit(level + 1, runtimeIterators);
        }
      }
    }

    /*
     * Asif : This function is used to obtain Indxe data at the time of index
     * creation. Each element of the List is an Object Array of size 3. The 0th
     * element of Object Array stores the value of Index Expression. The 1st
     * element of ObjectArray contains the RegionEntry object ( If the booelan
     * isFirstItrOnEntry is false, then the 0th iterator will give us the
     * Region.Entry object which can be used to obtain the underlying
     * RegionEntry object. If the boolean is true & additional projection
     * attribute is not null, then the Region.Entry object can be obtained by
     * evaluating the additional projection attribute. If the boolean
     * isFirstItrOnEntry is tru e& additional projection attribute is null, then
     * teh 0th iterator itself will evaluate to Region.Entry Object.
     * 
     * The 2nd element of Object Array contains the Struct object ( tuple)
     * created. If the boolean isFirstItrOnEntry is false, then the first
     * attribute of the Struct object is obtained by evaluating the additional
     * projection attribute.
     */
    private void applyProjectionForIndexInit(List currrentRuntimeIters)
        throws FunctionDomainException, TypeMismatchException,
        NameResolutionException, QueryInvocationTargetException, IMQException
    {
      if (QueryMonitor.isLowMemory()) {
        throw new IMQException(LocalizedStrings.IndexCreationMsg_CANCELED_DUE_TO_LOW_MEMORY.toLocalizedString());
      }
      
      LocalRegion.NonTXEntry temp = null;

      //Evaluate NonTXEntry for index on entries or additional projections
      //on Entry or just entry value.
      if (this.isFirstItrOnEntry && this.additionalProj != null) {
        temp = (LocalRegion.NonTXEntry)additionalProj
            .evaluate(this.initContext);
      }
      else {
        temp = (LocalRegion.NonTXEntry)(((RuntimeIterator)currrentRuntimeIters
            .get(0)).evaluate(this.initContext));
      }

      RegionEntry re = temp.getRegionEntry();
      Object indxResultSet = null;

      // Object tuple[] ;
      if (this.iteratorSize == 1) {
        indxResultSet = this.isFirstItrOnEntry ? ((this.additionalProj == null) ? temp
            : ((RuntimeIterator)currrentRuntimeIters.get(0))
                .evaluate(this.initContext))
            : additionalProj.evaluate(this.initContext);
      }
      else {
        Object[] tuple = new Object[this.iteratorSize];
        int i = (this.isFirstItrOnEntry) ? 0 : 1;
        for (; i < this.iteratorSize; i++) {
          RuntimeIterator iter = (RuntimeIterator)currrentRuntimeIters.get(i);
          tuple[i] = iter.evaluate(this.initContext);
        }
        if (!this.isFirstItrOnEntry)
          tuple[0] = additionalProj.evaluate(this.initContext);
        Support
            .Assert(
                this.indexResultSetType instanceof StructTypeImpl,
                "The Index ResultType should have been an instance of StructTypeImpl rather than ObjectTypeImpl. The indxeResultType is "
                    + this.indexResultSetType);
        indxResultSet = new StructImpl(
            (StructTypeImpl)this.indexResultSetType, tuple);
      }

      //Key must be evaluated after indexResultSet evaluation is done as Entry might be getting destroyed
      //and so if value is UNDEFINED, key will definitely will be UNDEFINED.
      Object indexKey = this.isFirstItrOnEntry ? this.indexedExpr
          .evaluate(this.initContext) : modifiedIndexExpr
          .evaluate(this.initContext);
      //based on the first key convert the rest to PdxString or String
      if(!isIndexedPdxKeysFlagSet){
        setPdxStringFlag(indexKey);
      }
      indexKey = getPdxStringForIndexedPdxKeys(indexKey);
      addMapping(indexKey, indxResultSet, re);
    }

    // TODO:Asif : This appears to be incorrect.
    private void doNestedIterations(int level, ExecutionContext context) throws TypeMismatchException,
        AmbiguousNameException, FunctionDomainException,
        NameResolutionException, QueryInvocationTargetException, IMQException
    {
      List iterList = context.getCurrentIterators();
      if (level == this.iteratorSize) {
        applyProjection(context);
      }
      else {
        RuntimeIterator rIter = (RuntimeIterator)iterList.get(level);
        // System.out.println("Level = "+level+" Iter = "+rIter.getDef());
        Collection c = rIter.evaluateCollection(context);
        if (c == null)
          return;
        Iterator cIter = c.iterator();
        while (cIter.hasNext()) {
          rIter.setCurrent(cIter.next());
          doNestedIterations(level + 1, context);
        }
      }
    }

    private void applyProjection(ExecutionContext context) throws FunctionDomainException,
        TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException, IMQException
    {
      List currrentRuntimeIters = context.getCurrentIterators();
      Object indxResultSet = null;
      // int size = currrentRuntimeIters.size();
      Object indexKey = indexedExpr.evaluate(context);
      //based on the first key convert the rest to PdxString or String
      if(!isIndexedPdxKeysFlagSet){
        setPdxStringFlag(indexKey);
      }
      indexKey = getPdxStringForIndexedPdxKeys(indexKey);
      if (this.iteratorSize == 1) {
        RuntimeIterator iter = (RuntimeIterator)currrentRuntimeIters.get(0);
        indxResultSet = iter.evaluate(context);
      }
      else {
        Object tuple[] = new Object[this.iteratorSize];
        for (int i = 0; i < this.iteratorSize; i++) {
          RuntimeIterator iter = (RuntimeIterator)currrentRuntimeIters.get(i);
          tuple[i] = iter.evaluate(context);
        }
        Support
            .Assert(
                this.indexResultSetType instanceof StructTypeImpl,
                "The Index ResultType should have been an instance of StructTypeImpl rather than ObjectTypeImpl. The indxeResultType is "
                    + this.indexResultSetType);
        indxResultSet = new StructImpl(
            (StructTypeImpl)this.indexResultSetType, tuple);
      }

      //Keep Entry value in fly untill all keys are evaluated
      RegionEntry entry = ((DummyQRegion) context.getBindArgument(1)).getEntry();
      saveMapping(indexKey, indxResultSet, entry);
    }

    // TODO :Asif: Test this function .
    // The struct type calculation is modified if the
    // 0th iterator is modified to make it dependent on Entry
    private ObjectType createIndexResultSetType()
    {
      List currentIterators = this.initContext.getCurrentIterators();
      int len = currentIterators.size();
      ObjectType type = null;
      // String fieldNames[] = new String[len];
      ObjectType fieldTypes[] = new ObjectType[len];
      int start = this.isFirstItrOnEntry ? 0 : 1;
      for (; start < len; start++) {
        RuntimeIterator iter = (RuntimeIterator)currentIterators.get(start);
        // fieldNames[start] = iter.getInternalId();
        fieldTypes[start] = iter.getElementType();
      }
      if (!this.isFirstItrOnEntry) {
        // fieldNames[0] = "iter1";
        fieldTypes[0] = addnlProjType;
      }
      type = (len == 1) ? fieldTypes[0] : new StructTypeImpl(
          this.canonicalIterNames, fieldTypes);
      return type;
    }

    private void printList(List list)
    {
      System.out.println("results.size = " + list.size());
      for (int i = 0; i < list.size(); i++) {
        Object arr[] = (Object[])list.get(i);
        System.out.println("Key = " + arr[0]);
        System.out.println("Value =" + arr[1]);
      }
    }

    int getTotalEntriesUpdated()
    {
      return this.initEntriesUpdated;
    }

    public ObjectType getIndexResultSetType()
    {
      return this.indexResultSetType;
    }

    public boolean isFirstItrOnEntry() {
      return isFirstItrOnEntry;
    }

    public boolean isFirstItrOnKey() {
      return isFirstItrOnKey;
    }

    @Override
    public List getAllDependentIterators() {
      return fromIterators;
    }
  }

  /**
   * Checks the limit for the resultset for distinct and non-distinct
   * queries separately. In case of non-distinct distinct elements size
   * of result-set is matched against limit passed in as an argument.
   * 
   * @param result
   * @param limit
   * @param context
   * @return true if limit is satisfied.
   */
  protected boolean verifyLimit(Collection result, int limit,
      ExecutionContext context) {   
    if (limit > 0) {
     /* if (!context.isDistinct()) {
        return ((Bag)result).size() == limit;
      } else if (result.size() == limit) {
        return true;
      }*/
      return result.size() == limit;
    }
    return false;
  }
  
  /**
   * This will verify the consistency between RegionEntry and IndexEntry.
   * RangeIndex has following entry structure,
   * 
   *  IndexKey --> [RegionEntry, [Iterator1, Iterator2....., IteratorN]]
   *
   * Where Iterator1 to IteratorN are iterators defined in index from clause.
   *
   * For example: "/portfolio p, p.positions.values pos" from clause has two
   * iterators where p is independent iterator and pos is dependent iterator.
   * 
   * Query iterators can be a subset, superset or exact match of index iterators.
   * But we take query iterators which are matching with index iterators to evaluate
   * RegionEntry for new value and compare it with index value which could be
   * a plain object or a Struct. 
   *
   * Note: Struct evaluated from RegionEntry can NOT have more field values than
   * Index Value Struct as we filter out iterators in query context before evaluating
   * Struct from RegionEntry.
   * @param re
   * @param value
   * @param context 
   * @return True if Region and Index entries are consistent.
   */
  protected boolean verifyEntryAndIndexVaue(RegionEntry re, Object value, ExecutionContext context) {
    IMQEvaluator evaluator = (IMQEvaluator)getEvaluator();
    List valuesInRegion = null;
    Object valueInIndex = null;

    try {
      // In a RegionEntry key and Entry itself can not be modified else
      // RegionEntry itself will change. So no need to verify anything just return
      // true.
      if (evaluator.isFirstItrOnKey()) {
        return true;
      } else if (evaluator.isFirstItrOnEntry()) {
        valuesInRegion = evaluateIndexIteratorsFromRE(re, context);
        valueInIndex = verifyAndGetPdxDomainObject(value);
      } else{
        @Released Object val = re.getValueInVM(context.getPartitionedRegion());
        Chunk valToFree = null;
        if (val instanceof Chunk) {
          valToFree = (Chunk)val;
        }
        try {
        if (val instanceof CachedDeserializable) {
          val = ((CachedDeserializable)val).getDeserializedValue(getRegion(), re);
        }
        val = verifyAndGetPdxDomainObject(val);   
        valueInIndex = verifyAndGetPdxDomainObject(value);
        valuesInRegion = evaluateIndexIteratorsFromRE(val, context);
        } finally {
          if (valToFree != null) {
            valToFree.release();
          }
        }
      }
    } catch (Exception e) {
      // TODO: Create a new LocalizedString for this.
      if (logger.isDebugEnabled()) {
        logger.debug("Exception occured while verifying a Region Entry value during a Query when the Region Entry is under update operation", e);
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
   * This method compares two objects in which one could be StructType and
   * other ObjectType.
   * Fur conditions are possible,
   * Object1 -> Struct  Object2-> Struct
   * Object1 -> Struct  Object2-> Object
   * Object1 -> Object  Object2-> Struct
   * Object1 -> Object  Object2-> Object
   *
   * @param valueInRegion
   * @param valueInIndex
   * @return true if valueInRegion's all objects are part of valueInIndex. 
   */
  private boolean compareStructWithNonStruct(Object valueInRegion,
      Object valueInIndex) {
    if (valueInRegion instanceof Struct && valueInIndex instanceof Struct) {
      Object[] regFields = ((StructImpl) valueInRegion).getFieldValues();
      List indFields = Arrays.asList(((StructImpl) valueInIndex).getFieldValues());
      for (Object regField : regFields) {
        if (!indFields.contains(regField)) {
          return false;
        }
      }
      return true;
    } else if (valueInRegion instanceof Struct && !(valueInIndex instanceof Struct)) {
      Object[] fields = ((StructImpl) valueInRegion).getFieldValues();
      for (Object field : fields) {
        if (field.equals(valueInIndex)) {
          return true;
        }
      }
    } else if (!(valueInRegion instanceof Struct) && valueInIndex instanceof Struct) {
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
   * Returns evaluated collection for dependent runtime iterator for this index
   * from clause and given RegionEntry.
   *
   * @param context passed here is query context.
   * @return Evaluated second level collection.
   * @throws QueryInvocationTargetException 
   * @throws NameResolutionException 
   * @throws TypeMismatchException 
   * @throws FunctionDomainException 
   */
  protected List evaluateIndexIteratorsFromRE(Object value, ExecutionContext context)
      throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    //We need NonTxEntry to call getValue() on it. RegionEntry does
    //NOT have public getValue() method.
    if (value instanceof RegionEntry) {
      value = ((LocalRegion) this.getRegion()).new NonTXEntry(
          (RegionEntry) value);
    }
    // Get all Independent and dependent iterators for this Index.
    List itrs = getAllDependentRuntimeIterators(context);

    List values = evaluateLastColl(value, context, itrs, 0);
    return values;
  }

  private List evaluateLastColl(Object value, ExecutionContext context,
      List itrs, int level)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // A tuple is a value generated from RegionEntry value which could be
    // a StructType (Multiple Dependent Iterators) or ObjectType (Single
    // Iterator) value.
    List tuples = new ArrayList(1);
    
    RuntimeIterator currItrator = (RuntimeIterator) itrs.get(level);
    currItrator.setCurrent(value);
    // If its last iterator then just evaluate final struct.
    if ((itrs.size() - 1) == level) {
      if (itrs.size() > 1) {
        Object tuple[] = new Object[itrs.size()];
        for (int i = 0; i < itrs.size(); i++) {
          RuntimeIterator iter = (RuntimeIterator)itrs.get(i);
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

      // If value is null or INVALID then the evaluated collection would be
      // Null.
      if (nextLevelValues != null) {
        for (Object nextLevelValue : nextLevelValues) {
          tuples.addAll(evaluateLastColl(nextLevelValue, context,
              itrs, level + 1));
        }
      }
    }
    return tuples;
  }

  /**
   * Matches the Collection reference in given context for this index's
   * from-clause in all current independent collection references associated to
   * the context. For example, if a join Query has "/region1 p, region2 e" from clause
   * context contains two region references for p and e and Index could be used for
   * any of those of both of those regions.
   *
   * Note: This Index contains its own from clause definition which corresponds to
   * a region collection reference in given context and must be contained at 0th index
   * in {@link AbstractIndex#canonicalizedDefinitions}.  
   * 
   * @param context
   * @return {@link RuntimeIterator} this should not be null ever.
   */
  public RuntimeIterator getRuntimeIteratorForThisIndex(ExecutionContext context) {
    List<RuntimeIterator> indItrs = context.getCurrentIterators();
    Region rgn  = this.getRegion();
    if (rgn instanceof BucketRegion) {
      rgn = ((BucketRegion)rgn).getPartitionedRegion();
    }
    String regionPath = rgn.getFullPath();
    String definition = this.getCanonicalizedIteratorDefinitions()[0];
    for (RuntimeIterator itr: indItrs) {
      //GemFireCacheImpl.getInstance().getLogger().fine("Shobhit: "+ itr.getDefinition() + "  "+ this.getRegion().getFullPath());
      if (itr.getDefinition().equals(regionPath) || itr.getDefinition().equals(definition)) {
        return itr;
      }
    }
    return null;
  }
  
  /**
   * Similar to {@link #getRuntimeIteratorForThisIndex(ExecutionContext)} except
   * that this one also matches the iterator name if present with alias used
   * in the {@link IndexInfo}
   * 
   * @param context
   * @param info
   * @return {@link RuntimeIterator}
   */
  public RuntimeIterator getRuntimeIteratorForThisIndex(
      ExecutionContext context, IndexInfo info) {
    List<RuntimeIterator> indItrs = context.getCurrentIterators();
    Region rgn = this.getRegion();
    if (rgn instanceof BucketRegion) {
      rgn = ((BucketRegion) rgn).getPartitionedRegion();
    }
    String regionPath = rgn.getFullPath();
    String definition = this.getCanonicalizedIteratorDefinitions()[0];
    for (RuntimeIterator itr : indItrs) {
      if ((itr.getDefinition().equals(regionPath) || itr.getDefinition()
          .equals(definition))) {
        // if iterator has name alias must be used in the query
        if(itr.getName() != null){
          CompiledValue path = info._path();
          // match the iterator name with alias
          String pathName = getReceiverNameFromPath(path);
          if(path.getType() == OQLLexerTokenTypes.Identifier
              || itr.getName().equals(pathName)) {
            return itr;
          }
        } else{
          return itr;
        }
      }
    }
    return null;
  }
  
  private String getReceiverNameFromPath(CompiledValue path) {
    if (path instanceof CompiledID) {
      return ((CompiledID) path).getId();
    }
    else if (path instanceof CompiledPath) {
      return getReceiverNameFromPath(path.getReceiver());
    }
    else if (path instanceof CompiledIndexOperation) {
      return getReceiverNameFromPath(((CompiledIndexOperation)path).getReceiver());
    }
    return "";
  }

  /**
   * Take all independent iterators from context and remove the one which
   * matches for this Index's independent iterator. Then get all Dependent
   * iterators from given context for this Index's independent iterator. 
   *
   * @param context from executing query.
   * @return List of all iterators pertaining to this Index.
   */
  public List getAllDependentRuntimeIterators(ExecutionContext context) {
    List<RuntimeIterator> indItrs = context
        .getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(getRuntimeIteratorForThisIndex(context));

    List<String> definitions = Arrays.asList(this.getCanonicalizedIteratorDefinitions());
    // These are the common iterators between query from clause and index from clause.
    ArrayList itrs = new ArrayList();

    for (RuntimeIterator itr : indItrs) {
      if (definitions.contains(itr.getDefinition())) {
        itrs.add(itr);
      }
    }
    return itrs;
  }

  /**
   * This map is not thread-safe. We rely on the fact that every thread which is
   * trying to update this kind of map (In Indexes), must have RegionEntry lock
   * before adding OR removing elements.
   * 
   * This map does NOT provide an iterator. To iterate over its element caller
   * has to get inside the map itself through addValuesToCollection() calls.
   * 
   * @author shobhit
   *
   */
  class RegionEntryToValuesMap
  {
    protected Map map;
    private boolean useList;
    private AtomicInteger numValues = new AtomicInteger(0);

    RegionEntryToValuesMap(boolean useList) {
      this.map = new ConcurrentHashMap(2, 0.75f, 1);
      this.useList = useList; 
    }

    RegionEntryToValuesMap(Map map, boolean useList) {
      this.map = map;
      this.useList = useList;
    }

    /**
     * We do NOT use any locks here as every add is for a RegionEntry
     * which is locked before coming here. No two threads can be
     * entering in this method together for a RegionEntry.
     * 
     * @param entry
     * @param value
     */
    public void add(RegionEntry entry, Object value)
    {
      assert value != null;
      // Values must NOT be null and ConcurrentHashMap does not
      // support null values.
      if (value == null) {
        return;
      }
      Object object = map.get(entry);
      if (object == null) {
        map.put(entry, value);
      } else if (object instanceof Collection) {
        Collection coll = (Collection) object;
        // If its a list query might get ConcurrentModificationException.
        // This can only happen for Null mapped or Undefined entries in a
        // RangeIndex. So we are synchronizing on ArrayList.
        if (useList) {
          synchronized (coll) {
            coll.add(value); 
          }
        } else {
          coll.add(value);
        }
      } else {
        Collection coll = useList?new ArrayList(2):new IndexConcurrentHashSet(2, 0.75f, 1);
        coll.add(object);
        coll.add(value);
        map.put(entry, coll);
      }
      numValues.incrementAndGet();
    }

    public void addAll(RegionEntry entry, Collection values)
    {
      Object object = map.get(entry);
      if (object == null) {
        Collection coll = useList?new ArrayList(values.size()):new IndexConcurrentHashSet(values.size(), 0.75f, 1);
        coll.addAll(values);
        map.put(entry, coll);
        numValues.addAndGet(values.size());
      } else if (object instanceof Collection) {
        Collection coll = (Collection) object;
        // If its a list query might get ConcurrentModificationException.
        // This can only happen for Null mapped or Undefined entries in a
        // RangeIndex. So we are synchronizing on ArrayList.
        if (useList) {
          synchronized (coll) {
            coll.addAll(values);
          }
        } else {
          coll.addAll(values);
        }
      } else {
        Collection coll = useList?new ArrayList(values.size() + 1):new IndexConcurrentHashSet(values.size()+1, 0.75f, 1);
        coll.addAll(values);
        coll.add(object);
        map.put(entry, coll);
      }
      numValues.addAndGet(values.size());
    }

    public Object get(RegionEntry entry)
    {
      return map.get(entry);
    }

    /**
     * We do NOT use any locks here as every remove is for a RegionEntry
     * which is locked before coming here. No two threads can be
     * entering in this method together for a RegionEntry.
     *
     * @param entry
     * @param value
     */
    public void remove(RegionEntry entry, Object value)
    {
      Object object = map.get(entry);
      if (object == null)
        return;
      if (object instanceof Collection) {
        Collection coll= (Collection)object;
        boolean removed = false;
        // If its a list query might get ConcurrentModificationException.
        // This can only happen for Null mapped or Undefined entries in a
        // RangeIndex. So we are synchronizing on ArrayList.
        if (useList) {
          synchronized (coll) {
            removed = coll.remove(value);
          }
        } else {
          removed = coll.remove(value);
        }
        if (removed) {
          if (coll.size() == 0) {
            map.remove(entry);
          }
          numValues.decrementAndGet();
        }
      }
      else {
        if (object.equals(value)) {
          map.remove(entry);
        }
        this.numValues.decrementAndGet();
      }
    }

    public Object remove(RegionEntry entry)
    {
      Object retVal = map.remove(entry);
      if (retVal != null) {
            numValues.addAndGet((retVal instanceof Collection) ?
              - ((Collection) retVal).size() : -1 );
      }
      return retVal;
    }

    public int getNumValues(RegionEntry entry)
    {
      Object object = map.get(entry);
      if (object == null)
        return 0;
      if (object instanceof Collection) {
        Collection coll = (Collection) object;
        return coll.size();
      } else {
        return 1;
      }
    }

    public int getNumValues()
    {
      return this.numValues.get();
    }

    public int getNumEntries()
    {
      return map.keySet().size();
    }

    public void addValuesToCollection(Collection result, int limit, ExecutionContext context )
    {

      Iterator entriesIter = map.entrySet().iterator();
      while (entriesIter.hasNext()) {
        // Check if query execution on this thread is canceled.
        QueryMonitor.isQueryExecutionCanceled();
        if (this.verifylimit(result, limit, context)) {
          return;
        }
        Map.Entry e = (Map.Entry)entriesIter.next();
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
              Iterator itr = ((Collection) value).iterator();
              while (itr.hasNext()) {
                Object val = itr.next();
                //Shobhit: Compare the value in index with in RegionEntry.
                if (!reUpdateInProgress || verifyEntryAndIndexVaue(re, val, context)) {
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
            Iterator itr = ((Collection) value).iterator();
            while (itr.hasNext()) {
              Object val = itr.next();
              //Shobhit: Compare the value in index with in RegionEntry.
              if (!reUpdateInProgress || verifyEntryAndIndexVaue(re, val, context)) {
                result.add(val);
              }
              if (limit != -1) {
                if (this.verifylimit(result, limit, context)) {
                  return;
                }
              }
            }
          }
        }
        else {
          if (!reUpdateInProgress
              || verifyEntryAndIndexVaue(re, value, context)) {
            if (context.isCqQueryContext()) {
              result
                  .add(new CqEntry(((RegionEntry) e.getKey()).getKey(), value));
            } else {
              result.add(verifyAndGetPdxDomainObject(value));
            }
          }
        }
      }
    }

    public void addValuesToCollection(Collection result, CompiledValue iterOp,
        RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
        SelectResults intermediateResults, boolean isIntersection, int limit)
        throws FunctionDomainException, TypeMismatchException,
        NameResolutionException, QueryInvocationTargetException
    {
      if (this.verifylimit(result, limit, context)) {
        return;
      }
      // Iterator valuesIter = map.values().iterator();
      Iterator entries = map.entrySet().iterator();
      while (entries.hasNext()) {
        // Check if query execution on this thread is canceled.
        QueryMonitor.isQueryExecutionCanceled();
        Map.Entry e = (Map.Entry)entries.next();
        Object value = e.getValue();
        //Key is a RegionEntry here.
        RegionEntry entry = (RegionEntry)e.getKey();
        boolean reUpdateInProgress = false;
        if (value != null) {
          if (entry.isUpdateInProgress()) {
            reUpdateInProgress = true;
          }
          if (value instanceof Collection) {
            // If its a list query might get ConcurrentModificationException.
            // This can only happen for Null mapped or Undefined entries in a
            // RangeIndex. So we are synchronizing on ArrayList.
            if (this.useList) {
              synchronized (value) {
                Iterator itr = ((Collection)value).iterator();
                while (itr.hasNext()) {
                  boolean ok = true;
                  Object val = itr.next();
                  if (reUpdateInProgress) {
                    //Shobhit: Compare the value in index with value in RegionEntry.
                    ok = verifyEntryAndIndexVaue(entry, val, context);
                  }
                  if (ok && runtimeItr != null) {
                    runtimeItr.setCurrent(val);
                    ok = QueryUtils.applyCondition(iterOp, context);
                  }
                  if (ok) {
                    applyProjection(projAttrib, context, result, val,
                        intermediateResults, isIntersection);
                    if (limit != -1 && result.size() == limit) {
                      return;
                    }
                    // return pResultSet;
                  }
                } 
              }
            } else {
              Iterator itr = ((Collection)value).iterator();
              while (itr.hasNext()) {
                boolean ok = true;
                Object val = itr.next();
                if (reUpdateInProgress) {
                  //Shobhit: Compare the value in index with value in RegionEntry.
                  ok = verifyEntryAndIndexVaue(entry, val, context);
                }
                if (ok && runtimeItr != null) {
                  runtimeItr.setCurrent(val);
                  ok = QueryUtils.applyCondition(iterOp, context);
                }
                if (ok) {
                  applyProjection(projAttrib, context, result, val,
                      intermediateResults, isIntersection);
                  if (this.verifylimit(result, limit, context)) {
                    return;
                  }
                  // return pResultSet;
                }
              }
            }
          }
          else {
            boolean ok = true;
            if (reUpdateInProgress) {
              //Shobhit: Compare the value in index with in RegionEntry.
              ok = verifyEntryAndIndexVaue(entry, value, context);
            }
            if (ok && runtimeItr != null) {
              runtimeItr.setCurrent(value);
              ok = QueryUtils.applyCondition(iterOp, context);
            }
            if (ok) {
              if (context.isCqQueryContext()) {
                result.add(new CqEntry(((RegionEntry)e.getKey()).getKey(),
                    value));
              }
              else {
                applyProjection(projAttrib, context, result, value,
                    intermediateResults, isIntersection);
              }
            }
          }
        }
      }
    }

    public void removeValuesFromCollection(Collection result,
        CompiledValue iterOps, RuntimeIterator runtimeItr,
        ExecutionContext context, List projAttrib,
        SelectResults intermediateResults, boolean isIntersection)
        throws FunctionDomainException, TypeMismatchException,
        NameResolutionException, QueryInvocationTargetException
    {
      // Iterator valuesIter = map.values().iterator();
      Iterator entries = map.entrySet().iterator();
      while (entries.hasNext()) {
        Map.Entry e = (Map.Entry)entries.next();
        Object value = e.getValue();
        if (value instanceof Collection) {
          Iterator itr = ((Collection)value).iterator();
          while (itr.hasNext()) {
            boolean ok = true;
            Object val = itr.next();
            if (runtimeItr != null) {
              runtimeItr.setCurrent(val);
              ok = QueryUtils.applyCondition(iterOps, context);

            }
            if (ok) {
              removeProjection(projAttrib, context, result, val,
                  intermediateResults, isIntersection);
            }
          }
        }
        else {
          boolean ok = true;
          if (runtimeItr != null) {
            // Attempt to remove only if it was apossibly added
            runtimeItr.setCurrent(value);
            ok = QueryUtils.applyCondition(iterOps, context);
          }
          if (ok) {
            if (context.isCqQueryContext()) {
              result.remove(new CqEntry(((RegionEntry)e.getKey()).getKey(),
                  value));
            }
            else {
              removeProjection(projAttrib, context, result, value,
                  intermediateResults, isIntersection);
            }
          }

        }
      }
    }

    public void removeValuesFromCollection(Collection result)
    {
      Iterator valuesIter = map.values().iterator();
      while (valuesIter.hasNext()) {
        Object value = valuesIter.next();
        if (value instanceof Collection)
          result.removeAll((Collection)value);
        else
          result.remove(value);
      }
    }

    private boolean verifylimit(Collection result, int limit,
        ExecutionContext context) {     
      if (limit > 0) {
        if (!context.isDistinct()) {
          return ((Bag)result).size() == limit;
        } else if (result.size() == limit) {
          return true;
        }
      }
      return false;
    }

    public boolean containsEntry(RegionEntry entry)
    {
      return map.containsKey(entry);
    }

    public boolean containsValue(Object value)
    {
      throw new RuntimeException(
          LocalizedStrings.RangeIndex_NOT_YET_IMPLEMENTED.toLocalizedString());
    }

    public void clear()
    {
      map.clear();
      this.numValues.set(0);
    }

    public Set entrySet()
    {
      return map.entrySet();
    }

    /**
     * This replaces a key's value along with updating the numValues
     * correctly.
     * @param entry
     * @param values
     */
    public void replace(RegionEntry entry, Object values) {
      int numOldValues = getNumValues(entry);
      this.map.put(entry, values);
      this.numValues.addAndGet(((values instanceof Collection) ? ((Collection) values)
          .size() : 1) - numOldValues);
    }
  }

  /**
   * This will populate resultset from both type of indexes,
   * {@link CompactRangeIndex} and {@link RangeIndex}.
   * 
   * @param list
   * @param outerEntries
   * @param innerEntries
   * @param context
   * @param key
   * @throws FunctionDomainException
   * @throws TypeMismatchException
   * @throws NameResolutionException
   * @throws QueryInvocationTargetException
   */
  protected void populateListForEquiJoin(List list, Object outerEntries,
      Object innerEntries, ExecutionContext context, Object key)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {

    Assert.assertTrue((outerEntries != null && innerEntries != null), "OuterEntries or InnerEntries must not be null");
    
    Object values[][] = new Object[2][];
    Iterator itr = null;
    int j = 0;

    while (j < 2) {
      boolean isRangeIndex = false;
      if (j == 0) {
        if (outerEntries instanceof RegionEntryToValuesMap) {
          itr = ((RegionEntryToValuesMap)outerEntries).map.entrySet().iterator();
          isRangeIndex = true;
        } else if (outerEntries instanceof CloseableIterator){
          itr = (Iterator) outerEntries;
        } 
      }
      else {
        if (innerEntries instanceof RegionEntryToValuesMap) {
          itr = ((RegionEntryToValuesMap)innerEntries).map.entrySet().iterator();
          isRangeIndex = true;
        } else if (innerEntries instanceof CloseableIterator){
          itr = (Iterator) innerEntries;
        } 
      }
      //TODO :Asif Identify appropriate size of the List
      
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
          Map.Entry entry = (Map.Entry)itr.next();
          val = entry.getValue();
          if (val instanceof Collection) {
            entryVal = ((Collection)val).iterator().next();
          } else {
            entryVal = val;
          }
          re = (RegionEntry) entry.getKey();
        } else {
          ie = (IndexStoreEntry)itr.next();
        }
        //Bug#41010: We need to verify if Inner and Outer Entries
        // are consistent with index key values.
        boolean ok = true;
        if (isRangeIndex) {
          if(re.isUpdateInProgress()) {
           ok = ((RangeIndex) indInfo._getIndex()).verifyEntryAndIndexVaue(re,
              entryVal, context);
          }
        } else if (ie.isUpdateInProgress()) {
          ok = ((CompactRangeIndex) indInfo._getIndex())
              .verifyInnerAndOuterEntryValues(ie, context, indInfo, key);
        }
        if (ok) {
          if (isRangeIndex) {
            if (val instanceof Collection) {
              dummy.addAll((Collection) val);
            }
            else {
              dummy.add(val);
            }
          } else {
            if (IndexManager.IS_TEST_EXPANSION) {
              dummy.addAll(((CompactRangeIndex)indInfo._getIndex()).expandValue(context, key, null, OQLLexerTokenTypes.TOK_EQ, -1, ie.getDeserializedValue()));
            }
            else {
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
   * Sets the isIndexedPdxKeys flag indicating if all the keys in the index are
   * Strings or PdxStrings. Also sets another flag isIndexedPdxKeysFlagSet that
   * indicates isIndexedPdxKeys has been set/reset to avoid frequent calculation
   * of map size
   * 
   * @param key
   */
  public synchronized void setPdxStringFlag(Object key) {
    // For Null and Undefined keys do not set the isIndexedPdxKeysFlagSet flag
    if (key == null || key == IndexManager.NULL
        || key == QueryService.UNDEFINED) {
      return;
    }
    if (!isIndexedPdxKeys) {
      if (key instanceof PdxString) {
        isIndexedPdxKeys = true;
      }
    }
    isIndexedPdxKeysFlagSet = true;
  }

  /**
   * Converts Strings to PdxStrings and vice-versa based on the isIndexedPdxKeys
   * flag
   * 
   * @param key
   * @return PdxString or String based on isIndexedPdxKeys flag
   */
  public Object getPdxStringForIndexedPdxKeys(Object key) {
    if (isIndexedPdxKeys) {
      if (key instanceof String){
        return new PdxString((String) key);
      }
    } else if (key instanceof PdxString) {
       return ((PdxString) key).toString();
    }
    return key; 
  }
  
  public boolean removeFromKeysToRemove(Collection keysToRemove, Object key) {
    Iterator iterator = keysToRemove.iterator();
    while (iterator.hasNext()) {
      try {
        if (TypeUtils.compare(key, iterator.next(), OQLLexerTokenTypes.TOK_EQ).equals(Boolean.TRUE)) {
          iterator.remove();
          return true;
        }
      }
      catch (TypeMismatchException e) {
        //they are not equals, so we just continue iterating
      }
    }
    return false;
  }
 
  public boolean acquireIndexReadLockForRemove() {
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
    if (logger.isDebugEnabled()) {
      logger.debug("Acquiring write lock on Index {}", this.getName());
    }
    removeIndexLock.writeLock().lock();
    if (logger.isDebugEnabled()) {
      logger.debug("Acquired write lock on index {}", this.getName());
    }
  }

  public void releaseIndexWriteLockForRemove() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Releasing write lock on Index {}", this.getName());
    }
    removeIndexLock.writeLock().unlock();
    if (isDebugEnabled) {
      logger.debug("Released write lock on Index {}", this.getName());
    }
  }

  public boolean isPopulated() {
    return isPopulated;
  }

  public void setPopulated(boolean isPopulated) {
    this.isPopulated = isPopulated;
  }
  
  
}
