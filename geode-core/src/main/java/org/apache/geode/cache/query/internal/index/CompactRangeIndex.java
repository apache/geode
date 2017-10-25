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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledBindArgument;
import org.apache.geode.cache.query.internal.CompiledIteratorDef;
import org.apache.geode.cache.query.internal.CompiledLiteral;
import org.apache.geode.cache.query.internal.CompiledPath;
import org.apache.geode.cache.query.internal.CompiledSortCriterion;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.CqEntry;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.IndexInfo;
import org.apache.geode.cache.query.internal.QRegion;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.QueryUtils;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.cache.query.internal.Support;
import org.apache.geode.cache.query.internal.index.IndexManager.TestHook;
import org.apache.geode.cache.query.internal.index.IndexStore.IndexStoreEntry;
import org.apache.geode.cache.query.internal.index.MemoryIndexStore.MemoryIndexStoreEntry;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.entries.VMThinRegionEntryHeap;
import org.apache.geode.internal.cache.persistence.query.CloseableIterator;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.internal.PdxString;

/**
 * A CompactRangeIndex is a range index that has simple data structures to minimize its footprint,
 * at the expense of doing extra work at index maintenance. It is selected as the index
 * implementation when the indexed expression is a path expression and the from clause has only one
 * iterator. This implies there is only one value in the index for each region entry.
 * 
 * This index does not support the storage of projection attributes.
 * 
 * Currently this implementation only supports an index on a region path.
 * 
 * @since GemFire 6.0
 */
public class CompactRangeIndex extends AbstractIndex {
  private static final Logger logger = LogService.getLogger();

  private static TestHook testHook;

  protected ThreadLocal<OldKeyValuePair> oldKeyValue;

  private IndexStore indexStore;

  static boolean TEST_ALWAYS_UPDATE_IN_PROGRESS = false;

  public CompactRangeIndex(String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes, String origFromClause,
      String origIndexExpr, String[] definitions, IndexStatistics stats) {
    super(indexName, region, fromClause, indexedExpression, projectionAttributes, origFromClause,
        origIndexExpr, definitions, stats);
    if (IndexManager.IS_TEST_LDM) {
      indexStore = new MapIndexStore(
          ((LocalRegion) region).getIndexMap(indexName, indexedExpression, origFromClause), region);
    } else {
      indexStore = new MemoryIndexStore(region, internalIndexStats);
    }
  }

  public IndexStore getIndexStorage() {
    return indexStore;
  }

  /**
   * Get the index type
   * 
   * @return the type of index
   */
  public IndexType getType() {
    return IndexType.FUNCTIONAL;
  }

  @Override
  protected boolean isCompactRangeIndex() {
    return true;
  }

  @Override
  public void initializeIndex(boolean loadEntries) throws IMQException {
    long startTime = System.nanoTime();
    this.evaluator.initializeIndex(loadEntries);
    this.internalIndexStats.incNumUpdates(((IMQEvaluator) this.evaluator).getTotalEntriesUpdated());
    long endTime = System.nanoTime();
    this.internalIndexStats.incUpdateTime(endTime - startTime);
  }

  void addMapping(RegionEntry entry) throws IMQException {
    this.evaluator.evaluate(entry, true);
    this.internalIndexStats.incNumUpdates();
  }

  /**
   * @param opCode one of OTHER_OP, BEFORE_UPDATE_OP, AFTER_UPDATE_OP.
   */
  void removeMapping(RegionEntry entry, int opCode) throws IMQException {
    if (opCode == BEFORE_UPDATE_OP) {
      // Either take key from reverse map OR evaluate it using IMQEvaluator.
      if (!IndexManager.isObjectModificationInplace()) {
        // It will always contain 1 element only, for this thread.
        if (oldKeyValue == null) {
          oldKeyValue = new ThreadLocal<OldKeyValuePair>();
        }
        oldKeyValue.set(new OldKeyValuePair());
        this.evaluator.evaluate(entry, false);
      }
    } else {
      // Need to reset the thread-local map as many puts and destroys might
      // happen in same thread.
      if (oldKeyValue != null) {
        oldKeyValue.remove();
      }
      this.evaluator.evaluate(entry, false);
      this.internalIndexStats.incNumUpdates();
    }
  }

  void removeMapping(Object key, RegionEntry entry) throws IMQException {
    indexStore.removeMapping(key, entry);
  }

  public boolean clear() {
    return indexStore.clear();
  }


  public List queryEquijoinCondition(IndexProtocol indx, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    // get a read lock when doing a lookup
    long start = updateIndexUseStats();
    ((AbstractIndex) indx).updateIndexUseStats();
    List data = new ArrayList();
    Iterator<IndexStoreEntry> outer = null;
    Iterator inner = null;
    try {
      // We will iterate over each of the index Map to obtain the keys
      outer = ((MemoryIndexStore) indexStore).getKeysIterator();

      if (indx instanceof CompactRangeIndex) {
        IndexStore indexStore = ((CompactRangeIndex) indx).getIndexStorage();
        inner = ((MemoryIndexStore) indexStore).getKeysIterator();

      } else {
        inner = ((RangeIndex) indx).getValueToEntriesMap().entrySet().iterator();
      }
      IndexStoreEntry outerEntry = null;
      Object innerEntry = null;
      Object outerKey = null;
      Object innerKey = null;
      boolean incrementInner = true;
      outer: while (outer.hasNext()) {
        outerEntry = outer.next();
        outerKey = outerEntry.getDeserializedKey();
        // TODO: eliminate all labels
        inner: while (!incrementInner || inner.hasNext()) {
          if (incrementInner) {
            innerEntry = inner.next();
            if (innerEntry instanceof IndexStoreEntry) {
              innerKey = ((IndexStoreEntry) innerEntry).getDeserializedKey();
            } else {
              innerKey = ((Map.Entry) innerEntry).getKey();
            }
          }
          int compare = ((Comparable) outerKey).compareTo(innerKey);
          if (compare == 0) {
            Object innerValue = null;
            CloseableIterator<IndexStoreEntry> iter = null;
            try {
              if (innerEntry instanceof IndexStoreEntry) {
                innerValue = ((CompactRangeIndex) indx).getIndexStorage().get(outerKey);
              } else {
                innerValue = ((Map.Entry) innerEntry).getValue();
              }
              iter = indexStore.get(outerKey);
              populateListForEquiJoin(data, iter, innerValue, context, innerKey);
            } finally {
              if (iter != null) {
                iter.close();
              }
              if (innerValue != null && innerValue instanceof CloseableIterator) {
                ((CloseableIterator<IndexStoreEntry>) innerValue).close();
              }
            }

            incrementInner = true;
            continue outer;
          } else if (compare < 0) {
            // The outer key is smaller than the inner key. That means
            // that we need
            // to increment the outer loop without moving inner loop.
            // incrementOuter = true;
            incrementInner = false;
            continue outer;
          } else {
            // The outer key is greater than inner key , so increment the
            // inner loop without changing outer
            incrementInner = true;
          }
        }
        break;
      }
      return data;
    } finally {
      ((AbstractIndex) indx).updateIndexUseEndStats(start);
      updateIndexUseEndStats(start);
      if (inner != null && indx instanceof CompactRangeIndex
          && inner instanceof CloseableIterator) {
        ((CloseableIterator<IndexStoreEntry>) inner).close();
      }
    }
  }

  /**
   * This evaluates the left and right side of a EQUI-JOIN where condition for which this Index was
   * used. Like, if condition is "p.ID = e.ID", {@link IndexInfo} will contain Left as p.ID, Right
   * as e.ID and operator as TOK_EQ. This method will evaluate p.ID OR e.ID based on if it is inner
   * or outer RegionEntry, and verify the p.ID = e.ID.
   * 
   * This method is called only for Memory indexstore
   * 
   * @return true if entry value and index value are consistent.
   */
  protected boolean verifyInnerAndOuterEntryValues(IndexStoreEntry entry, ExecutionContext context,
      IndexInfo indexInfo, Object keyVal) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // Verify index key in value only for memory index store
    CompactRangeIndex index = (CompactRangeIndex) indexInfo._getIndex();
    RuntimeIterator runtimeItr = index.getRuntimeIteratorForThisIndex(context, indexInfo);
    if (runtimeItr != null) {
      runtimeItr.setCurrent(((MemoryIndexStoreEntry) entry).getDeserializedValue());
    }
    return evaluateEntry(indexInfo, context, keyVal);
  }

  public int getSizeEstimate(Object key, int operator, int matchLevel)
      throws TypeMismatchException {
    // Get approx size;
    int size = 0;
    if (key == null) {
      key = IndexManager.NULL;
    }
    long start = updateIndexUseStats(false);
    try {
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ: {
          key = TypeUtils.indexKeyFor(key);
          key = getPdxStringForIndexedPdxKeys(key);
          size = indexStore.size(key);
          break;
        }
        case OQLLexerTokenTypes.TOK_NE_ALT:
        case OQLLexerTokenTypes.TOK_NE:
          size = this.region.size();
          key = TypeUtils.indexKeyFor(key);
          key = getPdxStringForIndexedPdxKeys(key);
          size -= indexStore.size(key);
          break;
        case OQLLexerTokenTypes.TOK_LE:
        case OQLLexerTokenTypes.TOK_LT:
          if (matchLevel <= 0 && (key instanceof Number)) {

            int totalSize = indexStore.size();
            if (CompactRangeIndex.testHook != null) {
              CompactRangeIndex.testHook.hook(1);
            }
            if (totalSize > 1) {
              Number keyAsNum = (Number) key;
              int x = 0;
              IndexStoreEntry firstEntry = null;
              CloseableIterator<IndexStoreEntry> iter1 = null;
              CloseableIterator<IndexStoreEntry> iter2 = null;

              try {
                iter1 = indexStore.iterator(null);
                if (iter1.hasNext()) {
                  firstEntry = iter1.next();

                  IndexStoreEntry lastEntry = null;

                  iter2 = indexStore.descendingIterator(null);
                  if (iter2.hasNext()) {
                    lastEntry = iter2.next();
                  }

                  if (firstEntry != null && lastEntry != null) {
                    Number first = (Number) firstEntry.getDeserializedKey();
                    Number last = (Number) lastEntry.getDeserializedKey();
                    if (first.doubleValue() != last.doubleValue()) {
                      // Shobhit: Now without ReadLoack on index we can end up with 0
                      // in denominator if the numbers are floating-point and
                      // truncated with conversion to long, and the first and last
                      // truncate to the same long, so safest calculation is to
                      // convert to doubles.
                      x = (int) (((keyAsNum.doubleValue() - first.doubleValue()) * totalSize)
                          / (last.doubleValue() - first.doubleValue()));
                    }
                  }

                  if (x < 0) {
                    x = 0;
                  }
                  size = x;
                }
              } finally {
                if (iter1 != null) {
                  iter1.close();
                }
                if (iter2 != null) {
                  iter1.close();
                }
              }

            } else {
              // not attempting to differentiate between LT & LE
              size = indexStore.size(key) > 0 ? 1 : 0;
            }
          } else {
            size = Integer.MAX_VALUE;
          }
          break;

        case OQLLexerTokenTypes.TOK_GE:
        case OQLLexerTokenTypes.TOK_GT:
          if (matchLevel <= 0 && (key instanceof Number)) {
            int totalSize = indexStore.size();
            if (CompactRangeIndex.testHook != null) {
              CompactRangeIndex.testHook.hook(2);
            }
            if (totalSize > 1) {
              Number keyAsNum = (Number) key;
              int x = 0;
              IndexStoreEntry firstEntry = null;
              CloseableIterator<IndexStoreEntry> iter1 = null;
              CloseableIterator<IndexStoreEntry> iter2 = null;

              try {
                iter1 = indexStore.iterator(null);
                if (iter1.hasNext()) {
                  firstEntry = iter1.next();
                }

                IndexStoreEntry lastEntry = null;

                iter2 = indexStore.descendingIterator(null);
                if (iter2.hasNext()) {
                  lastEntry = iter2.next();
                }


                if (firstEntry != null && lastEntry != null) {
                  Number first = (Number) firstEntry.getDeserializedKey();
                  Number last = (Number) lastEntry.getDeserializedKey();
                  if (first.doubleValue() != last.doubleValue()) {
                    // Shobhit: Now without ReadLoack on index we can end up with 0
                    // in denominator if the numbers are floating-point and
                    // truncated with conversion to long, and the first and last
                    // truncate to the same long, so safest calculation is to
                    // convert to doubles.
                    x = (int) (((last.doubleValue() - keyAsNum.doubleValue()) * totalSize)
                        / (last.doubleValue() - first.doubleValue()));
                  }
                }
                if (x < 0) {
                  x = 0;
                }
                size = x;
              } finally {
                if (iter1 != null) {
                  iter1.close();
                }
              }
            } else {
              // not attempting to differentiate between GT & GE
              size = indexStore.size(key) > 0 ? 1 : 0;
            }
          } else {
            size = Integer.MAX_VALUE;
          }
          break;
      }
    } catch (ClassCastException e) {
      // no values will match in this index because the key types are not the same
      // This means that there will be 0 results and it will be fast to use this index
      // because it has filtered everything out
      return 0;
    } catch (EntryDestroyedException ignore) {
      return Integer.MAX_VALUE;
    } finally {
      updateIndexUseEndStats(start, false);
    }
    return size;
  }

  /** Method called while appropriate lock held on index */
  private void lockedQueryPrivate(Object key, int operator, Collection results,
      CompiledValue iterOps, RuntimeIterator runtimeItr, ExecutionContext context, Set keysToRemove,
      List projAttrib, SelectResults intermediateResults, boolean isIntersection)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    if (keysToRemove == null) {
      keysToRemove = new HashSet(0);
    }
    int limit = -1;

    Boolean applyLimit = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX);
    if (applyLimit != null && applyLimit) {
      limit = (Integer) context.cacheGet(CompiledValue.RESULT_LIMIT);
    }

    Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
    boolean applyOrderBy = false;
    List orderByAttrs = null;
    if (orderByClause != null && orderByClause) {
      orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttrs.get(0);
      applyOrderBy = true;
    }
    if (isEmpty()) {
      return;
    }
    key = getPdxStringForIndexedPdxKeys(key);
    evaluate(key, operator, results, iterOps, runtimeItr, context, keysToRemove, projAttrib,
        intermediateResults, isIntersection, limit, applyOrderBy, orderByAttrs);
  }

  /** Method called while appropriate lock held on index */
  void lockedQuery(Object lowerBoundKey, int lowerBoundOperator, Object upperBoundKey,
      int upperBoundOperator, Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    lowerBoundKey = TypeUtils.indexKeyFor(lowerBoundKey);
    upperBoundKey = TypeUtils.indexKeyFor(upperBoundKey);
    boolean lowerBoundInclusive = lowerBoundOperator == OQLLexerTokenTypes.TOK_GE;
    boolean upperBoundInclusive = upperBoundOperator == OQLLexerTokenTypes.TOK_LE;
    // LowerBound Key inclusive , Upper bound key exclusive

    int limit = -1;
    Boolean applyLimit = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX);
    if (applyLimit != null && applyLimit) {
      limit = (Integer) context.cacheGet(CompiledValue.RESULT_LIMIT);
    }
    Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);

    List orderByAttrs;
    boolean asc = true;
    boolean multiColOrderBy = false;
    if (orderByClause != null && orderByClause) {
      orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttrs.get(0);
      asc = !csc.getCriterion();
      multiColOrderBy = orderByAttrs.size() > 1;
    }
    // return if the index map is still empty at this stage
    if (isEmpty()) {
      return;
    }
    lowerBoundKey = getPdxStringForIndexedPdxKeys(lowerBoundKey);
    upperBoundKey = getPdxStringForIndexedPdxKeys(upperBoundKey);
    if (keysToRemove == null) {
      keysToRemove = new HashSet();
    }
    CloseableIterator<IndexStoreEntry> iterator = null;
    try {
      if (asc) {
        iterator = indexStore.iterator(lowerBoundKey, lowerBoundInclusive, upperBoundKey,
            upperBoundInclusive, keysToRemove);
      } else {
        iterator = indexStore.descendingIterator(lowerBoundKey, lowerBoundInclusive, upperBoundKey,
            upperBoundInclusive, keysToRemove);
      }
      addToResultsFromEntries(lowerBoundKey, upperBoundKey, lowerBoundOperator, upperBoundOperator,
          iterator, results, null, null, context, null, null, true, multiColOrderBy ? -1 : limit);
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
  }


  private void evaluate(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator runtimeItr, ExecutionContext context, Set keysToRemove, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection, int limit, boolean applyOrderBy,
      List orderByAttribs) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    boolean multiColOrderBy = false;
    if (keysToRemove == null) {
      keysToRemove = new HashSet(0);
    }
    key = TypeUtils.indexKeyFor(key);
    if (key == null) {
      key = IndexManager.NULL;
    }
    boolean asc = true;
    if (applyOrderBy) {
      CompiledSortCriterion csc = (CompiledSortCriterion) orderByAttribs.get(0);
      asc = !csc.getCriterion();
      multiColOrderBy = orderByAttribs.size() > 1;
    }
    CloseableIterator<IndexStoreEntry> iterator = null;
    try {
      switch (operator) {
        case OQLLexerTokenTypes.TOK_EQ:
          assert keysToRemove.isEmpty();
          iterator = indexStore.get(key);
          addToResultsFromEntries(key, operator, iterator, results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
          break;
        case OQLLexerTokenTypes.TOK_LT: {
          if (asc) {
            iterator = indexStore.iterator(null, true, key, false, keysToRemove);
          } else {
            iterator = indexStore.descendingIterator(null, true, key, false, keysToRemove);
          }
          addToResultsFromEntries(key, operator, iterator, results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
        }
          break;
        case OQLLexerTokenTypes.TOK_LE: {
          if (asc) {
            iterator = indexStore.iterator(null, true, key, true, keysToRemove);
          } else {
            iterator = indexStore.descendingIterator(null, true, key, true, keysToRemove);
          }

          addToResultsFromEntries(key, operator, iterator, results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
        }
          break;
        case OQLLexerTokenTypes.TOK_GT: {
          if (asc) {
            iterator = indexStore.iterator(key, false, keysToRemove);
          } else {
            iterator = indexStore.descendingIterator(key, false, keysToRemove);
          }
          addToResultsFromEntries(key, operator, iterator, results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
        }
          break;
        case OQLLexerTokenTypes.TOK_GE: {
          if (asc) {
            iterator = indexStore.iterator(key, true, keysToRemove);
          } else {
            iterator = indexStore.descendingIterator(key, true, keysToRemove);
          }

          addToResultsFromEntries(key, operator, iterator, results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
        }
          break;
        case OQLLexerTokenTypes.TOK_NE_ALT:
        case OQLLexerTokenTypes.TOK_NE: {
          keysToRemove.add(key);
          if (asc) {
            iterator = indexStore.iterator(keysToRemove);
          } else {
            iterator = indexStore.descendingIterator(keysToRemove);
          }
          addToResultsFromEntries(key, operator, iterator, results, iterOps, runtimeItr, context,
              projAttrib, intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
          // If the key is not null, then add the nulls to the results as this is a not equals query
          if (!IndexManager.NULL.equals(key)) {
            // we pass in the operator TOK_EQ because we want to add results where the key is equal
            // to NULL
            addToResultsFromEntries(IndexManager.NULL, OQLLexerTokenTypes.TOK_EQ,
                indexStore.get(IndexManager.NULL), results, iterOps, runtimeItr, context,
                projAttrib, intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
          }
          // If the key is not undefined, then add the undefineds to the results as this is a not
          // equals query
          if (!QueryService.UNDEFINED.equals(key)) {
            // we pass in the operator TOK_EQ because we want to add results where the key is equal
            // to UNDEFINED
            addToResultsFromEntries(QueryService.UNDEFINED, OQLLexerTokenTypes.TOK_EQ,
                indexStore.get(QueryService.UNDEFINED), results, iterOps, runtimeItr, context,
                projAttrib, intermediateResults, isIntersection, multiColOrderBy ? -1 : limit);
          }
        }
          break;
        default:
          throw new AssertionError("Operator = " + operator);
      } // end switch
    } catch (ClassCastException ex) {
      if (operator == OQLLexerTokenTypes.TOK_EQ) { // result is empty set
        return;
      } else if (operator == OQLLexerTokenTypes.TOK_NE
          || operator == OQLLexerTokenTypes.TOK_NE_ALT) { // put all in result
        keysToRemove.add(key);
        try {
          if (asc) {
            iterator = indexStore.iterator(keysToRemove);
          } else {
            iterator = indexStore.descendingIterator(keysToRemove);
          }
          addToResultsFromEntries(key, OQLLexerTokenTypes.TOK_NE, iterator, results, iterOps,
              runtimeItr, context, projAttrib, intermediateResults, isIntersection,
              multiColOrderBy ? -1 : limit);
        } finally {
          if (iterator != null) {
            iterator.close();
          }
        }
      } else { // otherwise throw exception
        throw new TypeMismatchException("", ex);
      }
    } finally {
      if (iterator != null) {
        iterator.close();
      }
    }
  }

  @Override
  void instantiateEvaluator(IndexCreationHelper indexCreationHelper) {
    this.evaluator = new IMQEvaluator(indexCreationHelper);
  }

  // Only used by CompactMapRangeIndex. This is due to the way the index initialization happens
  // first we use the IMQEvaluator for CompactMapRangeIndex
  // Each index in CMRI is a CRI that has the CRI.IMQ and not AbstractIndex.IMQ
  // So instead we create create the evaluator
  // because we are not doing index init as usual (each value is just put directly?)
  // we must set the result type to match
  void instantiateEvaluator(IndexCreationHelper ich, ObjectType objectType) {
    instantiateEvaluator(ich);
    ((IMQEvaluator) this.evaluator).indexResultSetType = objectType;
  }

  public ObjectType getResultSetType() {
    return this.evaluator.getIndexResultSetType();
  }

  /*
   * 
   * @param lowerBoundKey the index key to match on
   * 
   * @param lowerBoundOperator the operator to use to determine a match
   */
  private void addToResultsFromEntries(Object lowerBoundKey, int lowerBoundOperator,
      CloseableIterator<IndexStoreEntry> entriesIter, Collection result, CompiledValue iterOps,
      RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection, int limit)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    addToResultsFromEntries(lowerBoundKey, null, lowerBoundOperator, -1, entriesIter, result,
        iterOps, runtimeItr, context, projAttrib, intermediateResults, isIntersection, limit);
  }

  /*
   * 
   * @param lowerBoundKey the index key to match on for a lower bound on a ranged query, otherwise
   * the key to match on
   * 
   * @param upperBoundKey the index key to match on for an upper bound on a ranged query, otherwise
   * null
   * 
   * @param lowerBoundOperator the operator to use to determine a match against the lower bound
   * 
   * @param upperBoundOperator the operator to use to determine a match against the upper bound
   */
  private void addToResultsFromEntries(Object lowerBoundKey, Object upperBoundKey,
      int lowerBoundOperator, int upperBoundOperator,
      CloseableIterator<IndexStoreEntry> entriesIter, Collection result, CompiledValue iterOps,
      RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection, int limit)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {

    QueryObserver observer = QueryObserverHolder.getInstance();
    boolean limitApplied = false;
    if (entriesIter == null || (limitApplied = verifyLimit(result, limit))) {
      if (limitApplied) {
        if (observer != null) {
          observer.limitAppliedAtIndexLevel(this, limit, result);
        }
      }
      return;
    }

    Set seenKey = null;
    if (IndexManager.IS_TEST_EXPANSION) {
      seenKey = new HashSet();
    }

    while (entriesIter.hasNext()) {
      try {
        // Check if query execution on this thread is canceled.
        QueryMonitor.isQueryExecutionCanceled();
        if (IndexManager.testHook != null) {
          if (this.region.getCache().getLogger().fineEnabled()) {
            this.region.getCache().getLogger()
                .fine("IndexManager TestHook is set in addToResultsFromEntries.");
          }
          IndexManager.testHook.hook(11);
        }

        IndexStoreEntry indexEntry = null;
        try {
          indexEntry = entriesIter.next();
        } catch (NoSuchElementException ignore) {
          // We are done with all the elements in array.
          // Continue from while.
          continue;
        }

        Object value = indexEntry.getDeserializedValue();

        if (IndexManager.IS_TEST_EXPANSION) {
          Object rk = indexEntry.getDeserializedRegionKey();
          if (seenKey.contains(rk)) {
            continue;
          }
          seenKey.add(rk);

          List expandedResults = expandValue(context, lowerBoundKey, upperBoundKey,
              lowerBoundOperator, upperBoundOperator, value);
          Iterator iterator = ((Collection) expandedResults).iterator();
          while (iterator.hasNext()) {
            value = iterator.next();
            if (value != null) {
              boolean ok = true;

              if (runtimeItr != null) {
                runtimeItr.setCurrent(value);
              }
              if (ok && runtimeItr != null && iterOps != null) {
                ok = QueryUtils.applyCondition(iterOps, context);
              }
              if (ok) {
                if (context != null && context.isCqQueryContext()) {
                  result.add(new CqEntry(indexEntry.getDeserializedRegionKey(), value));
                } else {
                  applyProjection(projAttrib, context, result, value, intermediateResults,
                      isIntersection);
                }
                if (verifyLimit(result, limit)) {
                  observer.limitAppliedAtIndexLevel(this, limit, result);
                  return;
                }
              }
            }
          }
        } else {
          if (value != null) {
            boolean ok = true;
            if (indexEntry.isUpdateInProgress() || TEST_ALWAYS_UPDATE_IN_PROGRESS) {
              IndexInfo indexInfo = (IndexInfo) context.cacheGet(CompiledValue.INDEX_INFO);
              if (runtimeItr == null) {
                runtimeItr = getRuntimeIteratorForThisIndex(context, indexInfo);
                if (runtimeItr == null) {
                  // could not match index with iterator
                  throw new QueryInvocationTargetException(
                      "Query alias's must be used consistently");
                }
              }
              runtimeItr.setCurrent(value);
              // Verify index key in region entry value.

              ok = evaluateEntry((IndexInfo) indexInfo, context, null);
            }
            if (runtimeItr != null) {
              runtimeItr.setCurrent(value);
            }
            if (ok && runtimeItr != null && iterOps != null) {
              ok = QueryUtils.applyCondition(iterOps, context);
            }
            if (ok) {
              if (context != null && context.isCqQueryContext()) {
                result.add(new CqEntry(indexEntry.getDeserializedRegionKey(), value));
              } else {
                if (IndexManager.testHook != null) {
                  IndexManager.testHook.hook(200);
                }
                applyProjection(projAttrib, context, result, value, intermediateResults,
                    isIntersection);
              }
              if (verifyLimit(result, limit)) {
                observer.limitAppliedAtIndexLevel(this, limit, result);
                return;
              }
            }
          }
        }
      } catch (ClassCastException | EntryDestroyedException ignore) {
        // ignore it
      }
    }
  }

  public List expandValue(ExecutionContext context, Object lowerBoundKey, Object upperBoundKey,
      int lowerBoundOperator, int upperBoundOperator, Object value) {
    try {
      List expandedResults = new ArrayList();
      this.evaluator.expansion(expandedResults, lowerBoundKey, upperBoundKey, lowerBoundOperator,
          upperBoundOperator, value);
      return expandedResults;
    } catch (IMQException e) {
      // TODO: never throw an anonymous inner class
      throw new CacheException(e) {};
    }
  }

  /**
   * This evaluates the left and right side of a where condition for which this Index was used.
   * Like, if condition is "ID > 1", {@link IndexInfo} will contain Left as ID, Right as '1' and
   * operator as TOK_GT. This method will evaluate ID from region entry value and verify the ID > 1.
   * 
   * Note: IndexInfo is created for each query separately based on the condition being evaluated
   * using the Index.
   * 
   * @return true if RegionEntry value satisfies the where condition (contained in IndexInfo).
   */
  protected boolean evaluateEntry(IndexInfo indexInfo, ExecutionContext context, Object keyVal)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    CompiledValue path = ((IndexInfo) indexInfo)._path();
    Object left = path.evaluate(context);
    CompiledValue key = ((IndexInfo) indexInfo)._key();
    Object right = null;

    // For CompiledUndefined indexInfo has null key.
    if (keyVal == null && key == null) {
      if (left == QueryService.UNDEFINED) {
        return true;
      } else {
        return false;
      }
    }

    if (key != null) {
      right = key.evaluate(context);
      // This next check is for map queries with In Clause, in those cases the reevaluation creates
      // a tuple. In other cases it does not
      if (null != right && indexInfo._getIndex() instanceof CompactMapRangeIndex
          && right instanceof Object[]) {
        right = ((Object[]) right)[0];
      }
    } else {
      right = keyVal;
    }

    int operator = indexInfo._operator();
    if (left == null && right == null) {
      return Boolean.TRUE;
    } else {
      if (left instanceof PdxString) {
        if (right instanceof String) {
          switch (key.getType()) {
            case CompiledValue.LITERAL:
              right = ((CompiledLiteral) key).getSavedPdxString();
              break;
            case OQLLexerTokenTypes.QUERY_PARAM:
              right = ((CompiledBindArgument) key).getSavedPdxString(context);
              break;
            case CompiledValue.FUNCTION:
            case CompiledValue.PATH:
              right = new PdxString((String) right);
          }
        }
      }
      Object result = TypeUtils.compare(left, right, operator);
      // result is Undefined if either left or right is Undefined or
      // either of them is null and operator is other than == or !=
      if (result == QueryService.UNDEFINED) {
        // Undefined is added to results for != conditions only
        if (operator != OQLLexerTokenTypes.TOK_NE || operator != OQLLexerTokenTypes.TOK_NE_ALT) {
          return Boolean.TRUE;
        } else {
          return Boolean.FALSE;
        }
      } else {
        return (Boolean) result;
      }
    }
  }

  void recreateIndexData() throws IMQException {
    indexStore.clear();
    int numKeys = (int) this.internalIndexStats.getNumberOfKeys();
    if (numKeys > 0) {
      this.internalIndexStats.incNumKeys(-numKeys);
    }
    int numValues = (int) this.internalIndexStats.getNumberOfValues();
    if (numValues > 0) {
      this.internalIndexStats.incNumValues(-numValues);
    }
    int updates = (int) this.internalIndexStats.getNumUpdates();
    if (updates > 0) {
      this.internalIndexStats.incNumUpdates(updates);
    }
    this.initializeIndex(true);
  }

  public String dump() {
    return this.indexStore.printAll();
  }

  protected InternalIndexStatistics createStats(String indexName) {
    return new RangeIndexStatistics(indexName);
  }

  class RangeIndexStatistics extends InternalIndexStatistics {
    private IndexStats vsdStats;

    public RangeIndexStatistics(String indexName) {
      this.vsdStats = new IndexStats(getRegion().getCache().getDistributedSystem(), indexName);
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
     * Returns the total amount of time (in nanoseconds) spent updating this index.
     */
    public long getTotalUpdateTime() {
      return this.vsdStats.getTotalUpdateTime();
    }

    /**
     * Returns the total number of times this index has been accessed by a query.
     */
    public long getTotalUses() {
      return this.vsdStats.getTotalUses();
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
      return indexStore.size(key);
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
      StringBuilder sb = new StringBuilder();
      sb.append("No Keys = ").append(getNumberOfKeys()).append("\n");
      sb.append("No Values = ").append(getNumberOfValues()).append("\n");
      sb.append("No Uses = ").append(getTotalUses()).append("\n");
      sb.append("No Updates = ").append(getNumUpdates()).append("\n");
      sb.append("Total Update time = ").append(getTotalUpdateTime()).append("\n");
      return sb.toString();
    }
  }

  class IMQEvaluator implements IndexedExpressionEvaluator {
    private InternalCache cache;
    private List fromIterators = null;
    private CompiledValue indexedExpr = null;
    private final String[] canonicalIterNames;
    private ObjectType indexResultSetType = null;
    private Region rgn = null;
    private Map dependencyGraph = null;

    /*
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
    // List of modified iterators, not null only when the boolean
    // isFirstItrOnEntry is false.
    private List indexInitIterators = null;
    // The additional Projection attribute representing the value of the
    // original 0th iterator. If the isFirstItrOnEntry is false, then it is not
    // null. However if the isFirstItrOnEntry is true and this attribute is not
    // null, this indicates that the 0th iterator is derived using an individual
    // entry thru Map operator on the Region.
    private CompiledValue additionalProj = null;
    // This is not null iff the boolean isFirstItrOnEntry is false.
    private CompiledValue modifiedIndexExpr = null;
    private ObjectType addnlProjType = null;
    private int initEntriesUpdated = 0;
    private boolean hasInitOccurredOnce = false;
    private boolean hasIndxUpdateOccurredOnce = false;
    private ExecutionContext initContext = null;
    private int iteratorSize = -1;

    /** Creates a new instance of IMQEvaluator */
    IMQEvaluator(IndexCreationHelper helper) {
      this.cache = helper.getCache();
      this.fromIterators = helper.getIterators();
      this.indexedExpr = helper.getCompiledIndexedExpression();
      this.canonicalIterNames = ((FunctionalIndexCreationHelper) helper).canonicalizedIteratorNames;
      this.rgn = helper.getRegion();

      // The modified iterators for optmizing Index cxreation
      isFirstItrOnEntry = ((FunctionalIndexCreationHelper) helper).isFirstIteratorRegionEntry;
      additionalProj = ((FunctionalIndexCreationHelper) helper).additionalProj;
      Object params1[] = {new QRegion(rgn, false)};
      initContext = new ExecutionContext(params1, cache);
      if (isFirstItrOnEntry) {
        this.indexInitIterators = this.fromIterators;
      } else {
        this.indexInitIterators = ((FunctionalIndexCreationHelper) helper).indexInitIterators;
        modifiedIndexExpr = ((FunctionalIndexCreationHelper) helper).modifiedIndexExpr;
        addnlProjType = ((FunctionalIndexCreationHelper) helper).addnlProjType;
      }
      this.iteratorSize = this.indexInitIterators.size();
      if (this.additionalProj instanceof CompiledPath) {
        String tailId = ((CompiledPath) this.additionalProj).getTailID();
        if (tailId.equals("key")) {
          // index on keys
          indexOnRegionKeys = true;
          indexStore.setIndexOnRegionKeys(true);
        } else if (!isFirstItrOnEntry) {
          // its not entries, its on value.
          indexOnValues = true;
          indexStore.setIndexOnValues(true);
        }
      }
    }

    public String getIndexedExpression() {
      return CompactRangeIndex.this.getCanonicalizedIndexedExpression();
    }

    public String getProjectionAttributes() {
      return CompactRangeIndex.this.getCanonicalizedProjectionAttributes();
    }

    public String getFromClause() {
      return CompactRangeIndex.this.getCanonicalizedFromClause();
    }

    public void expansion(List expandedResults, Object lowerBoundKey, Object upperBoundKey,
        int lowerBoundOperator, int upperBoundOperator, Object value) throws IMQException {
      try {
        ExecutionContext expansionContext = createExecutionContext(value);
        List iterators = expansionContext.getCurrentIterators();
        RuntimeIterator iter = (RuntimeIterator) iterators.get(0);
        iter.setCurrent(value);

        // first iter level is region entries, we can ignore as we already broke it down in the
        // index
        doNestedExpansion(1, expansionContext, expandedResults, lowerBoundKey, upperBoundKey,
            lowerBoundOperator, upperBoundOperator, value);
      } catch (Exception e) {
        throw new IMQException(e);
      }
    }

    private void doNestedExpansion(int level, ExecutionContext expansionContext,
        List expandedResults, Object lowerBoundKey, Object upperBoundKey, int lowerBoundOperator,
        int upperBoundOperator, Object value)
        throws TypeMismatchException, AmbiguousNameException, FunctionDomainException,
        NameResolutionException, QueryInvocationTargetException, IMQException {
      List iterList = expansionContext.getCurrentIterators();
      int iteratorSize = iterList.size();
      if (level == iteratorSize) {
        expand(expansionContext, expandedResults, lowerBoundKey, upperBoundKey, lowerBoundOperator,
            upperBoundOperator, value);
      } else {
        RuntimeIterator rIter = (RuntimeIterator) iterList.get(level);
        Collection c = rIter.evaluateCollection(expansionContext);
        if (c == null)
          return;
        Iterator cIter = c.iterator();
        while (cIter.hasNext()) {
          rIter.setCurrent(cIter.next());
          doNestedExpansion(level + 1, expansionContext, expandedResults, lowerBoundKey,
              upperBoundKey, lowerBoundOperator, upperBoundOperator, value);
        }
      }
    }

    /**
     * @param upperBoundKey if null, we do not do an upperbound check (may need to change this if we
     *        ever use null in a range query)
     */
    public void expand(ExecutionContext expansionContext, List expandedResults,
        Object lowerBoundKey, Object upperBoundKey, int lowerBoundOperator, int upperBoundOperator,
        Object value) throws IMQException {
      try {
        RuntimeIterator runtimeItr = getRuntimeIteratorForThisIndex(expansionContext);
        if (runtimeItr != null) {
          runtimeItr.setCurrent(value);
        }

        Object tupleIndexKey = indexedExpr.evaluate(expansionContext);
        tupleIndexKey = getPdxStringForIndexedPdxKeys(tupleIndexKey);

        Object compResult;
        // Check upper bound
        if (upperBoundKey != null) {
          compResult = TypeUtils.compare(tupleIndexKey, upperBoundKey, upperBoundOperator);
          if (compResult instanceof Boolean) {
            Boolean ok = (Boolean) compResult;
            if (!ok) {
              return;
            }
          }
        }

        if (tupleIndexKey instanceof Map) {
          if (lowerBoundOperator == OQLLexerTokenTypes.TOK_EQ) {
            if (!((Map) tupleIndexKey).containsKey(lowerBoundKey)) {
              return;
            }
          } else if (lowerBoundOperator == OQLLexerTokenTypes.TOK_NE) {
            if (((Map) tupleIndexKey).containsKey(lowerBoundKey)) {
              return;
            }
          }
        } else {
          // Check lower bound
          compResult = TypeUtils.compare(tupleIndexKey, lowerBoundKey, lowerBoundOperator);
          if (compResult instanceof Boolean) {
            Boolean ok = (Boolean) compResult;
            if (!ok) {
              return;
            }
          }
        }

        List currentRuntimeIters = expansionContext.getCurrentIterators();
        int iteratorSize = currentRuntimeIters.size();
        Object indxResultSet = null;

        // if the resultSetType is of structType, we need to create tuples
        // this is due to the way the resultsSets are being created
        boolean structType = (indexResultSetType instanceof StructType);
        if (iteratorSize == 1 && !structType) {
          RuntimeIterator iter = (RuntimeIterator) currentRuntimeIters.get(0);
          iter.setCurrent(value);
          indxResultSet = iter.evaluate(expansionContext);
          indxResultSet = value;
        } else {
          Object tuple[] = new Object[iteratorSize];
          tuple[0] = value;
          if (iteratorSize > 1) {
            for (int i = 1; i < iteratorSize; i++) {
              RuntimeIterator iter = (RuntimeIterator) currentRuntimeIters.get(i);
              tuple[i] = iter.evaluate(expansionContext);
            }
            Support.Assert(this.indexResultSetType instanceof StructTypeImpl,
                "The Index ResultType should have been an instance of StructTypeImpl rather than ObjectTypeImpl. The indxeResultType is "
                    + this.indexResultSetType);
          }
          indxResultSet = new StructImpl((StructTypeImpl) this.indexResultSetType, tuple);
        }

        expandedResults.add(indxResultSet);
      } catch (Exception e) {
        throw new IMQException(e);
      }
    }

    private ExecutionContext createExecutionContext(Object value) {
      DummyQRegion dQRegion = new DummyQRegion(rgn);
      dQRegion.setEntry(
          VMThinRegionEntryHeap.getEntryFactory().createEntry((RegionEntryContext) rgn, 0, value));
      Object params[] = {dQRegion};
      ExecutionContext context = new ExecutionContext(params, this.cache);
      context.newScope(IndexCreationHelper.INDEX_QUERY_SCOPE_ID);
      try {
        if (this.dependencyGraph != null) {
          context.setDependencyGraph(dependencyGraph);
        }
        for (int i = 0; i < this.iteratorSize; i++) {
          CompiledIteratorDef iterDef = (CompiledIteratorDef) fromIterators.get(i);
          if (this.dependencyGraph == null) {
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

        Support.Assert(this.indexResultSetType != null,
            "IMQEvaluator::evaluate:The StrcutType should have been initialized during index creation");
      } catch (Exception e) {
        logger.debug(e);
        throw new Error("Unable to reevaluate, this should not happen");
      }
      return context;
    }

    /**
     * @param add true if adding to index, false if removing
     */
    public void evaluate(RegionEntry target, boolean add) throws IMQException {
      assert !target.isInvalid() : "value in RegionEntry should not be INVALID";
      DummyQRegion dQRegion = new DummyQRegion(rgn);
      dQRegion.setEntry(target);
      Object params[] = {dQRegion};
      ExecutionContext context = new ExecutionContext(params, this.cache);
      context.newScope(IndexCreationHelper.INDEX_QUERY_SCOPE_ID);
      try {
        if (this.dependencyGraph != null) {
          context.setDependencyGraph(dependencyGraph);
        }
        for (int i = 0; i < this.iteratorSize; i++) {
          CompiledIteratorDef iterDef = (CompiledIteratorDef) fromIterators.get(i);
          // We are re-using the same ExecutionContext on every evaluate -- this
          // is not how ExecutionContext was intended to be used.
          // Compute the dependency only once. The call to methods of this
          // class are thread safe as for update lock on Index is taken .
          if (this.dependencyGraph == null) {
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

        Support.Assert(this.indexResultSetType != null,
            "IMQEvaluator::evaluate:The StrcutType should have been initialized during index creation");

        doNestedIterations(0, add, context);

      } catch (TypeMismatchException tme) {
        if (tme.getRootCause() instanceof EntryDestroyedException) {
          // This code relies on current implementation of remove mapping, relying on behavior that
          // will force a
          // crawl through the index to remove the entry if it exists, even if it is not present at
          // the provided key
          removeMapping(QueryService.UNDEFINED, target);
        } else {
          throw new IMQException(tme);
        }
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
    public void initializeIndex(boolean loadEntries) throws IMQException {
      this.initEntriesUpdated = 0;
      try {
        // Since an index initialization can happen multiple times
        // for a given region, due to clear operation, we are using harcoded
        // scope ID of 1 , as otherwise if obtained from ExecutionContext
        // object, it will get incremented on very index initialization
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
     * This function is used to obtain Index data at the time of index creation. Each element of the
     * List is an Object Array of size 3. The 0th element of Object Array stores the value of Index
     * Expression. The 1st element of ObjectArray contains the RegionEntry object ( If the booelan
     * isFirstItrOnEntry is false, then the 0th iterator will give us the Region.Entry object which
     * can be used to obtain the underlying RegionEntry object. If the boolean is true & additional
     * projection attribute is not null, then the Region.Entry object can be obtained by evaluating
     * the additional projection attribute. If the boolean isFirstItrOnEntry is tru e& additional
     * projection attribute is null, then teh 0th iterator itself will evaluate to Region.Entry
     * Object.
     * 
     * The 2nd element of Object Array contains the Struct object ( tuple) created. If the boolean
     * isFirstItrOnEntry is false, then the first attribute of the Struct object is obtained by
     * evaluating the additional projection attribute.
     */
    private void applyProjectionForIndexInit(List currentRuntimeIters)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException, IMQException {
      if (QueryMonitor.isLowMemory()) {
        throw new IMQException(
            LocalizedStrings.IndexCreationMsg_CANCELED_DUE_TO_LOW_MEMORY.toLocalizedString());
      }

      Object indexKey = this.isFirstItrOnEntry ? this.indexedExpr.evaluate(this.initContext)
          : modifiedIndexExpr.evaluate(this.initContext);

      if (indexKey == null) {
        indexKey = IndexManager.NULL;
      }
      // if the first key is PdxString set the flag so that rest of the keys
      // would be converted to PdxString
      if (!isIndexedPdxKeysFlagSet) {
        setPdxStringFlag(indexKey);
      }
      indexKey = getPdxStringForIndexedPdxKeys(indexKey);
      LocalRegion.NonTXEntry temp = null;
      if (this.isFirstItrOnEntry && this.additionalProj != null) {
        temp = (LocalRegion.NonTXEntry) additionalProj.evaluate(this.initContext);
      } else {
        temp = (LocalRegion.NonTXEntry) (((RuntimeIterator) currentRuntimeIters.get(0))
            .evaluate(this.initContext));
      }
      RegionEntry re = temp.getRegionEntry();
      indexStore.addMapping(indexKey, re);
    }

    /**
     * @param add true if adding to index, false if removing
     */
    private void doNestedIterations(int level, boolean add, ExecutionContext context)
        throws TypeMismatchException, AmbiguousNameException, FunctionDomainException,
        NameResolutionException, QueryInvocationTargetException, IMQException {
      List iterList = context.getCurrentIterators();
      if (level == this.iteratorSize) {
        applyProjection(add, context);
      } else {
        RuntimeIterator rIter = (RuntimeIterator) iterList.get(level);
        // System.out.println("Level = "+level+" Iter = "+rIter.getDef());
        Collection c = rIter.evaluateCollection(context);
        if (c == null)
          return;
        Iterator cIter = c.iterator();
        while (cIter.hasNext()) {
          rIter.setCurrent(cIter.next());
          doNestedIterations(level + 1, add, context);
        }
      }
    }

    /**
     * @param add true if adding, false if removing from index
     */
    private void applyProjection(boolean add, ExecutionContext context)
        throws FunctionDomainException, TypeMismatchException, NameResolutionException,
        QueryInvocationTargetException, IMQException {
      Object indexKey = indexedExpr.evaluate(context);
      if (indexKey == null) {
        indexKey = IndexManager.NULL;
      }
      // if the first key is PdxString set the flag so that rest of the keys
      // would be converted to PdxString
      if (!isIndexedPdxKeysFlagSet) {
        setPdxStringFlag(indexKey);
      }
      indexKey = getPdxStringForIndexedPdxKeys(indexKey);
      RegionEntry entry = ((DummyQRegion) context.getBindArgument(1)).getEntry();
      // Get thread local reverse map if available.
      OldKeyValuePair oldKeyValuePair = null;
      if (oldKeyValue != null) {
        oldKeyValuePair = oldKeyValue.get();
      }

      if (add) {
        Object oldKey = null;
        Object oldValue = null;
        // Get Old keys to be removed.
        if (oldKeyValuePair != null) {
          oldKey = oldKeyValuePair.getOldKey();
          oldValue = oldKeyValuePair.getOldValue();
        }

        // Add new index entries
        // A null oldKey means this is a create
        // oldKey would be a NullToken in case of update
        if (oldKey == null) {
          indexStore.addMapping(indexKey, entry);
        } else {
          // Add new key and remove old
          indexStore.updateMapping(indexKey, oldKey, entry, oldValue);
          // reset the thread local as the update is done
          if (oldKeyValue != null) {
            oldKeyValue.remove();
          }
        }
      } else { // remove from forward and reverse maps
        // We will cleanup the index entry later.
        if (oldKeyValuePair != null) {
          oldKeyValuePair.setOldKeyValuePair(indexKey, entry);
        } else {
          indexStore.removeMapping(indexKey, entry);
        }
      }
    }

    // The struct type calculation is modified if the
    // 0th iterator is modified to make it dependent on Entry
    private ObjectType createIndexResultSetType() {
      List currentIterators = this.initContext.getCurrentIterators();
      int len = currentIterators.size();
      ObjectType type = null;
      ObjectType fieldTypes[] = new ObjectType[len];
      int start = this.isFirstItrOnEntry ? 0 : 1;
      for (; start < len; start++) {
        RuntimeIterator iter = (RuntimeIterator) currentIterators.get(start);
        fieldTypes[start] = iter.getElementType();
      }
      if (!this.isFirstItrOnEntry) {
        fieldTypes[0] = addnlProjType;
      }
      type = (len == 1) ? fieldTypes[0] : new StructTypeImpl(this.canonicalIterNames, fieldTypes);
      return type;
    }

    int getTotalEntriesUpdated() {
      return this.initEntriesUpdated;
    }

    public ObjectType getIndexResultSetType() {
      return this.indexResultSetType;
    }

    public List getAllDependentIterators() {
      return fromIterators;
    }
  }

  void lockedQuery(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator indpndntItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection) throws TypeMismatchException,
      FunctionDomainException, NameResolutionException, QueryInvocationTargetException {
    this.lockedQueryPrivate(key, operator, results, iterOps, indpndntItr, context, null, projAttrib,
        intermediateResults, isIntersection);
  }

  void lockedQuery(Object key, int operator, Collection results, Set keysToRemove,
      ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    this.lockedQueryPrivate(key, operator, results, null, null, context, keysToRemove, null, null,
        true);
  }

  @Override
  void addMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    // Only called from CompactMapRangeIndex
    indexStore.addMapping(key, entry);
  }

  public static void setTestHook(TestHook hook) {
    testHook = hook;
  }

  @Override
  void saveMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    // TODO Auto-generated method stub
  }

  public boolean isEmpty() {
    return indexStore.size() == 0 ? true : false;
  }

  @Override
  public Map getValueToEntriesMap() {
    throw new UnsupportedOperationException("valuesToEntriesMap should not be accessed directly");
  }

  public void addSavedMappings(RegionEntry entry) {

  }

  private class OldKeyValuePair {
    private Object oldKey;
    private Object oldValue;

    public void setOldKeyValuePair(Object oldKey, RegionEntry entry) {
      this.oldKey = oldKey;
      // We obtain the object currently in vm, we are using this old value
      // only to detect if in place modifications have occurred
      // if the object is not in memory, obviously an in place modification could
      // not have occurred
      this.oldValue = indexStore.getTargetObjectInVM(entry);
    }

    public Object getOldValue() {
      return oldValue;
    }

    public Object getOldKey() {
      return oldKey;
    }
  }
}


