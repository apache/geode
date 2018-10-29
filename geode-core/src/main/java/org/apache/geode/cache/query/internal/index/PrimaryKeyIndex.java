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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.CqEntry;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QueryMonitor;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.QueryUtils;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.internal.Support;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.pdx.internal.PdxString;

public class PrimaryKeyIndex extends AbstractIndex {

  protected long numUses = 0;
  ObjectType indexResultType;

  public PrimaryKeyIndex(InternalCache cache, String indexName, Region region, String fromClause,
      String indexedExpression, String projectionAttributes, String origFromClause,
      String origIndxExpr, String[] defintions, IndexStatistics indexStatistics) {
    super(cache, indexName, region, fromClause, indexedExpression, projectionAttributes,
        origFromClause, origIndxExpr, defintions, indexStatistics);
    // TODO: Check if the below is correct
    Class constr = region.getAttributes().getValueConstraint();
    if (constr == null)
      constr = Object.class;
    this.indexResultType = new ObjectTypeImpl(constr);
    markValid(true);
  }

  public IndexType getType() {
    return IndexType.PRIMARY_KEY;
  }

  @Override
  protected boolean isCompactRangeIndex() {
    return false;
  }

  public ObjectType getResultSetType() {
    return this.indexResultType;
  }

  void removeMapping(RegionEntry entry, int opCode) {}

  @Override
  void addMapping(RegionEntry entry) throws IMQException {}

  @Override
  void instantiateEvaluator(IndexCreationHelper indexCreationHelper) {}

  @Override
  void lockedQuery(Object key, int operator, Collection results, Set keysToRemove,
      ExecutionContext context) throws TypeMismatchException {
    assert keysToRemove == null;

    int limit = -1;

    // Key cannot be PdxString in a region
    if (key instanceof PdxString) {
      key = key.toString();
    }

    Boolean applyLimit = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX);
    if (applyLimit != null && applyLimit) {
      limit = (Integer) context.cacheGet(CompiledValue.RESULT_LIMIT);
    }
    QueryObserver observer = QueryObserverHolder.getInstance();
    if (limit != -1 && results.size() == limit) {
      observer.limitAppliedAtIndexLevel(this, limit, results);
      return;
    }

    switch (operator) {
      case OQLLexerTokenTypes.TOK_EQ: {
        if (key != null && key != QueryService.UNDEFINED) {
          Region.Entry entry = ((LocalRegion) getRegion()).accessEntry(key, false);
          if (entry != null) {
            Object value = entry.getValue();
            if (value != null) {
              addResultToResults(context, results, key, value);
            }
          }
        }
        break;
      }
      case OQLLexerTokenTypes.TOK_NE_ALT:
      case OQLLexerTokenTypes.TOK_NE: { // add all btree values
        Set values = (Set) getRegion().values();
        // Add data one more than the limit
        if (limit != -1) {
          ++limit;
        }
        // results.addAll(values);
        Iterator iter = values.iterator();
        while (iter.hasNext()) {
          // Check if query execution on this thread is canceled.
          QueryMonitor.throwExceptionIfQueryOnCurrentThreadIsCanceled();
          addResultToResults(context, results, key, iter.next());
          if (limit != -1 && results.size() == limit) {
            observer.limitAppliedAtIndexLevel(this, limit, results);
            return;
          }
        }

        boolean removeOneRow = limit != -1;
        if (key != null && key != QueryService.UNDEFINED) {
          Region.Entry entry = ((LocalRegion) getRegion()).accessEntry(key, false);
          if (entry != null) {
            if (entry.getValue() != null) {
              results.remove(entry.getValue());
              removeOneRow = false;
            }
          }
        }
        if (removeOneRow) {
          Iterator itr = results.iterator();
          if (itr.hasNext()) {
            itr.next();
            itr.remove();
          }
        }
        break;
      }
      default: {
        throw new IllegalArgumentException(
            "Invalid Operator");
      }
    } // end switch
    numUses++;
  }

  void lockedQuery(Object key, int operator, Collection results, CompiledValue iterOps,
      RuntimeIterator runtimeItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection) throws TypeMismatchException,
      FunctionDomainException, NameResolutionException, QueryInvocationTargetException {

    QueryObserver observer = QueryObserverHolder.getInstance();
    int limit = -1;

    Boolean applyLimit = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_LIMIT_AT_INDEX);

    if (applyLimit != null && applyLimit.booleanValue()) {
      limit = ((Integer) context.cacheGet(CompiledValue.RESULT_LIMIT)).intValue();
    }
    if (limit != -1 && results.size() == limit) {
      observer.limitAppliedAtIndexLevel(this, limit, results);
      return;
    }
    // Key cannot be PdxString in a region
    if (key instanceof PdxString) {
      key = key.toString();
    }
    switch (operator) {
      case OQLLexerTokenTypes.TOK_EQ: {
        if (key != null && key != QueryService.UNDEFINED) {
          Region.Entry entry = ((LocalRegion) getRegion()).accessEntry(key, false);
          if (entry != null) {
            Object value = entry.getValue();
            if (value != null) {
              boolean ok = true;
              if (runtimeItr != null) {
                runtimeItr.setCurrent(value);
                ok = QueryUtils.applyCondition(iterOps, context);
              }
              if (ok) {
                applyCqOrProjection(projAttrib, context, results, value, intermediateResults,
                    isIntersection, key);
              }
            }
          }
        }
        break;
      }

      case OQLLexerTokenTypes.TOK_NE_ALT:
      case OQLLexerTokenTypes.TOK_NE: { // add all btree values
        Set entries = (Set) getRegion().entrySet();
        Iterator itr = entries.iterator();
        while (itr.hasNext()) {
          Map.Entry entry = (Map.Entry) itr.next();

          if (key != null && key != QueryService.UNDEFINED && key.equals(entry.getKey())) {
            continue;
          }
          Object val = entry.getValue();
          // TODO: is this correct. What should be the behaviour of null values?
          if (val != null) {
            boolean ok = true;
            if (runtimeItr != null) {
              runtimeItr.setCurrent(val);
              ok = QueryUtils.applyCondition(iterOps, context);
            }
            if (ok) {
              applyCqOrProjection(projAttrib, context, results, val, intermediateResults,
                  isIntersection, key);
            }
            if (limit != -1 && results.size() == limit) {
              observer.limitAppliedAtIndexLevel(this, limit, results);
              break;
            }
          }
        }
        break;
      }
      default: {
        throw new IllegalArgumentException("Invalid Operator");
      }
    } // end switch
    numUses++;
  }

  void recreateIndexData() throws IMQException {
    Support.Assert(false,
        "PrimaryKeyIndex::recreateIndexData: This method should not have got invoked at all");
  }

  public boolean clear() throws QueryException {
    return true;
  }

  private void addResultToResults(ExecutionContext context, Collection results, Object key,
      Object result) {
    if (context != null && context.isCqQueryContext()) {
      results.add(new CqEntry(key, result));
    } else {
      results.add(result);
    }
  }

  protected InternalIndexStatistics createStats(String indexName) {
    return new PrimaryKeyIndexStatistics();
  }

  class PrimaryKeyIndexStatistics extends InternalIndexStatistics {
    /**
     * Returns the total number of times this index has been accessed by a query.
     */
    public long getTotalUses() {
      return numUses;
    }

    /**
     * Returns the number of keys in this index.
     */
    public long getNumberOfKeys() {
      return getRegion().keySet().size();
    }

    /**
     * Returns the number of values in this index.
     */
    public long getNumberOfValues() {
      return getRegion().values().size();
    }

    /**
     * Return the number of values for the specified key in this index.
     */
    public long getNumberOfValues(Object key) {
      if (getRegion().containsValueForKey(key))
        return 1;
      return 0;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("No Keys = ").append(getNumberOfKeys()).append("\n");
      sb.append("No Values = ").append(getNumberOfValues()).append("\n");
      sb.append("No Uses = ").append(getTotalUses()).append("\n");
      sb.append("No Updates = ").append(getNumUpdates()).append("\n");
      sb.append("Total Update time = ").append(getTotalUpdateTime()).append("\n");
      return sb.toString();
    }
  }

  @Override
  void lockedQuery(Object lowerBoundKey, int lowerBoundOperator, Object upperBoundKey,
      int upperBoundOperator, Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException {
    throw new UnsupportedOperationException(
        "For a PrimaryKey Index , a range has no meaning");

  }

  public int getSizeEstimate(Object key, int op, int matchLevel) {
    return 1;
  }

  @Override
  void addMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    // do nothing
  }

  @Override
  void saveMapping(Object key, Object value, RegionEntry entry) throws IMQException {
    // Do Nothing; We are not going to call this for PrimaryKeyIndex ever.
  }

  public boolean isEmpty() {
    return createStats("primaryKeyIndex").getNumberOfKeys() == 0 ? true : false;
  }
}
