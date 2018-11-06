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
package org.apache.geode.cache.query.internal;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.IndexData;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.query.internal.index.IndexUtils;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.pdx.internal.PdxString;

public class CompiledIn extends AbstractCompiledValue implements Indexable {
  private static final Logger logger = LogService.getLogger();

  private CompiledValue elm;
  private CompiledValue colln;

  public CompiledIn(CompiledValue elm, CompiledValue colln) {
    this.elm = elm;
    this.colln = colln;
  }

  @Override
  public List getChildren() {
    List list = new ArrayList();
    list.add(elm);
    list.add(colln);
    return list;
  }

  public int getType() {
    return LITERAL_in;
  }

  /**
   * We retrieve the collection from the context cache if it exists This allows us to not have to
   * reevaluate the sub query on every iteration. This improves performance for queries such as
   * "select * from /receipts r where r.type = 'large' and r.id in (select c.id from /customers c
   * where c.status = 'preferred') The sub query would create a set that would not change and store
   * it into the context if it does not yet exist
   */
  private Object evaluateColln(ExecutionContext context) throws QueryInvocationTargetException,
      NameResolutionException, TypeMismatchException, FunctionDomainException {
    Object evalColln = null;
    if (this.colln.isDependentOnCurrentScope(context)) {
      evalColln = this.colln.evaluate(context);
    } else {
      evalColln = context.cacheGet(this.colln);
      if (evalColln == null) {
        evalColln = this.colln.evaluate(context);
        context.cachePut(this.colln, evalColln);
      }
    }
    return evalColln;
  }

  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    Object evalElm = this.elm.evaluate(context);

    Object evalColln = evaluateColln(context);

    if (evalColln == null || evalColln == QueryService.UNDEFINED) {
      return QueryService.UNDEFINED;
    }

    // handle each type of collection that we support
    if (evalColln instanceof Map) {
      evalColln = ((Map) evalColln).entrySet();
    }

    if (evalColln instanceof Collection) {
      Iterator iterator = ((Iterable) evalColln).iterator();
      while (iterator.hasNext()) {
        Object evalObj = evalElm;
        Object collnObj = iterator.next();
        if (evalElm instanceof PdxString && collnObj instanceof String) {
          evalObj = ((PdxString) evalElm).toString();
        } else if (collnObj instanceof PdxString && evalElm instanceof String) {
          collnObj = ((PdxString) collnObj).toString();
        }
        if (TypeUtils.compare(evalObj, collnObj, OQLLexerTokenTypes.TOK_EQ).equals(Boolean.TRUE)) {
          return Boolean.TRUE;
        }
      }
      return Boolean.FALSE;
    }

    if (!evalColln.getClass().isArray()) {
      throw new TypeMismatchException(
          String.format("Operand of IN cannot be interpreted as a Collection. Is instance of %s",
              evalColln.getClass().getName()));
    }
    if (evalColln.getClass().getComponentType().isPrimitive()) {
      if (evalElm == null) {
        throw new TypeMismatchException(
            "IN operator, check for null IN primitive array");
      }
    }

    int numElements = Array.getLength(evalColln);
    for (int i = 0; i < numElements; i++) {
      Object o = Array.get(evalColln, i);
      if (TypeUtils.compare(evalElm, o, TOK_EQ).equals(Boolean.TRUE)) {
        return Boolean.TRUE;
      }
    }

    return Boolean.FALSE;
  }

  /**
   * If the size of aray is two this implies that it is a relation ship index & so the key field
   * will be null in both the indexes as key is not a meaningful entity. The 0th element will refer
   * to LHS operand and 1th element will refer to RHS operannd
   */
  public IndexInfo[] getIndexInfo(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    IndexInfo[] indexInfo = privGetIndexInfo(context);
    if (indexInfo != null) {
      // TODO: == check is identity only
      if (indexInfo == NO_INDEXES_IDENTIFIER) {
        return null;
      } else {
        return indexInfo;
      }
    }
    if (!IndexUtils.indexesEnabled)
      return null;
    // get the path and index key to try
    PathAndKey pAndK = getPathAndKey(context);
    IndexInfo newIndexInfo[] = null;
    if (pAndK != null) {
      CompiledValue path = pAndK._path;
      CompiledValue indexKey = pAndK._key;
      IndexData indexData = QueryUtils.getAvailableIndexIfAny(path, context, TOK_EQ);
      IndexProtocol index = null;
      if (indexData != null) {
        index = indexData.getIndex();
      }
      if (index != null && index.isValid()) {
        newIndexInfo = new IndexInfo[1];
        newIndexInfo[0] = new IndexInfo(indexKey, path, index, indexData.getMatchLevel(),
            indexData.getMapping(), TOK_EQ);
      }
    }
    if (newIndexInfo != null) {
      privSetIndexInfo(newIndexInfo, context);
    } else {
      privSetIndexInfo(NO_INDEXES_IDENTIFIER, context);
    }
    return newIndexInfo;
  }

  /**
   * _indexInfo is a transient field if this is just faulted in then can be null
   */
  private IndexInfo[] privGetIndexInfo(ExecutionContext context) {
    return (IndexInfo[]) context.cacheGet(this);
  }

  private void privSetIndexInfo(IndexInfo[] indexInfo, ExecutionContext context) {
    context.cachePut(this, indexInfo);
  }

  /**
   * Invariant: the receiver is dependent on the current iterator.
   */
  protected PlanInfo protGetPlanInfo(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    PlanInfo result = new PlanInfo();
    IndexInfo[] indexInfo = getIndexInfo(context);
    if (indexInfo == null)
      return result;
    for (int i = 0; i < indexInfo.length; ++i) {
      result.indexes.add(indexInfo[i]._index);
    }
    result.evalAsFilter = true;
    String preferredCondn = (String) context.cacheGet(PREF_INDEX_COND);
    if (preferredCondn != null) {
      // This means that the system is having only one independent iterator so equi join is ruled
      // out.
      // thus the first index is guaranteed to be on the condition which may match our preferred
      // index
      if (indexInfo[0]._index.getCanonicalizedIndexedExpression().equals(preferredCondn)
          && (indexInfo[0]._index.getType() == IndexType.FUNCTIONAL
              || indexInfo[0]._index.getType() == IndexType.HASH)) {
        result.isPreferred = true;
      }
    }
    return result;
  }

  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    context.addDependencies(this, this.elm.computeDependencies(context));
    return context.addDependencies(this, this.colln.computeDependencies(context));
  }

  /**
   * specialized optimization for doing a bulk get on a region.
   *
   * @return a List of entries if optimization was performed, null if no match
   */
  List optimizeBulkGet(CompiledRegion cRgn, ExecutionContext context)
      throws RegionNotFoundException, TypeMismatchException, NameResolutionException,
      FunctionDomainException, QueryInvocationTargetException {
    // check the elm to see if it's the key on a map entry
    boolean match = false;
    CompiledValue resolution = null;

    if (this.elm instanceof CompiledID) {
      String id = ((CompiledID) this.elm).getId();
      if (id.equals("key") || id.equals("getKey")) {
        resolution = context.resolve(id);
        if (resolution instanceof CompiledPath) {
          resolution = ((CompiledPath) resolution).getReceiver();
        }
      }
    } else if (this.elm instanceof CompiledPath) {
      CompiledPath cPath = (CompiledPath) this.elm;
      if (cPath.getTailID().equals("key") || cPath.getTailID().equals("getKey")) {
        CompiledValue cVal = cPath.getReceiver();
        if (cVal instanceof CompiledID) {
          resolution = context.resolve(((CompiledID) cVal).getId());
        }
      }
    } else if (this.elm instanceof CompiledOperation) {
      CompiledOperation cOp = (CompiledOperation) this.elm;
      if (cOp.getMethodName().equals("key") || cOp.getMethodName().equals("getKey")) {
        if (cOp.getReceiver(context) instanceof CompiledID) {
          resolution = context.resolve(((CompiledID) cOp.getReceiver(context)).getId());
        } else if (cOp.getReceiver(context) == null) {
          match = true; // implicit operation on the iterator
        }
      }
    }
    // only one possible iterator in this case, so it's a match if resolution
    // is a RuntimeIterator
    if (!match && !(resolution instanceof RuntimeIterator)) {
      return null;
    }

    // element matches; finally, check to make sure the collection expression
    // is independent of all iterators
    if (context.isDependentOnAnyIterator(this.colln)) {
      return null;
    }

    // evaluate the collection
    Object evalColln = evaluateColln(context);
    if (evalColln == null || evalColln == QueryService.UNDEFINED) {
      return null;
    }

    // only handle an actual Collection or an Object[] for this optimization
    Collection colln = null;
    if (evalColln instanceof Collection) {
      colln = (Collection) evalColln;
    }
    if (evalColln instanceof Object[]) {
      colln = Arrays.asList((Object[]) evalColln);
    }

    if (colln != null) {
      QRegion rgn = (QRegion) cRgn.evaluate(context);

      // only do this optimization if the region keys is larger than the
      // collection
      if (rgn.keySet().size() < colln.size()) {
        return null;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Executing BULK GET on keys {}, in region {}", colln, rgn);
      }
      List results = new ArrayList(colln.size());

      // now do the iteration over this collection
      for (Iterator itr = colln.iterator(); itr.hasNext();) {
        Object key = itr.next();
        Region.Entry entry = rgn.getEntry(key);
        if (entry != null) {
          // the region contains this key, so add the entry to the results
          results.add(entry);
        }
      }
      return results;
    }
    return null;
  }

  /**
   * get the path to see if there's an index for, and also determine which CompiledValue is the key
   * while we're at it
   */
  private PathAndKey getPathAndKey(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException {

    boolean isLeftDependent = context.isDependentOnCurrentScope(this.elm);
    boolean isRightDependent = context.isDependentOnCurrentScope(this.colln);
    if (!isLeftDependent || isRightDependent)
      return null;
    CompiledValue indexKey;
    CompiledValue path;
    path = this.elm;
    indexKey = this.colln;
    // Do not worry about the nature of the collection. As long as it
    // is not dependent on the current scope we should be fine

    return new PathAndKey(path, indexKey);
  }

  /**
   * Evaluates as a filter taking advantage of indexes if appropriate. This function has a
   * meaningful implementation only in CompiledComparison & CompiledUndefined . It is unsupported in
   * other classes. The additional parameters which it takes are a boolean which is used to indicate
   * whether the index result set needs to be expanded to the top level or not. The second is a
   * CompiledValue representing the operands which are only iter evaluatable. The CompiledValue
   * passed will be null except if a GroupJunction has only one filter evaluatable condition & rest
   * are iter operands. In such cases , the iter operands will be evaluated while expanding/cutting
   * down the index resultset
   *
   */
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults,
      boolean completeExpansionNeeded, CompiledValue iterOperands, RuntimeIterator[] indpndntItrs,
      boolean isIntersection, boolean conditioningNeeded, boolean evalProj)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // see if we're dependent on the current iterator
    // if not let super handle it
    if (!isDependentOnCurrentScope(context)) {
      return super.filterEvaluate(context, intermediateResults);
    }
    IndexInfo[] idxInfo = getIndexInfo(context);
    Support.Assert(idxInfo != null,
        "a comparison that is dependent, not indexed, and filter evaluated is not possible");
    Support.Assert(idxInfo.length == 1, "In operator should have only one index");

    return singleBaseCollectionFilterEvaluate(context, intermediateResults, completeExpansionNeeded,
        iterOperands, idxInfo[0], indpndntItrs, isIntersection, conditioningNeeded, evalProj);

  }

  public int getOperator() {
    return TOK_EQ;
  }

  private void queryIndex(Object key, IndexInfo indexInfo, SelectResults results,
      CompiledValue iterOperands, RuntimeIterator[] indpndntItrs, ExecutionContext context,
      List projAttrib, boolean conditioningNeeded) throws TypeMismatchException,
      FunctionDomainException, NameResolutionException, QueryInvocationTargetException {

    assert indexInfo != null;
    assert indexInfo._index != null;

    // Get new IndexInfo to put in context as we dont want it to evaluate
    // key collection again if its a CompiledSelect (Nested Query).
    IndexInfo contextIndexInfo = new IndexInfo(new CompiledLiteral(key), indexInfo._path(),
        indexInfo._getIndex(), 0, null, indexInfo._operator());
    context.cachePut(CompiledValue.INDEX_INFO, contextIndexInfo);
    indexInfo._index.query(key, TOK_EQ, results, !conditioningNeeded ? iterOperands : null,
        indpndntItrs == null ? null : indpndntItrs[0], context, projAttrib, null, false);
  }

  /**
   * evaluate as a filter, involving a single iterator. Use an index if possible.
   *
   * Invariant: the receiver is dependent on the current iterator.
   */
  SelectResults singleBaseCollectionFilterEvaluate(ExecutionContext context,
      SelectResults intermediateResults, boolean completeExpansionNeeded,
      CompiledValue iterOperands, IndexInfo indexInfo, RuntimeIterator[] indpndntItr,
      boolean isIntersection, boolean conditioningNeeded, boolean evalProj)
      throws TypeMismatchException, AmbiguousNameException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    ObjectType resultType = indexInfo._index.getResultSetType();
    int indexFieldsSize = -1;
    SelectResults results = null;
    if (resultType instanceof StructType) {
      indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
    } else {
      indexFieldsSize = 1;
    }
    boolean useLinkedDataStructure = false;
    boolean nullValuesAtStart = true;
    Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
    if (orderByClause != null && orderByClause) {
      List orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
      useLinkedDataStructure = orderByAttrs.size() == 1;
      nullValuesAtStart = !((CompiledSortCriterion) orderByAttrs.get(0)).getCriterion();
    }

    List projAttrib = null;

    ObjectType projResultType = null;
    if (!conditioningNeeded) {
      projResultType = evalProj ? (ObjectType) context.cacheGet(RESULT_TYPE) : null;
      if (projResultType != null) {
        resultType = projResultType;
        context.cachePut(RESULT_TYPE, Boolean.TRUE);
        projAttrib = (List) context.cacheGet(PROJ_ATTRIB);
      }
      if (isIntersection) {
        if (resultType instanceof StructType) {
          context.getCache().getLogger()
              .fine("StructType resultType.class=" + resultType.getClass().getName());
          if (useLinkedDataStructure) {
            results = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) resultType)
                : new SortedResultsBag<Struct>((StructTypeImpl) resultType, nullValuesAtStart);
          } else {
            results = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
          }
          indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
        } else {
          context.getCache().getLogger()
              .fine("non-StructType resultType.class=" + resultType.getClass().getName());
          if (useLinkedDataStructure) {
            results = context.isDistinct() ? new LinkedResultSet(resultType)
                : new SortedResultsBag(resultType, nullValuesAtStart);
          } else {
            results = QueryUtils.createResultCollection(context, resultType);
          }
          indexFieldsSize = 1;
        }
      } else {
        if (intermediateResults != null && !completeExpansionNeeded) {
          results = intermediateResults;
        } else {
          if (resultType instanceof StructType) {
            context.getCache().getLogger()
                .fine("StructType resultType.class=" + resultType.getClass().getName());
            if (useLinkedDataStructure) {
              results = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) resultType)
                  : new SortedResultsBag<Struct>((StructTypeImpl) resultType, nullValuesAtStart);
            } else {
              results = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
            }
            indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
          } else {
            context.getCache().getLogger()
                .fine("non-StructType resultType.class=" + resultType.getClass().getName());
            if (useLinkedDataStructure) {
              results = context.isDistinct() ? new LinkedResultSet(resultType)
                  : new SortedResultsBag(resultType, nullValuesAtStart);
            } else {
              results = QueryUtils.createResultCollection(context, resultType);
            }
            indexFieldsSize = 1;
          }
        }
      }

    } else {
      if (resultType instanceof StructType) {
        context.getCache().getLogger()
            .fine("StructType resultType.class=" + resultType.getClass().getName());
        if (useLinkedDataStructure) {
          results = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) resultType)
              : new SortedResultsBag<Struct>((StructTypeImpl) resultType, nullValuesAtStart);
        } else {
          results = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
        }
        indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
      } else {
        context.getCache().getLogger()
            .fine("non-StructType resultType.class=" + resultType.getClass().getName());
        if (useLinkedDataStructure) {
          results = context.isDistinct() ? new LinkedResultSet(resultType)
              : new SortedResultsBag(resultType, nullValuesAtStart);
        } else {
          results = QueryUtils.createResultCollection(context, resultType);
        }
        indexFieldsSize = 1;
      }
    }

    QueryObserver observer = QueryObserverHolder.getInstance();
    try {
      Object evalColln = evaluateColln(context);
      observer.beforeIndexLookup(indexInfo._index, TOK_EQ, evalColln);
      // We need to reset the result type just in case the colln turned out to
      // be a compiled comparison which could change the result type
      // Exec caches are incorrectly shared across all queries, this would result
      // in overriding the result type. Once the result type was overridden
      // multiple projections and class cast exceptions could result due to
      // unexpected values overwriting expected values
      if (!conditioningNeeded) {
        if (projResultType != null) {
          resultType = projResultType;
          context.cachePut(RESULT_TYPE, Boolean.TRUE);
        }
      }
      // handle each type of collection that we support
      if (evalColln instanceof Map) {
        Iterator itr = ((Map) evalColln).entrySet().iterator();
        while (itr.hasNext()) {
          this.queryIndex(itr.next(), indexInfo, results, iterOperands, indpndntItr, context,
              projAttrib, conditioningNeeded);
        }

      } else if (evalColln instanceof Collection) {
        Object key = indexInfo.evaluateIndexKey(context);
        // If the index is a map index, the key is actually an object[] tuple that contains the map
        // key in the [1]
        // and the evalColln in the [0] position
        if (key instanceof Object[]) {
          Iterator iterator = ((Iterable) ((Object[]) key)[0]).iterator();
          while (iterator.hasNext()) {
            this.queryIndex(new Object[] {iterator.next(), ((Object[]) key)[1]}, indexInfo, results,
                iterOperands, indpndntItr, context, projAttrib, conditioningNeeded);
          }
        } else {
          // Removing duplicates from the collection
          HashSet set = new HashSet((Collection) evalColln);
          Iterator itr = set.iterator();
          while (itr.hasNext()) {
            this.queryIndex(itr.next(), indexInfo, results, iterOperands, indpndntItr, context,
                projAttrib, conditioningNeeded);
          }
        }
      } else {

        if (!evalColln.getClass().isArray()) {
          throw new TypeMismatchException("Operand of IN cannot be interpreted as a Collection. "
              + "Is instance of " + evalColln.getClass().getName());
        }

        int evalCollnLength = Array.getLength(evalColln);
        for (int i = 0; i < evalCollnLength; ++i) {
          this.queryIndex(Array.get(evalColln, i), indexInfo, results, iterOperands, indpndntItr,
              context, projAttrib, conditioningNeeded);
        }
      }

      if (conditioningNeeded) {
        results = QueryUtils.getConditionedIndexResults(results, indexInfo, context,
            indexFieldsSize, completeExpansionNeeded, iterOperands, indpndntItr);
      } else {
        if (isIntersection && intermediateResults != null) {
          results = QueryUtils.intersection(intermediateResults, results, context);
        }
      }
      return results;
    } finally {
      observer.afterIndexLookup(results);
    }
  }

  public boolean isProjectionEvaluationAPossibility(ExecutionContext context) {
    return true;
  }

  @Override
  public boolean isLimitApplicableAtIndexLevel(ExecutionContext context) {
    return true;
  }

  @Override
  public boolean isOrderByApplicableAtIndexLevel(ExecutionContext context,
      String canonicalizedOrderByClause) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    return false;
  }

  public boolean isConditioningNeededForIndex(RuntimeIterator independentIter,
      ExecutionContext context, boolean completeExpnsNeeded)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    IndexConditioningHelper ich = null;
    IndexInfo[] idxInfo = getIndexInfo(context);
    assert idxInfo.length == 1;
    int indexFieldsSize = -1;
    boolean conditioningNeeded = true;
    ObjectType indexRsltType = idxInfo[0]._index.getResultSetType();
    if (indexRsltType instanceof StructType) {
      indexFieldsSize = ((StructTypeImpl) indexRsltType).getFieldNames().length;
    } else {
      indexFieldsSize = 1;
    }

    if (independentIter != null && indexFieldsSize == 1) {
      ich = new IndexConditioningHelper(idxInfo[0], context, indexFieldsSize, completeExpnsNeeded,
          null, independentIter);
    }
    conditioningNeeded = ich == null || ich.shufflingNeeded;
    return conditioningNeeded;
  }

  /**
   * evaluate as a filter, producing an intermediate result set. This may require iteration if there
   * is no index available. The boolean true implies that CompiledComparison when existing on its
   * own always requires a CompleteExpansion to top level iterators. This flag can get toggled to
   * false only from inside a GroupJunction
   *
   * @param intermediateResults if this parameter is provided, and we have to iterate, then iterate
   *        over this result set instead of the entire base collection.
   */
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // This function can be invoked only if the where clause contains
    // a single condition which is CompiledComparison.
    // If a CompiledComparison exists inside a GroupJunction, then it will
    // always
    // call the overloaded filterEvalauate with the RuntimeIterator passed
    // as not null.
    // Thus if the RuntimeIterator array passed is null then it is
    // guaranteed
    // that the condition was a isolatory condition ( directly in where
    // clause)
    // and the final iterators to which we need to expand to is all the
    // iterators
    // of the scope
    RuntimeIterator indpndntItr = null;
    List currentScopeIndpndntItrs = context.getAllIndependentIteratorsOfCurrentScope();
    Set rntmItrs = QueryUtils.getCurrentScopeUltimateRuntimeIteratorsIfAny(this, context);
    if (rntmItrs.size() == 1 && currentScopeIndpndntItrs.size() == 1) {
      indpndntItr = (RuntimeIterator) rntmItrs.iterator().next();
    }

    /*
     * It is safe to pass null as the independent iterator to which the condition belongs is
     * required only if boolean complete expansion turns out to be false, which can happen only in
     * case of CompiledComparison/CompiledUndefined called from GroupJunction or
     * CompositeGroupJunction
     */
    return filterEvaluate(context, intermediateResults, true, null,
        indpndntItr != null ? new RuntimeIterator[] {indpndntItr} : null, true,
        this.isConditioningNeededForIndex(indpndntItr, context, true), true);
  }

  /**
   * This function should never get invoked as now if a CompiledJunction or GroupJunction contains a
   * single filterable CompiledComparison it should directly call filterEvaluate rather than
   * auxFilterEvalutae. Overriding this function just for ensuring that auxFilterEvaluate is not
   * being called by mistake.
   */
  public SelectResults auxFilterEvaluate(ExecutionContext context,
      SelectResults intermediateResults) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    Support.assertionFailed(" This auxFilterEvaluate of CompiledIn should never have got invoked.");
    return null;
  }

  public boolean isBetterFilter(Filter comparedTo, ExecutionContext context, final int thisSize)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // If the current filter is equality & comparedTo filter is also equality based , then
    // return the one with lower size estimate is better
    boolean isThisBetter = true;

    int thatSize = comparedTo.getSizeEstimate(context);
    int thatOperator = comparedTo.getOperator();

    // Go with the lowest cost when hint is used.
    if (context instanceof QueryExecutionContext && ((QueryExecutionContext) context).hasHints()) {
      return thisSize <= thatSize;
    }

    switch (thatOperator) {
      case TOK_EQ:
      case TOK_NE:
      case TOK_NE_ALT:
        isThisBetter = thisSize < thatSize;
        break;
      case LITERAL_and:
        // Give preference to IN . Is this right? It does not appear . Ideally we need to get
        // some estimate on Range. This case is possible only in case of RangeJunction
        break;
      case TOK_LE:
      case TOK_LT:
      case TOK_GE:
      case TOK_GT:
        // Give preference to this rather than that as this is more deterministic
        break;
      default:
        throw new IllegalArgumentException("The operator type =" + thatOperator + " is unknown");
    }

    return isThisBetter;
  }

  public int getSizeEstimate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    IndexInfo[] idxInfo = getIndexInfo(context);
    if (idxInfo == null) {
      // This implies it is an independent condition. So evaluate it first in filter operand
      return 0;
    }
    assert idxInfo.length == 1;
    Object key = idxInfo[0].evaluateIndexKey(context);

    if (key != null && key.equals(QueryService.UNDEFINED)) {
      return 0;
    }

    if (context instanceof QueryExecutionContext) {
      QueryExecutionContext qcontext = (QueryExecutionContext) context;
      if (qcontext.isHinted(idxInfo[0]._index.getName())) {
        return qcontext.getHintSize(idxInfo[0]._index.getName());
      }
    }

    Object evalColln = evaluateColln(context);

    int size = 0;
    // handle each type of collection that we support
    if (evalColln instanceof Map) {
      Iterator itr = ((Map) evalColln).entrySet().iterator();
      while (itr.hasNext()) {
        size += idxInfo[0]._index.getSizeEstimate(itr.next(), TOK_EQ, idxInfo[0]._matchLevel);
      }

    } else if (evalColln instanceof Collection) {
      if (key instanceof Object[]) {
        Iterator iterator = ((ResultsSet) ((Object[]) key)[0]).iterator();
        while (iterator.hasNext()) {
          size += idxInfo[0]._index.getSizeEstimate(
              new Object[] {iterator.next(), ((Object[]) key)[1]}, TOK_EQ, idxInfo[0]._matchLevel);
        }
      } else {

        Iterator itr = ((Collection) evalColln).iterator();
        while (itr.hasNext()) {
          size += idxInfo[0]._index.getSizeEstimate(itr.next(), TOK_EQ, idxInfo[0]._matchLevel);
        }
      }
    } else {
      if (!evalColln.getClass().isArray()) {
        throw new TypeMismatchException("Operand of IN cannot be interpreted as a Collection. "
            + "Is instance of " + evalColln.getClass().getName());
      }
      if (evalColln instanceof Object[]) {
        Object[] arr = (Object[]) evalColln;
        for (int i = 0; i < arr.length; ++i) {
          size += idxInfo[0]._index.getSizeEstimate(arr[i], TOK_EQ, idxInfo[0]._matchLevel);
        }

      } else if (evalColln instanceof long[]) {
        long[] a = (long[]) evalColln;
        for (int i = 0; i < a.length; i++) {
          size += idxInfo[0]._index.getSizeEstimate(a[i], TOK_EQ, idxInfo[0]._matchLevel);
        }

      } else if (evalColln instanceof double[]) {
        double[] a = (double[]) evalColln;
        for (int i = 0; i < a.length; i++) {
          size += idxInfo[0]._index.getSizeEstimate(a[i], TOK_EQ, idxInfo[0]._matchLevel);
        }

      } else if (evalColln instanceof float[]) {
        float[] a = (float[]) evalColln;
        for (int i = 0; i < a.length; i++) {
          size += idxInfo[0]._index.getSizeEstimate(a[i], TOK_EQ, idxInfo[0]._matchLevel);
        }

      } else if (evalColln instanceof int[]) {
        int[] a = (int[]) evalColln;
        for (int i = 0; i < a.length; i++) {
          size += idxInfo[0]._index.getSizeEstimate(a[i], TOK_EQ, idxInfo[0]._matchLevel);
        }
      } else if (evalColln instanceof short[]) {
        short[] a = (short[]) evalColln;
        for (int i = 0; i < a.length; i++) {
          size += idxInfo[0]._index.getSizeEstimate(a[i], TOK_EQ, idxInfo[0]._matchLevel);
        }

      } else if (evalColln instanceof char[]) {
        char[] a = (char[]) evalColln;
        for (int i = 0; i < a.length; i++) {
          size += idxInfo[0]._index.getSizeEstimate(a[i], TOK_EQ, idxInfo[0]._matchLevel);
        }

      } else if (evalColln instanceof byte[]) {
        byte[] a = (byte[]) evalColln;
        for (int i = 0; i < a.length; i++) {
          size += idxInfo[0]._index.getSizeEstimate(a[i], TOK_EQ, idxInfo[0]._matchLevel);
        }

      } else {
        throw new TypeMismatchException(
            "Operand of IN cannot be interpreted as a Comparable Object. Operand is of type ="
                + evalColln.getClass());
      }
    }
    return size;
  }

  public boolean isRangeEvaluatable() {
    return false;
  }

  static class PathAndKey {

    CompiledValue _path;
    CompiledValue _key;

    PathAndKey(CompiledValue path, CompiledValue indexKey) {
      _path = path;
      _key = indexKey;
    }
  }
}
