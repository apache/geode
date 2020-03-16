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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
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
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxString;

/**
 * Comparison value: <, >, <=, >=, <>, =
 *
 */
public class CompiledComparison extends AbstractCompiledValue
    implements Negatable, OQLLexerTokenTypes, Indexable {

  // persistent inst vars
  public final CompiledValue _left;
  public final CompiledValue _right;
  private int _operator;

  // List groupRuntimeItrs = null;
  // List definitions = null;
  CompiledComparison(CompiledValue left, CompiledValue right, int op) {
    // invariant:
    // operator must be one of <,>,<=,>=,=,<>
    Support.Assert(op == TOK_LT || op == TOK_LE || op == TOK_GT || op == TOK_GE || op == TOK_EQ
        || op == TOK_NE, String.valueOf(op));
    _left = left;
    _right = right;
    _operator = op;
  }

  /* ******** CompiledValue Methods **************** */
  @Override
  public List getChildren() {
    List list = new ArrayList();
    list.add(_left);
    list.add(_right);
    return list;
  }

  @Override
  public int getType() {
    return COMPARISON;
  }

  @Override
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    Object left = _left.evaluate(context);
    Object right = _right.evaluate(context);

    if (context.isCqQueryContext() && left instanceof Region.Entry) {
      left = ((Region.Entry) left).getValue();
    }
    if (context.isCqQueryContext() && right instanceof Region.Entry) {
      right = ((Region.Entry) right).getValue();
    }

    if (left == null || right == null) {
      return TypeUtils.compare(left, right, _operator);
    }

    // if read-serialized is not set, deserialize the pdx instance for comparison
    // only if it is of the same class as that of the object being compared
    if (!context.getCache().getPdxReadSerialized()) {
      if (left instanceof PdxInstance && !(right instanceof PdxInstance)
          && ((PdxInstance) left).getClassName().equals(right.getClass().getName())) {
        left = ((PdxInstance) left).getObject();
      } else if (right instanceof PdxInstance && !(left instanceof PdxInstance)
          && ((PdxInstance) right).getClassName().equals(left.getClass().getName())) {
        right = ((PdxInstance) right).getObject();
      }
    }

    if (left instanceof PdxString) {
      if (right instanceof String) {
        switch (_right.getType()) {
          case LITERAL:
            right = ((CompiledLiteral) _right).getSavedPdxString();
            break;
          case QUERY_PARAM:
            right = ((CompiledBindArgument) _right).getSavedPdxString(context);
            break;
          case FUNCTION:
          case PATH:
            right = new PdxString((String) right);
        }
      }
    } else if (right instanceof PdxString) {
      switch (_left.getType()) {
        case LITERAL:
          left = ((CompiledLiteral) _left).getSavedPdxString();
          break;
        case QUERY_PARAM:
          left = ((CompiledBindArgument) _left).getSavedPdxString(context);
          break;
        case FUNCTION:
        case PATH:
          left = new PdxString((String) left);
      }
    }
    return TypeUtils.compare(left, right, _operator);
  }

  /**
   * Asif : Evaluates as a filter taking advantage of indexes if appropriate. This function has a
   * meaningful implementation only in CompiledComparison & CompiledUndefined . It is unsupported in
   * other classes. The additional parameters which it takes are a boolean which is used to indicate
   * whether the index result set needs to be expanded to the top level or not. The second is a
   * CompiledValue representing the operands which are only iter evaluatable. The CompiledValue
   * passed will be null except if a GroupJunction has only one filter evaluatable condition & rest
   * are iter operands. In such cases , the iter operands will be evaluated while expanding/cutting
   * down the index resultset
   *
   */
  @Override
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults,
      boolean completeExpansionNeeded, @Retained CompiledValue iterOperands,
      RuntimeIterator[] indpndntItrs, boolean isIntersection, boolean conditioningNeeded,
      boolean evaluateProjection) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // see if we're dependent on the current iterator
    // if not let super handle it
    // RuntimeIterator itr = context.getCurrentIterator();
    // Support.Assert(itr != null);
    if (!isDependentOnCurrentScope(context))
      return super.filterEvaluate(context, intermediateResults);
    IndexInfo[] idxInfo = getIndexInfo(context);
    Support.Assert(idxInfo != null,
        "a comparison that is dependent, not indexed, and filter evaluated is not possible");
    if (idxInfo.length == 1) {
      return singleBaseCollectionFilterEvaluate(context, intermediateResults,
          completeExpansionNeeded, iterOperands, idxInfo[0], indpndntItrs, isIntersection,
          conditioningNeeded, evaluateProjection);
    } else {
      Support.Assert(idxInfo.length == 2,
          "A Composite CompiledComparison which is filter evaluatable needs to have two indexes");
      return doubleBaseCollectionFilterEvaluate(context, intermediateResults,
          completeExpansionNeeded, iterOperands, idxInfo, indpndntItrs);
    }
  }

  /**
   * evaluate as a filter, producing an intermediate result set. This may require iteration if there
   * is no index available. Asif :The booelan true implies that CompiledComparsion when existing on
   * its own always requires a Completeexpansion to top level iterators. This flag can get toggled
   * to false only from inside a GroupJunction
   *
   * @param intermediateResults if this parameter is provided, and we have to iterate, then iterate
   *        over this result set instead of the entire base collection.
   */
  @Override
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // Asif : This function can be invoked only if the where clause contains
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


    return filterEvaluate(context, intermediateResults, true/*
                                                             * Complete Expansion needed
                                                             */, null,
        indpndntItr != null ? new RuntimeIterator[] {indpndntItr}
            : null/*
                   * Asif :It is safe to pass null as the independent iterator to which the
                   * condition belongs is required only if boolean complete expansion turns out to
                   * be false, which can happen only in case of CompiledComparison/CompiledUndefined
                   * called from roupJunction or CompositeGroupJunction
                   */,
        true, this.isConditioningNeededForIndex(indpndntItr, context, true),
        true /* evaluate projection attribute */);
  }

  /*
   * Asif : This function should never get invoked as now if a CompiledJunction or GroupJunction
   * contains a single filterable CompiledComparison it should directly call filterEvaluate rather
   * than auxFilterEvalutae. Overriding this function just for ensuring that auxFilterEvaluate is
   * not being called by mistake.
   */
  @Override
  public SelectResults auxFilterEvaluate(ExecutionContext context,
      SelectResults intermediateResults) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    Support.assertionFailed(
        " This auxFilterEvaluate of CompiledComparison should never have got invoked.");
    return null;
  }

  @Override
  public void negate() {
    _operator = inverseOperator(_operator);
  }

  // Invariant: the receiver is dependent on the current iterator.
  @Override
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
          && indexInfo[0]._index.getType() != IndexType.PRIMARY_KEY) {
        result.isPreferred = true;
      }
    }
    return result;
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    context.addDependencies(this, _left.computeDependencies(context));
    return context.addDependencies(this, _right.computeDependencies(context));
  }

  int reflectOnOperator(CompiledValue key) {
    int operator = _operator;
    if (key == _left)
      operator = reflectOperator(operator);
    return operator;
  }

  @Override
  public boolean isRangeEvaluatable() {
    if (this._left instanceof MapIndexable || this._right instanceof MapIndexable) {
      return false;
    }
    return true;
  }

  @Override
  public int getSizeEstimate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    IndexInfo[] idxInfo = getIndexInfo(context);

    // Both operands are indexed, evaluate it first in the filter operand.
    if (idxInfo != null && idxInfo.length > 1) {
      return 0;
    }

    // Asif: This implies it is an independent condition. So evaluate it second in filter operand.
    if (idxInfo == null) {
      return 1;
    }

    assert idxInfo.length == 1;
    Object key = idxInfo[0].evaluateIndexKey(context);

    // Key not found (indexes have mapping for UNDEFINED), evaluation is fast so do it first.
    if (key != null && key.equals(QueryService.UNDEFINED)) {
      return 0;
    }

    if (context instanceof QueryExecutionContext) {
      QueryExecutionContext qcontext = (QueryExecutionContext) context;
      if (qcontext.isHinted(idxInfo[0]._index.getName())) {
        return qcontext.getHintSize(idxInfo[0]._index.getName());
      }
    }
    // if the key is the LEFT operand, then reflect the operator
    // before the index lookup
    int op = reflectOnOperator(idxInfo[0]._key());

    return idxInfo[0]._index.getSizeEstimate(key, op, idxInfo[0]._matchLevel);
  }

  /* **************** PRIVATE METHODS ************************** */
  /*
   * evaluate as a filter, involving a single iterator. Use an index if possible.
   */
  // Invariant: the receiver is dependent on the current iterator.
  private SelectResults singleBaseCollectionFilterEvaluate(ExecutionContext context,
      SelectResults intermediateResults, final boolean completeExpansionNeeded,
      @Retained CompiledValue iterOperands, IndexInfo indexInfo, RuntimeIterator[] indpndntItr,
      boolean isIntersection, boolean conditioningNeeded, boolean evaluateProj)
      throws TypeMismatchException, AmbiguousNameException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    ObjectType resultType = indexInfo._index.getResultSetType();
    int indexFieldsSize = -1;
    SelectResults set = null;
    boolean createEmptySet = false;
    // Direct comparison with UNDEFINED will always return empty set
    Object key = indexInfo.evaluateIndexKey(context);
    createEmptySet = (key != null && key.equals(QueryService.UNDEFINED));
    if (resultType instanceof StructType) {
      indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
    } else {
      indexFieldsSize = 1;
    }

    // if the key is the LEFT operand, then reflect the operator
    // before the index lookup
    int op = reflectOnOperator(indexInfo._key());
    // actual index lookup
    QueryObserver observer = QueryObserverHolder.getInstance();
    List projAttrib = null;
    /*
     * Asif : First obtain the match level of index resultset. If the match level happens to be zero
     * , this implies that we just have to change the StructType ( again if only the Index resultset
     * is a StructBag). If the match level is zero & expand to to top level flag is true & iff the
     * total no. of iterators in current scope is greater than the no. of fields in StructBag , then
     * only we need to do any expansion.
     *
     */

    try {
      if (!createEmptySet) {
        observer.beforeIndexLookup(indexInfo._index, op, key);
        context.cachePut(CompiledValue.INDEX_INFO, indexInfo);
      }
      // //////////////////////////////////////////////////////////
      // Asif:Create an instance of IndexConditionHelper to see , if the match
      // level is zero & expansion is to Group level & that no reshuffling is
      // needed.
      // If this holds true , then we will try to evaluate iter operand while
      // collecting the results itself. Pass the iter operand as null so that
      // we get the right idea. Also right now we will assume that only single
      // iterator cases will be candidates for this oprtmization.
      // dependent iterators will come later.
      boolean useLinkedDataStructure = false;
      boolean nullValuesAtStart = true;
      Boolean orderByClause = (Boolean) context.cacheGet(CompiledValue.CAN_APPLY_ORDER_BY_AT_INDEX);
      if (orderByClause != null && orderByClause.booleanValue()) {
        List orderByAttrs = (List) context.cacheGet(CompiledValue.ORDERBY_ATTRIB);
        useLinkedDataStructure = orderByAttrs.size() == 1;
        nullValuesAtStart = !((CompiledSortCriterion) orderByAttrs.get(0)).getCriterion();
      }
      // ////////////////////////////////////////////////////////////////
      if (!conditioningNeeded) {
        ObjectType projResultType =
            evaluateProj ? (ObjectType) context.cacheGet(RESULT_TYPE) : null;
        if (projResultType != null) {
          resultType = projResultType;
          projAttrib = (List) context.cacheGet(PROJ_ATTRIB);
          context.cachePut(RESULT_TYPE, Boolean.TRUE);
        }

        if (isIntersection) {
          if (resultType instanceof StructType) {
            context.getCache().getLogger()
                .fine("StructType resultType.class=" + resultType.getClass().getName());
            if (useLinkedDataStructure) {
              set = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) resultType)
                  : new SortedResultsBag<Struct>((StructTypeImpl) resultType, nullValuesAtStart);
            } else {
              set = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
            }
            indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
          } else {
            context.getCache().getLogger()
                .fine("non-StructType resultType.class=" + resultType.getClass().getName());
            if (useLinkedDataStructure) {
              set = context.isDistinct() ? new LinkedResultSet(resultType)
                  : new SortedResultsBag(resultType, nullValuesAtStart);
            } else {
              set = QueryUtils.createResultCollection(context, resultType);
            }
            indexFieldsSize = 1;
          }
        } else {

          if (intermediateResults != null && context.getQuery() != null
              && ((DefaultQuery) context.getQuery()).getSelect().isDistinct()) {
            set = intermediateResults;
            intermediateResults = null;
          } else {
            if (resultType instanceof StructType) {
              context.getCache().getLogger()
                  .fine("StructType resultType.class=" + resultType.getClass().getName());
              if (useLinkedDataStructure) {
                set = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) resultType)
                    : new SortedResultsBag<Struct>((StructTypeImpl) resultType, nullValuesAtStart);
              } else {
                set = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
              }
              indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
            } else {
              context.getCache().getLogger()
                  .fine("non-StructType resultType.class=" + resultType.getClass().getName());
              if (useLinkedDataStructure) {
                set = context.isDistinct() ? new LinkedResultSet(resultType)
                    : new SortedResultsBag(resultType, nullValuesAtStart);
              } else {
                set = QueryUtils.createResultCollection(context, resultType);
              }
              indexFieldsSize = 1;
            }
          }
        }
        if (!createEmptySet) {
          indexInfo._index.query(key, op, set, iterOperands,
              indpndntItr != null ? indpndntItr[0] : null, context, projAttrib, intermediateResults,
              isIntersection);
        }
      } else {
        if (resultType instanceof StructType) {
          context.getCache().getLogger()
              .fine("StructType resultType.class=" + resultType.getClass().getName());
          if (useLinkedDataStructure) {
            set = context.isDistinct() ? new LinkedStructSet((StructTypeImpl) resultType)
                : new SortedResultsBag<Struct>((StructTypeImpl) resultType, nullValuesAtStart);
          } else {
            set = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
          }

          indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
        } else {
          context.getCache().getLogger()
              .fine("non-StructType resultType.class=" + resultType.getClass().getName());
          if (useLinkedDataStructure) {
            set = context.isDistinct() ? new LinkedResultSet(resultType)
                : new SortedResultsBag(resultType, nullValuesAtStart);
          } else {
            set = QueryUtils.createResultCollection(context, resultType);
          }
          indexFieldsSize = 1;
        }
        if (!createEmptySet) {
          indexInfo._index.query(key, op, set, context); // tow rows qualify from index
        }
        // look up. rahul
      }
    } finally {
      if (!createEmptySet) {
        observer.afterIndexLookup(set);
      }
    }
    if (conditioningNeeded) {
      return QueryUtils.getConditionedIndexResults(set, indexInfo, context, indexFieldsSize,
          completeExpansionNeeded, iterOperands, indpndntItr);
    } else {
      return set;
    }
  }

  /**
   * evaluate as a filter, involving a two independent iterators. Use an index if possible on both.
   * And merge theresults obtained.
   */
  // Invariant: the receiver is dependent on the current iterator.
  private SelectResults doubleBaseCollectionFilterEvaluate(ExecutionContext context,
      SelectResults intermediateResults, boolean completeExpansionNeeded,
      CompiledValue iterOperands, IndexInfo[] indxInfo, RuntimeIterator[] indpdntItrs)
      throws TypeMismatchException, AmbiguousNameException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException {
    // Asif : First we need to collect the results of the two indexes
    // We will be calling a function of Index & passing the other Index as a
    // parameter.
    // We will be getting List object. Each element of the List will contain a
    // two dimesional
    // Object array. The first row will contain the result objects of the first
    // Index
    // & secondrow will contain that of second index. The object contained in
    // each of the
    // one dimensional array can be either genuine result object or StructImpl
    // object.
    QueryObserver observer = QueryObserverHolder.getInstance();
    context.cachePut(CompiledValue.INDEX_INFO, indxInfo);
    /*
     * Asif : If the independent Group of iterators passed is not null or the independent Group of
     * iterators passed is null & complete expansion flag is true & the intermediate result set is
     * empty or null, in such cases , we have to definitely use indexes on both conditions & go for
     * complete expansion or CompsoiteGroupJunction expansion as be the case. ( ie retaining the old
     * logic of expanding the index result to top or compositeGroupJunction level)
     *
     * The condition in which we sure are to use indexes on both LHS & RHS & the the resultset
     * expanded to CGJ or top level are 1) A stand alone filterable composite condition : In such
     * cases the independent group of itr is passed as null , the intermediate resultset is empty &
     * the complete expansion flag is true. This indicates that we have to expand to top level. 2)
     * Multiple filterable composite condition in OR junction: In such cases the independent grp of
     * its passed is not null but the intermediate resultset is null . This means that indexes on
     * both LHS & RHS to be used & expanded to top or CGJ level ( same as above)
     *
     * The case which is different from above is AND condition evaluation In such cases, till the
     * last but one filerable condition we pass complete expn flag as false & group of indpndt itrs
     * as null. Only last filterabel condn is passed genuine value of expn flag & the not null grp
     * of independent itrs . The intermedaite set is not null & not empty for such cases which wil
     * help us distinguish the case from OR junction
     *
     * IndependentGroup Iterators being null or not null is not a definitive & exclusive * criteria
     * for knowing whether the CC result needs to be expanded to Top or CGJ level as for independent
     * grp not null will happen in OR junction implying expn to CGJ level & ignoring intermediate
     * resultset as well as in evalaution of last filterable condition of AND junction implying
     * usage of intermediate resultset for cartesian.
     *
     * Similarly grp of indpendent itrs can be null for stand alone condition implying expn to top
     * level & ignoring intermediate set & also for evaluating all the conditions except last in AND
     * junction( which means using intermediate resultset for cartesian)
     *
     */
    if ((intermediateResults == null || intermediateResults.isEmpty())
        && (indpdntItrs != null || completeExpansionNeeded)) {
      /*
       * Asif : First obtain the match level of index resultset. If the match level happens to be
       * zero , this implies that we just have to change the StructType ( again if only the Index
       * resultset is a StructBag). If the match level is zero & expand to to top level flag is true
       * & iff the total no. of iterators in current scope is greater than the no. of fields in
       * StructBag , then only we need to do any expansion.
       *
       */
      Support.Assert(this._operator == TOK_EQ,
          "A relationship index is not usable for any condition other than equality");

      List data = null;
      try {
        observer.beforeIndexLookup(indxInfo[0]._index, this._operator, null);
        observer.beforeIndexLookup(indxInfo[1]._index, this._operator, null);
        if (context.getBucketList() != null) {
          data = QueryUtils.queryEquijoinConditionBucketIndexes(indxInfo, context);
        } else {
          data = indxInfo[0]._index.queryEquijoinCondition(indxInfo[1]._index, context);
        }
      } finally {
        observer.afterIndexLookup(data);
      }
      return QueryUtils.getConditionedRelationshipIndexResultsExpandedToTopOrCGJLevel(data,
          indxInfo, context, completeExpansionNeeded, iterOperands, indpdntItrs);
    } else {
      // Asif . We are in this block , this itself guarantees that this
      // conditioned is being called
      // from AND junction evalauation of CompositeGroupJunction . The nature of
      // resultset obtained from here
      // will depend upon the intermediate Resultset passed etc.
      return QueryUtils.getRelationshipIndexResultsMergedWithIntermediateResults(
          intermediateResults, indxInfo, context, completeExpansionNeeded, iterOperands,
          indpdntItrs);
    }
  }

  // !!!:ezoerner:20081031 get rid of this method once we are compiling at java 1.5+
  // @see Class#getSimpleName
  public static String getSimpleClassName(Class cls) {
    return cls.getName().substring(cls.getPackage().getName().length() + 1);
  }

  // Asif: If the size of aray is two this implies that it is
  // a relation ship index & so the key field will be null in both the indexes
  // as key is not a meaningful entity. The 0th element will refer to LHS
  // operand
  // and 1th element will refer to RHS operannd
  @Override
  public IndexInfo[] getIndexInfo(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    IndexInfo[] indexInfo = privGetIndexInfo(context);
    if (indexInfo != null) {
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
    if (pAndK == null) {
      IndexData[] indexData =
          QueryUtils.getRelationshipIndexIfAny(_left, _right, context, this._operator);// findOnlyFunctionalIndex.
      if (indexData != null) {
        newIndexInfo = new IndexInfo[2];
        for (int i = 0; i < 2; ++i) {
          newIndexInfo[i] = new IndexInfo(null, i == 0 ? _left : _right, indexData[i].getIndex(),
              indexData[i].getMatchLevel(), indexData[i].getMapping(),
              i == 0 ? this._operator : reflectOperator(this._operator));
        }
      }
    } else {
      CompiledValue path = pAndK._path;
      CompiledValue indexKey = pAndK._key;
      IndexData indexData = null;
      // CompiledLike should not use HashIndex and PrimarKey Index.
      if (this instanceof CompiledLike) {
        indexData =
            QueryUtils.getAvailableIndexIfAny(path, context, OQLLexerTokenTypes.LITERAL_like);
      } else {
        indexData = QueryUtils.getAvailableIndexIfAny(path, context, this._operator);
      }

      IndexProtocol index = null;
      if (indexData != null) {
        index = indexData.getIndex();
      }
      if (index != null && index.isValid()) {
        newIndexInfo = new IndexInfo[1];
        newIndexInfo[0] = new IndexInfo(indexKey, path, index, indexData.getMatchLevel(),
            indexData.getMapping(), reflectOnOperator(indexKey));
      }
    }
    if (newIndexInfo != null) {
      privSetIndexInfo(newIndexInfo, context);
    } else {
      privSetIndexInfo(NO_INDEXES_IDENTIFIER, context);
    }
    return newIndexInfo;
  }

  CompiledValue getKey(ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException {
    return getPathAndKey(context)._key;
  }

  /**
   * get the path to see if there's an index for, and also determine which CompiledValue is the key
   * while we're at it
   */
  private PathAndKey getPathAndKey(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException {
    // RuntimeIterator lIter = context.findRuntimeIterator(_left);
    // RuntimeIterator rIter = context.findRuntimeIterator(_right);
    boolean isLeftDependent = context.isDependentOnCurrentScope(_left);
    boolean isRightDependent = context.isDependentOnCurrentScope(_right);
    if ((isLeftDependent == false) == (isRightDependent == false))
      return null;
    CompiledValue indexKey;
    CompiledValue path;
    if (isLeftDependent == false) {
      path = _right;
      indexKey = _left;
    } else {
      path = _left;
      indexKey = _right;
    }
    if (indexKey.isDependentOnCurrentScope(context))
      return null; // this check
    // seems to be
    // redunant.
    return new PathAndKey(path, indexKey);
  }

  // _indexInfo is a transient field
  // if this is just faulted in then can be null
  private IndexInfo[] privGetIndexInfo(ExecutionContext context) {
    return (IndexInfo[]) context.cacheGet(this);
  }

  private void privSetIndexInfo(IndexInfo[] indexInfo, ExecutionContext context) {
    context.cachePut(this, indexInfo);
  }

  /* Inner classes for passing stuff around */
  static class PathAndKey {

    CompiledValue _path;
    CompiledValue _key;

    PathAndKey(CompiledValue path, CompiledValue indexKey) {
      _path = path;
      _key = indexKey;
    }
  }

  @Override
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

    if (this.getPlanInfo(context).evalAsFilter) {
      PlanInfo pi = this.getPlanInfo(context);
      if (pi.indexes.size() == 1) {
        IndexProtocol ip = (IndexProtocol) pi.indexes.get(0);
        if (ip.getCanonicalizedIndexedExpression().equals(canonicalizedOrderByClause)
            && ip.getType() != IndexType.PRIMARY_KEY && pi.isPreferred) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public boolean isConditioningNeededForIndex(RuntimeIterator independentIter,
      ExecutionContext context, boolean completeExpnsNeeded)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    IndexConditioningHelper ich = null;
    IndexInfo[] idxInfo = getIndexInfo(context);
    int indexFieldsSize = -1;
    boolean conditioningNeeded = true;
    if (idxInfo == null || idxInfo.length > 1) {
      return conditioningNeeded;
    }
    // assert idxInfo.length == 1;
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
    return ich == null || ich.shufflingNeeded;


  }

  @Override
  public int getOperator() {
    return this._operator;
  }

  @Override
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

    // There may be some hard rules that give unoptimal selections based on these switch cases.
    if (this._operator == TOK_EQ || this._operator == TOK_NE || this._operator == TOK_NE_ALT) {
      switch (thatOperator) {
        case TOK_EQ:
        case TOK_NE:
        case TOK_NE_ALT:
          isThisBetter = thisSize <= thatSize;
          break;
        case LITERAL_and:
          // This is is possible only in case of RangeJunction.
          if (this._operator == TOK_NE || this._operator == TOK_NE_ALT) {
            // Asif: Give preference to range as I am assuming that range will fetch less data
            // as compared to NOT EQUALs
            isThisBetter = false;
          }
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
    } else {
      // This is a inequality. If that is true the priority goes to equality & Not Equality & Range
      switch (thatOperator) {
        case TOK_EQ:
        case TOK_NE:
        case TOK_NE_ALT:
        case LITERAL_and:
          // Asif: Give preference to range as I am assuming that raneg will fetch less data
          // as compared to NOT EQUALs
          isThisBetter = false;
          break;
        case TOK_LE:
        case TOK_LT:
        case TOK_GE:
        case TOK_GT:
          isThisBetter = thisSize <= thatSize;
          break;
        default:
          throw new IllegalArgumentException("The operator type =" + thatOperator + " is unknown");

      }
    }
    return isThisBetter;
  }

}

// IndexInfo was removed from here to its own file.
