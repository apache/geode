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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.IndexData;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.query.internal.index.IndexUtils;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;

/**
 * Predefined function for identity of the UNDEFINED literal
 *
 * @version $Revision: 1.2 $
 */
public class CompiledUndefined extends AbstractCompiledValue implements Negatable, Indexable {

  private CompiledValue _value;
  private boolean _is_defined;

  public CompiledUndefined(CompiledValue value, boolean is_defined) {
    _value = value;
    _is_defined = is_defined;
  }

  @Override
  public List getChildren() {
    return Collections.singletonList(this._value);
  }

  @Override
  public int getType() {
    return FUNCTION;
  }

  @Override
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    boolean b = _value.evaluate(context) == QueryService.UNDEFINED;
    return Boolean.valueOf(_is_defined ? !b : b);
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
      boolean completeExpansionNeeded, CompiledValue iterOperands, RuntimeIterator[] indpndntItrs,
      boolean isIntersection, boolean conditioningNeeded, boolean evaluateProjAttrib)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // this method is called if we are independent of the iterator,
    // or if we can use an index.
    // if we are independent, then we should not have been here in the first
    // place
    Support.Assert(this._value.isDependentOnCurrentScope(context),
        "For a condition which does not depend on any RuntimeIterator of current scope , we should not have been in this function");
    IndexInfo idxInfo[] = getIndexInfo(context);
    ObjectType resultType = idxInfo[0]._index.getResultSetType();
    int indexFieldsSize = -1;
    SelectResults set = null;
    if (resultType instanceof StructType) {
      set = QueryUtils.createStructCollection(context, (StructTypeImpl) resultType);
      indexFieldsSize = ((StructTypeImpl) resultType).getFieldNames().length;
    } else {
      set = QueryUtils.createResultCollection(context, resultType);
      indexFieldsSize = 1;
    }
    int op = _is_defined ? TOK_NE : TOK_EQ;
    Object key = QueryService.UNDEFINED;
    QueryObserver observer = QueryObserverHolder.getInstance();
    try {
      observer.beforeIndexLookup(idxInfo[0]._index, op, key);
      context.cachePut(CompiledValue.INDEX_INFO, idxInfo[0]);
      idxInfo[0]._index.query(key, op, set, context);
    } finally {
      observer.afterIndexLookup(set);
    }
    return QueryUtils.getConditionedIndexResults(set, idxInfo[0], context, indexFieldsSize,
        completeExpansionNeeded, iterOperands, indpndntItrs);
  }

  @Override
  public int getSizeEstimate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    IndexInfo[] idxInfo = getIndexInfo(context);
    assert idxInfo.length == 1;

    if (context instanceof QueryExecutionContext) {
      QueryExecutionContext qcontext = (QueryExecutionContext) context;
      if (qcontext.isHinted(idxInfo[0]._index.getName())) {
        return qcontext.getHintSize(idxInfo[0]._index.getName());
      }
    }

    int op = _is_defined ? TOK_NE : TOK_EQ;
    return idxInfo[0]._index.getSizeEstimate(QueryService.UNDEFINED, op, idxInfo[0]._matchLevel);
  }

  @Override
  public int getOperator() {
    return _is_defined ? TOK_NE : TOK_EQ;
  }

  /**
   * evaluate as a filter, producing an intermediate result set. This may require iteration if there
   * is no index available. Asif :The boolean true implies that CompiledComparsion when existing on
   * its own always requires a Complete expansion to top level iterators. This flag can get toggled
   * to false only from inside a GroupJunction
   *
   * <p>
   * param intermediateResults if this parameter is provided, and we have to iterate, then iterate
   * over this result set instead of the entire base collection.
   */
  @Override
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    return filterEvaluate(context, iterationLimit, true/* Complete Expansion needed */, null, null,
        true, isConditioningNeededForIndex(null, context, true), false);
  }

  /*
   * Asif : This function should never get invoked as now if a CompiledJunction or GroupJunction
   * contains a single filterable CompiledUndefined, it should directly call filterEvaluate rather
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
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    return context.addDependencies(this, this._value.computeDependencies(context));
  }

  @Override
  public void negate() {
    _is_defined = !_is_defined;
  }

  // Invariant: the receiver is dependent on the current iterator.
  @Override
  protected PlanInfo protGetPlanInfo(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    PlanInfo result = new PlanInfo();
    IndexInfo[] indexInfo = getIndexInfo(context);
    if (indexInfo == null) {
      return result;
    }
    Support.Assert(indexInfo.length == 1,
        "For a CompiledUndefined  we cannot have a join of two indexes. There should be only a single index to use");
    result.indexes.add(indexInfo[0]._index);
    result.evalAsFilter = true;
    return result;
  }


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
    if (!IndexUtils.indexesEnabled) {
      return null;
    }
    // TODO:Asif : Check if the condition is such that Primary Key Index is used
    // & its key is DEFINED
    // , then are we returning all the values of the region ?
    // & that if the key is UNDEFINED are we returning an empty set.?
    IndexData indexData =
        QueryUtils.getAvailableIndexIfAny(this._value, context, _is_defined ? TOK_NE : TOK_EQ);
    IndexProtocol index = null;
    IndexInfo[] newIndexInfo = null;
    if (indexData != null) {
      index = indexData.getIndex();
    }
    if (index != null && index.isValid()) {
      newIndexInfo = new IndexInfo[1];
      /*
       * Pass the Key as null as the key is not of type CompiledValue( but of type
       * QueryService.UNDEFINED)
       */
      newIndexInfo[0] = new IndexInfo(null, this._value, index, indexData.getMatchLevel(),
          indexData.getMapping(), _is_defined ? TOK_NE : TOK_EQ);
    }
    if (newIndexInfo != null) {
      privSetIndexInfo(newIndexInfo, context);
    } else {
      privSetIndexInfo(NO_INDEXES_IDENTIFIER, context);
    }
    return newIndexInfo;
  }

  @Override
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    clauseBuffer.insert(0, ')');
    _value.generateCanonicalizedExpression(clauseBuffer, context);
    if (_is_defined) {
      clauseBuffer.insert(0, "IS_DEFINED(");
    } else {
      clauseBuffer.insert(0, "IS_UNDEFINED(");
    }
  }

  // _indexInfo is a transient field
  // if this is just faulted in then can be null
  private IndexInfo[] privGetIndexInfo(ExecutionContext context) {
    return (IndexInfo[]) context.cacheGet(this);
  }

  private void privSetIndexInfo(IndexInfo[] indexInfo, ExecutionContext context) {
    context.cachePut(this, indexInfo);
  }

  @Override
  public boolean isRangeEvaluatable() {
    return false;
  }

  @Override
  public boolean isProjectionEvaluationAPossibility(ExecutionContext context) {
    return true;
  }

  // TODO:Asif: This should ideally be treated like CompiledComparison in terms evaluation of
  // iter operands etc
  @Override
  public boolean isConditioningNeededForIndex(RuntimeIterator independentIter,
      ExecutionContext context, boolean completeExpnsNeeded)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    return true;
  }

  @Override
  public boolean isBetterFilter(Filter comparedTo, ExecutionContext context, int thisSize)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // If the current filter is equality & comparedTo filter is also equality based , then
    // return the one with lower size estimate is better
    boolean isThisBetter = true;
    int thisOperator = this.getOperator();
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
        isThisBetter = thisSize <= thatSize;
        break;
      case LITERAL_and:
        // This is possible only in case of RangeJunction
        if (thisOperator == TOK_NE || thisOperator == TOK_NE_ALT) {
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
    return isThisBetter;
  }
}
