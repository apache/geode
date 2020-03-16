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
/*
 * Abstract Super class of the Group or Range Junction. The common functionality of Group or Range
 * Junction is present in this class.
 */
package org.apache.geode.cache.query.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.Assert;
import org.apache.geode.util.internal.GeodeGlossary;

public abstract class AbstractGroupOrRangeJunction extends AbstractCompiledValue
    implements Filter, OQLLexerTokenTypes {
  /** left operand */
  final CompiledValue[] _operands;
  private static final int INDEX_RESULT_THRESHOLD_DEFAULT = 100;
  public static final String INDX_THRESHOLD_PROP_STR =
      GeodeGlossary.GEMFIRE_PREFIX + "Query.INDEX_THRESHOLD_SIZE";
  private static final int indexThresholdSize =
      Integer.getInteger(INDX_THRESHOLD_PROP_STR, INDEX_RESULT_THRESHOLD_DEFAULT).intValue();
  private int _operator = 0;
  private CompiledValue iterOperands;
  // Asif: In normal circumstances , there will be only one indpendent
  // RuntimeIterator for a GroupJunction. But if inside a
  // CompositeGroupJunction, there exists only a single GroupJunction , then it
  // is advantageous to evaluate the the GroupJunction directly to the level of
  // ComspositeGroupJunction or complete expansion ( rather than expanding it
  // within CompositeGroupJunction). This will save some extra iteration. In
  // such cases , the GroupJunction may contain multiple Iterators ( this will
  // happen only if called from CompositeGroupJunction
  private RuntimeIterator indpndntItr[] = null;

  private boolean completeExpansion = false;

  AbstractGroupOrRangeJunction(int operator, RuntimeIterator[] indpndntItr,
      boolean isCompleteExpansion, CompiledValue[] operands) {
    this.indpndntItr = indpndntItr;
    _operator = operator;
    this.completeExpansion = isCompleteExpansion;
    this._operands = operands;
  }

  AbstractGroupOrRangeJunction(AbstractGroupOrRangeJunction oldGJ, boolean completeExpansion,
      RuntimeIterator indpnds[], CompiledValue iterOp) {
    this._operator = oldGJ._operator;
    this.completeExpansion = completeExpansion;
    this.indpndntItr = indpnds;
    if (iterOp != null) {
      if (iterOp instanceof CompiledComparison || iterOp instanceof CompiledIn) {
        int finalSize = 1 + oldGJ._operands.length;
        this._operands = new CompiledValue[finalSize];
        System.arraycopy(oldGJ._operands, 0, this._operands, 0, finalSize - 1);
        this._operands[finalSize - 1] = iterOp;
      } else {
        // Instance of CompiledJunction
        CompiledJunction temp = (CompiledJunction) iterOp;
        int operator = temp.getOperator();
        if (_operator == operator) {
          int tempOpSize = temp.getOperands().size();
          int oldGJOpSize = oldGJ._operands.length;
          this._operands = new CompiledValue[tempOpSize + oldGJOpSize];
          System.arraycopy(oldGJ._operands, 0, this._operands, 0, oldGJOpSize);
          Iterator itr = temp.getOperands().iterator();
          int i = oldGJOpSize;
          while (itr.hasNext()) {
            this._operands[i++] = (CompiledValue) itr.next();
          }
        } else {
          int oldGJOpSize = oldGJ._operands.length;
          this._operands = new CompiledValue[1 + oldGJOpSize];
          System.arraycopy(oldGJ._operands, 0, this._operands, 0, oldGJOpSize);
          this._operands[oldGJOpSize] = temp;
        }
      }
    } else {
      this._operands = oldGJ._operands;
    }
  }

  @Override
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    throw new AssertionError("Should not have come here");
  }

  @Override
  public int getType() {
    return GROUPJUNCTION;
  }

  void setCompleteExpansionOn() {
    this.completeExpansion = true;
  }

  void addIterOperands(CompiledValue iterOps) {
    this.iterOperands = iterOps;
  }

  CompiledValue getIterOperands() {
    return this.iterOperands;
  }

  @Override
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit,
      boolean completeExpansionNeeded, CompiledValue iterOperands, RuntimeIterator[] indpndntItrs,
      boolean isIntersection, boolean conditioningNeeded, boolean evalProj)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    if (iterOperands != null) {
      this.addIterOperands(iterOperands);
    }
    return filterEvaluate(context, null /* The intermediate results is null */);

  }

  @Override
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    OrganizedOperands newOperands = organizeOperands(context);
    SelectResults result = intermediateResults;
    Support.Assert(newOperands.filterOperand != null);
    if (newOperands.isSingleFilter) {
      // The below assertion used to hold true previously that if there used to be only
      // a RangeJunction & all iter operands , then GroupJunction would never be formed
      // in the first place & we would be calling the organizeOperands of RangeJunction.
      // But now even if a GroupJunction has two RangeJUnctions, we use only one of them
      // as filter & so the other RangeJunction acts as an iter operand, thus a GroupJunction
      // is formed with a single RanegJunction as filter operand & the other RangeJunction
      // as iter operand.

      // Asif : This means that new operand is either a CompiledComparison or
      // CompiledUndefined or RangeJunctionEvaluator, BUT it cannot be
      // a single RangeJunction within a GroupJunction.
      // The interpretation of isConditioning etc is not provided for RangeEvaluators
      result = (newOperands.filterOperand).filterEvaluate(context, result, this.completeExpansion,
          newOperands.iterateOperand, this.indpndntItr,
          true /* this is necessarily an ANDjunction */,
          newOperands.filterOperand.isConditioningNeededForIndex(
              this.indpndntItr.length == 1 ? this.indpndntItr[0] : null, context,
              this.completeExpansion),
          true /* evaluate projection */);
    } else {
      // With multiple filter conditions, the newOperands.filterOperand is bound
      // to be GroupJunction
      assert newOperands.filterOperand instanceof GroupJunction;

      // evaluate directly on the operand
      result = (newOperands.filterOperand).auxFilterEvaluate(context, result);

      List unevaluatedFilterOps =
          ((GroupJunction) newOperands.filterOperand).getUnevaluatedFilterOperands();
      if (unevaluatedFilterOps != null) {
        if (newOperands.iterateOperand == null) {
          if (unevaluatedFilterOps.size() == 1) {
            newOperands.iterateOperand = (CompiledValue) unevaluatedFilterOps.get(0);
          } else {
            int len = unevaluatedFilterOps.size();
            CompiledValue[] iterOps = new CompiledValue[len];
            for (int i = 0; i < len; i++) {
              iterOps[i] = (CompiledValue) unevaluatedFilterOps.get(i);
            }
            newOperands.iterateOperand = new CompiledJunction(iterOps, getOperator());
          }
        } else {
          if (newOperands.iterateOperand instanceof CompiledJunction
              && ((CompiledJunction) newOperands.iterateOperand).getOperator() == getOperator()) {
            CompiledJunction temp = (CompiledJunction) newOperands.iterateOperand;
            List prevOps = temp.getOperands();
            unevaluatedFilterOps.addAll(prevOps);
          } else {
            unevaluatedFilterOps.add(newOperands.iterateOperand);
          }
          int totalLen = unevaluatedFilterOps.size();
          CompiledValue[] combinedOps = new CompiledValue[totalLen];
          Iterator itr = unevaluatedFilterOps.iterator();
          int j = 0;
          while (itr.hasNext()) {
            combinedOps[j++] = (CompiledValue) itr.next();
          }
          newOperands.iterateOperand = new CompiledJunction(combinedOps, getOperator());
        }
      }
      if (newOperands.iterateOperand != null) {
        // call private method here to evaluate
        result = auxIterateEvaluate(newOperands.iterateOperand, context, result);
      }
    }
    return result;
  }



  private List getCondtionsSortedOnIncreasingEstimatedIndexResultSize(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // The checks before this function is invoked
    // have ensured that all the operands are of type ComparisonQueryInfo
    // and of the form var = constant. Also need for sorting will not arise
    // if there are only two operands

    List sortedList = new ArrayList(this._operands.length);
    int len = this._operands.length;
    for (int i = 0; i < len; ++i) {
      Filter toSort = (Filter) this._operands[i];
      int indxRsltToSort = toSort.getSizeEstimate(context);
      int sortedListLen = sortedList.size();
      int j = 0;
      for (; j < sortedListLen; ++j) {
        Filter currSorted = (Filter) sortedList.get(j);
        if (currSorted.getSizeEstimate(context) > indxRsltToSort) {
          break;
        }
      }
      sortedList.add(j, toSort);
    }
    return sortedList;

  }



  /**
   * Asif : This function is always invoked on a DummyGroupJunction object formed as a part of
   * organization of operands of a GroupJunction . This also guranatees that the operands are all of
   * type CompiledComparison or CompiledUndefined
   */
  @Override
  public SelectResults auxFilterEvaluate(ExecutionContext context,
      SelectResults intermediateResults) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // evaluate the result set from the indexed values
    // using the intermediate results so far (passed in)
    // put results into new intermediate results

    List sortedConditionsList =
        this.getCondtionsSortedOnIncreasingEstimatedIndexResultSize(context);

    // Sort the operands in increasing order of resultset size
    Iterator i = sortedConditionsList.iterator();
    // SortedSet intersectionSet = new TreeSet(new SelectResultsComparator());
    while (i.hasNext()) {
      // Asif:TODO The intermediate ResultSet should be passed as null when
      // invoking filterEvaluate. Just because filterEvaluate is being called,
      // itself guarantees that there will be at least on auxFilterEvalaute call.
      // The filterEvalaute will return after invoking auxIterEvaluate.
      // The auxIterEvaluate cannot get invoked for an OR junction.
      // The approach of calculation of resultset should be Bubble UP
      // ( i.e bottom to Top of the tree. This implies that for
      // filterEvaluate, we should not be passing IntermediateResultSet.
      // Each invocation of filterEvaluate will be complete in itself
      // with the recursion being ended by evaluating auxIterEvaluate
      // if any. The passing of IntermediateResult in filterEvalaute
      // causes AND junction evaluation to be corrupted ,
      // if the intermediateResultset contains some value.
      // If the parent object is a GroupJunction then the filter may be a
      // RangeJunction or a CompiledComparison. But if the parent Object is a
      // RangeJunction then the Filter is a RangeJunctionEvaluator
      SelectResults filterResults = null;
      Filter filter = (Filter) i.next();
      boolean isConditioningNeeded = filter.isConditioningNeededForIndex(
          this.indpndntItr.length == 1 ? this.indpndntItr[0] : null, context,
          this.completeExpansion);

      // TODO:Asif: For RangeJunction I am right now returning true as
      // isConditioningNeeded because there is no provision right now to pass
      // intermediate results from RangeJunction & also no code to utilize the
      // intermediate results in the evaluator created out of RangeJunction.
      filterResults = filter.filterEvaluate(context,
          !isConditioningNeeded ? intermediateResults : null, this.completeExpansion,
          null/*
               * Asif * Asif :The iter operands passed are null, as a not null value can exists only
               * if there exists a single Filter operand in original GroupJunction
               */, this.indpndntItr, _operator == LITERAL_and, isConditioningNeeded,
          false /* do not evaluate projection */);
      if (_operator == LITERAL_and) {
        if (filterResults != null && filterResults.isEmpty()) {
          return filterResults;
        } else if (filterResults != null) {
          intermediateResults =
              (intermediateResults == null || !isConditioningNeeded) ? filterResults
                  : QueryUtils.intersection(intermediateResults, filterResults, context);
          i.remove();
          if (intermediateResults.size() <= indexThresholdSize) {
            // Abort further intersection , the residual filter operands will be transferred for
            // iter evaluation
            break;
          }
        }
      } else {
        // Asif : In case of OR clause, the filterEvaluate cannot return a
        // null value ,as a null value implies Cartesian of Iterators
        // in current scope which can happen only if the operand of OR
        // clause evaluates to true (& which we are not allowing).
        // Though if the operand evaluates to false,it is permissible case
        // for filterEvaluation.
        Assert.assertTrue(filterResults != null);
        boolean isDistinct = (DefaultQuery) context.getQuery() != null
            ? ((DefaultQuery) context.getQuery()).getSelect().isDistinct() : false;
        /*
         * Shobhit: Only for distinct query union can be avoided. For non-distinct queries
         * filterResults and intermediateResults are different for OR conditions and final result
         * can contain duplicate objects based on occurrences.
         */
        if (intermediateResults == null) {
          intermediateResults = filterResults;
        } else if (isDistinct && !isConditioningNeeded) {
          intermediateResults.addAll(filterResults);
        } else {
          intermediateResults = QueryUtils.union(intermediateResults, filterResults, context);
        }
      }
    }
    if (_operator == LITERAL_and && !sortedConditionsList.isEmpty()) {
      this.addUnevaluatedFilterOperands(sortedConditionsList);
    }
    return intermediateResults;
  }

  /** invariant: the operand is known to be evaluated by iteration */
  private SelectResults auxIterateEvaluate(CompiledValue operand, ExecutionContext context,
      SelectResults intermediateResults) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // Asif: This can be a valid value if we have an AND condition
    // like ID=1 AND true = true. In such cases , if an index is not
    // available on ID still we have at least one expression which is
    // independent of current scope. In such cases a value of null
    // means ignore the null ( because it means all the results )
    // thus a the resultset of current auxIterate which will be the
    // subset of total resultset , will be the right data.
    // auxIterEvalaute can be called only from an AND junction.
    // This also implies that there will be at least one
    // operand which will be evaluated using auxFilterEvaluate.Thus
    // the resultset given to this function can be empty ( obtained
    // using auxFilterEvaluate ,meaning no need to do iterations below )
    // but it can never be null. As null itself signifies that the junction
    // cannot be evaluated as a filter.
    if (intermediateResults == null)
      throw new RuntimeException(
          "intermediateResults can not be null");
    if (intermediateResults.isEmpty()) // short circuit
      return intermediateResults;
    List currentIters = (this.completeExpansion) ? context.getCurrentIterators()
        : QueryUtils.getDependentItrChainForIndpndntItrs(this.indpndntItr, context);
    SelectResults resultSet = null;
    RuntimeIterator rIters[] = new RuntimeIterator[currentIters.size()];
    currentIters.toArray(rIters);
    ObjectType elementType = intermediateResults.getCollectionType().getElementType();
    if (elementType.isStructType()) {
      resultSet = QueryUtils.createStructCollection(context, (StructTypeImpl) elementType);
    } else {
      resultSet = QueryUtils.createResultCollection(context, elementType);
    }

    QueryObserver observer = QueryObserverHolder.getInstance();
    try {
      observer.startIteration(intermediateResults, operand);
      Iterator iResultsIter = intermediateResults.iterator();
      while (iResultsIter.hasNext()) {
        Object tuple = iResultsIter.next();
        if (tuple instanceof Struct) {
          Object values[] = ((Struct) tuple).getFieldValues();
          for (int i = 0; i < values.length; i++) {
            rIters[i].setCurrent(values[i]);
          }
        } else {
          rIters[0].setCurrent(tuple);
        }
        Object result = null;
        try {
          result = operand.evaluate(context);
        } finally {
          observer.afterIterationEvaluation(result);
        }
        if (result instanceof Boolean) {
          if (((Boolean) result).booleanValue()) {
            resultSet.add(tuple);
          }
        } else if (result != null && result != QueryService.UNDEFINED)
          throw new TypeMismatchException("AND/OR operands must be of type boolean, not type '"
              + result.getClass().getName() + "'");
      }
    } finally {
      observer.endIteration(resultSet);
    }
    return resultSet;
  }

  /* Package methods */
  @Override
  public int getOperator() {
    return _operator;
  }

  List getOperands() {
    // return unmodifiable copy
    return Collections.unmodifiableList(Arrays.asList(_operands));
  }

  boolean getExpansionFlag() {
    return this.completeExpansion;
  }

  RuntimeIterator[] getIndependentIteratorForGroup() {
    return this.indpndntItr;
  }

  /**
   * Segregates the Where clause operands of a Range or Group Junction into filter evaluatable &
   * iter evaluatable operands by creating an Object of Organizedoperands. This method is invoked
   * from organizeOperands of Group or Range Junction.
   *
   * @param indexCount Indicates the number of operands of the evalOperands List which are filter
   *        evaluatable. The filter evaluatable operands are present at the start of the List
   * @param evalOperands List containing the operands of the Group or Range Junction. The operands
   *        may be filter or iter evaluatable
   * @return Object of OrganziedOperands type having filter & iter operand segregated
   */
  OrganizedOperands createOrganizedOperandsObject(int indexCount, List evalOperands) {
    OrganizedOperands result = new OrganizedOperands();
    // group the operands into those that use indexes and those that don't,
    // so we can evaluate all the operands that don't use indexes together
    // in one iteration first get filter operands
    Filter filterOperands = null;
    // there must be at least one filter operand or else we wouldn't be here
    Support.Assert(indexCount > 0);
    if (indexCount == 1) {
      filterOperands = (Filter) evalOperands.get(0);
      // Asif : Toggle the flag of OrgaizedOperands to indicate a single Filter
      // condition
      result.isSingleFilter = true;
    } else {
      CompiledValue[] newOperands = new CompiledValue[indexCount];
      for (int i = 0; i < indexCount; i++)
        newOperands[i] = (CompiledValue) evalOperands.get(i);
      filterOperands = new GroupJunction(getOperator(), getIndependentIteratorForGroup(),
          getExpansionFlag(), newOperands);
    }
    // get iterating operands
    // INVARIANT: the number of iterating operands when the _operator is
    // LITERAL_or must be zero. This is an invariant on filter evaluation
    CompiledValue iterateOperands = null;
    // int numIterating = _operands.length - indexCount;
    int numIterating = evalOperands.size() - indexCount;
    // Commenting this assert as for CompiledLike there could be an
    // iterOperand in a GroupJunction with OR condition.
    // For example, query with WHERE clause as
    // NOT (status like 'a%' or pkid like '1%')
    // results in GroupJunction for status (status<'a'||status>'b').
    // In this case (pkid like '1') is iterating condition for the GJ.
    // Support.Assert(getOperator() == LITERAL_and || numIterating == 0);
    if (numIterating > 0) {
      if (numIterating == 1)
        iterateOperands = (CompiledValue) evalOperands.get(indexCount);
      else {
        CompiledValue[] newOperands = new CompiledValue[numIterating];
        for (int i = 0; i < numIterating; i++)
          newOperands[i] = (CompiledValue) evalOperands.get(i + indexCount);
        iterateOperands = new CompiledJunction(newOperands, getOperator());
      }
    }
    result.filterOperand = filterOperands;
    result.iterateOperand = iterateOperands;
    return result;
  }

  /**
   * Helper method which creates a new RangeJunction or Group Junction from the old junction. Thus
   * if the invoking junction is a RangeJunction , the new object created will be a RangeJunction
   * else if a GroupJunction then that will be created. This method retains the operands of the old
   * junction while creating the new junction and adds the iter operands passed, to the existing
   * operands.
   *
   * @param completeExpansion if true indicates that the junction on evaluation should be expanded
   *        to the query from clause level.
   * @param indpnds The independent runtime iterators to which this group belongs to. Normally it
   *        will always be a single iterator, but if there exists a single GroupJunction in a
   *        CompositeGroupJunction then this would be more than 1 ,as in that case the GroupJunction
   *        will be evaluated to the CompositeGroupJunction level
   * @param iterOp The Iter operands which along with the operands of the old junction should be
   *        present in the new junction. These operands are definitely not filter evaluatable
   * @return RangeJunction or GroupJunction depending on the nature of the old junction ( Range or
   *         Group)
   */
  abstract AbstractGroupOrRangeJunction recreateFromOld(boolean completeExpansion,
      RuntimeIterator indpnds[], CompiledValue iterOp);

  /**
   * Helper method which creates a new RangeJunction or Group Junction from the old junction. Thus
   * if the invoking junction is a RangeJunction , the new object created will be a RangeJunction
   * else if a GroupJunction then that will be created. This method discards the operands of the old
   * junction while creating the new junction . The new junction will only have the operands passed
   * to it.
   *
   * @param operator The junction type ( AND or OR ) of the old junction
   * @param indpndntItr Array of RuntimeIterator representing the Independent Iterator for the Group
   *        or Range Junction
   * @return Object of type GroupJunction or RangeJunction depending on the type of the invoking
   *         Object
   */
  abstract AbstractGroupOrRangeJunction createNewOfSameType(int operator,
      RuntimeIterator[] indpndntItr, boolean isCompleteExpansion, CompiledValue[] operands);

  /**
   * Segregates the conditions into those which are filter evaluatable and which are iter
   * evaluatable. This method is appropriately overridden in the GroupJunction and RangeJunction
   * class.
   *
   * @param context ExecutionContext object
   * @return Object of type OrganizedOperands
   */
  abstract OrganizedOperands organizeOperands(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException;

  abstract void addUnevaluatedFilterOperands(List unevaluatedFilterOps);


}
