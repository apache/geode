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
package com.gemstone.gemfire.cache.query.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Conjunctions and Disjunctions (LITERAL_and LITERAL_or) As a part of feature
 * development to ensure index usage for multiple regions & index usage in equi
 * join conditions across the regions , a CompiledJunction's organized operands
 * method creates internal structures like GroupJunction, AllGroupJunction &
 * CompositeGroupJunction. The method createJunction is where the decision of
 * creating an AllGroupJunction ( wrapping multiple GroupJunctions and
 * CompositeGroupJunctions) or a single GroupJunction or a singe
 * CompositeGroupJunction is taken.
 * 
 *          CompiledJunction -----> on organization of Operands may result in any 
 *          of the following cases
 *          
 *          1)    CompiledJunction
 *                     |
 *                     |
 *          --------------------------------
 *          |                               |                     
 *       CompiledJunction                GroupJunction (Filter evaluable)
 *       (Filter evaluable)   
 *       
 *       2)    CompiledJunction
 *                     |
 *                     |
 *          --------------------------------
 *          |                               |                     
 *       CompiledJunction                RangeJunction (Filter evaluable same index operands. May contain iter operands belonging to the same Group)
 *       (Filter evaluable)
 *           
 *           
 *      3)      CompiledJunction
 *                     |
 *                     |
 *          --------------------------------
 *          |                               |                     
 *       CompiledJunction                GroupJunction (Filter evaluable)
 *       (Filter evaluable)                     |
 *                                                | 
 *                                      ----------------------------------------------
 *                                      |                                             |
 *                                   CompiledComparison                               RangeJunction 
 *                            (at least one filter evaluable compiled
 *                             Comparison using an index shared by no other
 *                             condition)           
 *                             
 *                             
 *     5)      CompiledJunction
 *                     |
 *                     |
 *          --------------------------------
 *          |                               |                     
 *       CompiledJunction                GroupJunction (Filter evaluable)
 *       (Filter evaluable)                     |
 *                                                | 
 *                                      ---------------------------------------
 *                                      |                     |                |
 *                                   RangeJunction          RangeJunction   CompiledComparison
 *                                                                            (zero or more Filter evaluable or iter
 *                                                                             evaluable )      
 * 
 *   6)    CompiledJunction
 *             |
 *     ----------------------------------
 *     |                                 |
 *   CompiledJunction                AllGroupJunction
 *   (filter evaluable)                  |
 *                             ------------------------------------------------
 *                             |                             |                 |
 *                          GroupJunction               GroupJunction       CompositeGroupJunction
 *                                                                                |
 *                                                                            ---------------------------------------
 *                                                                            |             |                        |
 *                                                                         GroupJunction  equi join conditions    GroupJunction       
 * 
 * 
 * 
 * @version $Revision: 1.2 $                                  
 * @author ericz
 * @author Asif
 */
public class CompiledJunction extends AbstractCompiledValue implements
    Negatable {

  /** left operand */
  private final CompiledValue[] _operands;
  private int _operator = 0;
  private List unevaluatedFilterOperands = null; 
  
  //A token to place into the samesort map.  This is to let the engine know there is more than one index
  //being used for this junction but allows actual operands to form range junctions if enough exist. 
  //The mechanism checks to see if the mapped object is an integer, if so, it increments, if it's not it sets as 1
  //Because we are a string place holder, the next actual operand would just start at one.  If the join is added
  //after a valid operand has already set the counter to an integer, we instead just ignore and do not set the place holder
  private final static String PLACEHOLDER_FOR_JOIN = "join";  
  CompiledJunction(CompiledValue[] operands, int operator) {
    // invariant: operator must be LITERAL_and or LITERAL_or
    // invariant: at least two operands
    if (!((operator == LITERAL_or || 
        operator == LITERAL_and) && operands.length >= 2)) { 
      throw new InternalGemFireError(
        "operator=" + operator + "operands.length =" + operands.length); }
    _operator = operator;
    _operands = operands;
  }
  
  @Override
  public List getChildren() {
    return Arrays.asList(this._operands);
  }

  public int getType() {
    return JUNCTION;
  }

  public Object evaluate(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    Object r = _operands[0].evaluate(context); // UNDEFINED, null, or a Boolean
    // if it's true, and op is or then return true immediately
    // if it's false and the op is and then return false immediately
    if (r instanceof Boolean)
        if (((Boolean) r).booleanValue() && _operator == LITERAL_or)
          return r;
        else if (!((Boolean) r).booleanValue() && _operator == LITERAL_and)
            return r;
    if (r == null || r == QueryService.UNDEFINED)
      r = QueryService.UNDEFINED; // keep going to see if we hit a
    // short-circuiting truth value
    else if (!(r instanceof Boolean))
        throw new TypeMismatchException(LocalizedStrings.CompiledJunction_LITERAL_ANDLITERAL_OR_OPERANDS_MUST_BE_OF_TYPE_BOOLEAN_NOT_TYPE_0.toLocalizedString(r.getClass().getName()));
    for (int i = 1; i < _operands.length; i++) {
      Object ri = null;
      try {
        ri = _operands[i].evaluate(context); // UNDEFINED, null, or
      } catch (EntryDestroyedException ede) {
        continue;
      }
      // Boolean
      if (ri instanceof Boolean)
          if (((Boolean) ri).booleanValue() && _operator == LITERAL_or)
            return ri;
          else if (!((Boolean) ri).booleanValue() && _operator == LITERAL_and)
              return ri;
      if (ri == null || ri == QueryService.UNDEFINED
          || r == QueryService.UNDEFINED) {
        r = QueryService.UNDEFINED;
        continue; // keep going to see if we hit a short-circuiting truth value
      }
      else if (!(ri instanceof Boolean))
          throw new TypeMismatchException(LocalizedStrings.CompiledJunction_LITERAL_ANDLITERAL_OR_OPERANDS_MUST_BE_OF_TYPE_BOOLEAN_NOT_TYPE_0.toLocalizedString(ri.getClass().getName()));
      // now do the actual and/or
      if (_operator == LITERAL_and)
        r = Boolean.valueOf(((Boolean) r).booleanValue()
            && ((Boolean) ri).booleanValue());
      else
        // LITERAL_or
        r = Boolean.valueOf(((Boolean) r).booleanValue()
            || ((Boolean) ri).booleanValue());
    }
    return r;
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    for (int i = 0; i < _operands.length; i++) {
      context.addDependencies(this, this._operands[i]
          .computeDependencies(context));
    }
    return context.getDependencySet(this, true);
  }

  @Override
  public SelectResults filterEvaluate(ExecutionContext context,
      SelectResults intermediateResults) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException
  {
    OrganizedOperands newOperands = organizeOperands(context);
    SelectResults result = intermediateResults;
    Support.Assert(newOperands.filterOperand != null);
    // evaluate directly on the operand
    result = newOperands.isSingleFilter ? (newOperands.filterOperand)
        .filterEvaluate(context, result) : (newOperands.filterOperand)
        .auxFilterEvaluate(context, result);
    if (!newOperands.isSingleFilter) {
      List unevaluatedOps = ((CompiledJunction)newOperands.filterOperand).unevaluatedFilterOperands;
      if (unevaluatedOps != null) {
        if (newOperands.iterateOperand == null) {
          if (unevaluatedOps.size() == 1) {
            newOperands.iterateOperand = (CompiledValue)unevaluatedOps.get(0);
          }
          else {
            int len = unevaluatedOps.size();
            CompiledValue[] iterOps = new CompiledValue[len];
            for (int i = 0; i < len; i++) {
              iterOps[i] = (CompiledValue)unevaluatedOps.get(i);
            }
            newOperands.iterateOperand = new CompiledJunction(iterOps,
                getOperator());
          }
        }
        else {
          // You cannot have two CompiledJunctions a the same level
          if (newOperands.iterateOperand instanceof CompiledJunction) {
            CompiledJunction temp = (CompiledJunction)newOperands.iterateOperand;
            List prevOps = temp.getOperands();
            unevaluatedOps.addAll(prevOps);
          }
          else {
            unevaluatedOps.add(newOperands.iterateOperand);
          }
          int totalLen = unevaluatedOps.size();
          CompiledValue[] combinedOps = new CompiledValue[totalLen];
          Iterator itr = unevaluatedOps.iterator();
          int j = 0;
          while (itr.hasNext()) {
            combinedOps[j++] = (CompiledValue)itr.next();
          }
          newOperands.iterateOperand = new CompiledJunction(combinedOps,
              getOperator());
        }
      }
    }
    if (newOperands.iterateOperand != null)
      // call private method here to evaluate
      result = auxIterateEvaluate(newOperands.iterateOperand, context, result);
    return result;
  }

  private List getCondtionsSortedOnIncreasingEstimatedIndexResultSize(
      ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // The checks invoked before this function have ensured that all the
    // operands are of type ComparisonQueryInfo and of the form 'var = constant'.
    // Also need for sorting will not arise if there are only two operands
    int len = this._operands.length;
    List sortedList = new ArrayList(len);
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
   * invariant: all operands are known to be evaluated as a filter no operand
   * organization is necessary
   */
  @Override
  public SelectResults auxFilterEvaluate(ExecutionContext context,
      SelectResults intermediateResults) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // evaluate the result set from the indexed values
    // using the intermediate results so far (passed in)
    // put results into new intermediate results
    List sortedConditionsList = this.getCondtionsSortedOnIncreasingEstimatedIndexResultSize(context);

    // Sort the operands in increasing order of resultset size
    Iterator sortedConditionsItr = sortedConditionsList.iterator();
    while (sortedConditionsItr.hasNext()) {
      // Asif:TODO The intermediate ResultSet should be passed as null when invoking
      // filterEvaluate. Just because filterEvaluate is being called, itself
      // guarantees that there will be at least on auxFilterEvalaute call.
      // The filterEvalaute will return after invoking auxIterEvaluate.
      // The auxIterEvaluate cannot get invoked for an OR junction.
      // The approach of calculation of resultset should be Bubble UP ( i.e
      // bottom to Top of the tree. This implies that for filterEvaluate, we
      // should not be passing IntermediateResultSet.
      // Each invocation of filterEvaluate will be complete in itself with the
      // recursion being ended by evaluating auxIterEvaluate if any. The passing
      // of IntermediateResult in filterEvalaute causes AND junction evaluation
      // to be corrupted , if the intermediateResultset contains some value.
      SelectResults filterResults = ((Filter) sortedConditionsItr.next()).filterEvaluate(context, null);
      if (_operator == LITERAL_and) {
        if (filterResults != null && filterResults.isEmpty()) {
          return filterResults;
        }
        else if (filterResults != null) {
          intermediateResults = (intermediateResults == null) ? filterResults
              : QueryUtils.intersection(intermediateResults, filterResults, context);

          sortedConditionsItr.remove();

          if (intermediateResults.size() <= indexThresholdSize) {
            // Abort further intersection , the remaining filter operands will be
            // transferred for
            // iter evaluation
            break;
          }
        }
      }
      else {
        //Asif : In case of OR clause, the filterEvaluate cannot return a
        // null value ,as a null value implies Cartesian of Iterators
        // in current scope which can happen only if the operand of OR
        // clause evaluates to true (& which we are not allowing).
        // Though if the operand evaluates to false,it is permissible case
        // for filterEvaluation.
        Assert.assertTrue(filterResults != null);
        intermediateResults = intermediateResults == null ? filterResults
            : QueryUtils.union(intermediateResults, filterResults, context);
      }
    }
    if (_operator == LITERAL_and && !sortedConditionsList.isEmpty()) {          
      this.unevaluatedFilterOperands = sortedConditionsList;
    }    
    return intermediateResults;
  }

  /** invariant: the operand is known to be evaluated by iteration */
   SelectResults auxIterateEvaluate(CompiledValue operand,
      ExecutionContext context, SelectResults intermediateResults)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // Asif: This can be a valid value if we have an AND condition like ID=1 AND
    // true = true. In such cases , if an index is not available on ID still we
    // have at least one expression which is independent of current scope. In
    // such cases a value of null means ignore the null ( because it means all
    // the results ) thus a the result-set of current auxIterate which will be
    // the subset of total result-set , will be the right data.
     
    // auxIterEvalaute can be called only from an AND junction.
    
     // This also implies that there will be at least one operand which will be
    // evaluated using auxFilterEvaluate.Thus the result-set given to this
    // function can be empty ( obtained using auxFilterEvaluate ,meaning no need
    // to do iterations below ) but it can never be null. As null itself
    // signifies that the junction cannot be evaluated as a filter.
    if (intermediateResults == null)
        throw new RuntimeException(LocalizedStrings.CompiledJunction_INTERMEDIATERESULTS_CAN_NOT_BE_NULL.toLocalizedString());
    if (intermediateResults.isEmpty()) // short circuit
        return intermediateResults;
    List currentIters = context.getCurrentIterators();
    RuntimeIterator rIters[] = new RuntimeIterator[currentIters.size()];
    currentIters.toArray(rIters);
    ObjectType elementType = intermediateResults.getCollectionType()
        .getElementType();
    SelectResults resultSet ;
    if(elementType.isStructType()) {
      resultSet = QueryUtils.createStructCollection(context, (StructTypeImpl) elementType) ;
    }else {
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
        }
        else {
          rIters[0].setCurrent(tuple);
        }
        Object result = null;
        try {
          result = operand.evaluate(context);
        }
        finally {
          observer.afterIterationEvaluation(result);
        }
        if (result instanceof Boolean) {
          if (((Boolean) result).booleanValue()) resultSet.add(tuple);
        }
        else if (result != null && result != QueryService.UNDEFINED)
            throw new TypeMismatchException(LocalizedStrings.CompiledJunction_ANDOR_OPERANDS_MUST_BE_OF_TYPE_BOOLEAN_NOT_TYPE_0.toLocalizedString(result.getClass().getName()));
      }
    }
    finally {
      observer.endIteration(resultSet);
    }
    return resultSet;
  }

  /**
   * invariant: all operands are known to be evaluated as a filter no operand
   * organization is necessary
   */
  public void negate() {
    _operator = inverseOperator(_operator);
    for (int i = 0; i < _operands.length; i++) {
      if (_operands[i] instanceof Negatable)
        ((Negatable) _operands[i]).negate();
      else
        _operands[i] = new CompiledNegation(_operands[i]);
    }
  }

  @Override
  protected PlanInfo protGetPlanInfo(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    Support.Assert(_operator == LITERAL_or || _operator == LITERAL_and);
    PlanInfo resultPlanInfo = new PlanInfo();
    // set default evalAsFilter depending on operator
    boolean isOr = (_operator == LITERAL_or);
    resultPlanInfo.evalAsFilter = isOr;
    // collect indexes
    // for LITERAL_and operator, if any say yes to filter,
    //     then change default evalAsFilter from false to true
    // of LITERAL_or operator, if any say no to filter, change to false
    for (int i = 0; i < _operands.length; i++) {
      PlanInfo opPlanInfo = _operands[i].getPlanInfo(context);
      resultPlanInfo.indexes.addAll(opPlanInfo.indexes);
      if (!isOr && opPlanInfo.evalAsFilter) {
        resultPlanInfo.evalAsFilter = true;
      }
      else if (isOr && !opPlanInfo.evalAsFilter) {
        resultPlanInfo.evalAsFilter = false;
      }
    }
    return resultPlanInfo;
  }

  /* Package methods */
  public int getOperator() {
    return _operator;
  }

  List getOperands() {
    // return unmodifiable copy
    return Collections.unmodifiableList(Arrays.asList(_operands));
  }


  /**
   * TODO: Should composite operands be part of iterator operands of
   * CompiledJunction or should it be part of AllGroupJunction Write a unit Test
   * for this function.
   *
   * Asif: The iterators which can be considered part of GroupJunction are
   * those which are exclusively dependent only on the independent
   * iterator of the group. Any iterator which is ultimately dependent
   * on more than one independent iterators cannot be assumed to be part of
   * the GroupJunction, even if one of independent iterator belongs to a
   * different scope.
   * 
   * @param context
   * @return New combination of AbstractCompiledValue(s) in form of CompiledJunction.
   * @throws FunctionDomainException
   * @throws TypeMismatchException
   * @throws NameResolutionException
   * @throws QueryInvocationTargetException
   */
  OrganizedOperands organizeOperands(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    // get the list of operands to evaluate, and evaluate operands that can use
    // indexes first.
    List evalOperands = new ArrayList(_operands.length);
    int indexCount = 0;
    // TODO: Check if we can defer the creation of this array list only
    // if there exists an eval operand
    List compositeIterOperands = new ArrayList(_operands.length);
    // Asif: This Map will contain as key the composite filter operand & as
    // value , the set containing independent RuntimeIterators ( which will
    // necessarily be two )
    Map compositeFilterOpsMap = new LinkedHashMap();
    Map iterToOperands = new HashMap();
    CompiledValue operand = null;
    boolean isJunctionNeeded = false;
    boolean indexExistsOnNonJoinOp = false;

    for (int i = 0; i < _operands.length; i++) {
      // Asif : If we are inside this function this itself indicates
      // that there exists at least on operand which can be evaluated
      // as an auxFilterEvaluate. If any operand even if its flag of
      // evalAsFilter happens to be false, but if it is independently
      // evaluable, we should attach it with the auxFilterevaluable
      // operands irrespective of its value ( either true or false.)
      // This is because if it is true or false, it can always be paired
      // up with other filter operands for AND junction. But for
      // OR junction it can get paired with the filterOperands only
      // if it is false. But we do not have to worry about whether
      // the junction is AND or OR because, if the independent operand's
      // value is true & was under an OR junction , it would not have
      // been filterEvaluated. Instead it would have gone for auxIterEvaluate.
      // We are here itself implies, that any independent operand can be
      // either true or false for an AND junction but always false for an
      // OR Junction.
      operand = this._operands[i];
      if (!operand.isDependentOnCurrentScope(context)) {
        indexCount++;
        // Asif Ensure that independent operands are always at the start
        evalOperands.add(0, operand);
      }
      else if (operand instanceof CompiledJunction) {
        if (operand.getPlanInfo(context).evalAsFilter) {
          // Asif Ensure that independent operands are always at the start
          evalOperands.add(indexCount++, operand);
        }
        else {
          evalOperands.add(operand);
        }
      }
      else {
        // If the operand happens to be a Like predicate , this is the point
        // where we can expand as now the structures created are all for the current
        // execution. We expand only if the current junction is AND because if its 
        // OR we cannot bring Like at level of OR , because like itself is an AND
        // Also cannot expand for NOT LIKE because CCs generated by CompiledLike with AND
        // will be converted to OR by negation
        CompiledValue expandedOperands[] = null;
        
        if (operand.getType() == LIKE
            && this._operator == OQLLexerTokenTypes.LITERAL_and
            && ((CompiledLike) operand).getOperator() != OQLLexerTokenTypes.TOK_NE) {
          expandedOperands = ((CompiledLike) operand)
              .getExpandedOperandsWithIndexInfoSetIfAny(context);
        } else {
          expandedOperands = new CompiledValue[] { operand };
        }
        
        for (CompiledValue expndOperand : expandedOperands) {
          boolean operandEvalAsFilter = expndOperand.getPlanInfo(context).evalAsFilter;
          isJunctionNeeded = isJunctionNeeded
              || operandEvalAsFilter;
          Set set = QueryUtils.getCurrentScopeUltimateRuntimeIteratorsIfAny(
              expndOperand, context);
          if (set.size() != 1) {
            // Asif: A set of size not equal to 1 ( which can mean anything more
            // than 1, will mean a composite condition. For a Composite
            // Condition which is filter evaluable that means necessarily that
            // RHS is dependent on one independent iterator & LHS on the other.
            if (operandEvalAsFilter) {
              Support.Assert(set.size() == 2,
                  " The no of independent iterators should be equal to 2");
              compositeFilterOpsMap.put(expndOperand, set);
            }
            else {
              compositeIterOperands.add(expndOperand);
            }
          }
          else {
            Support
                .Assert(set.size() == 1,
                    "The size has to be 1 & cannot be zero as that would mean it is independent");
            RuntimeIterator rIter = (RuntimeIterator)set.iterator().next();
            List operandsList = (List)iterToOperands.get(rIter);
            if (operandsList == null) {
              operandsList = new ArrayList();
              iterToOperands.put(rIter, operandsList);
            }
            if (operandEvalAsFilter && _operator == LITERAL_and) {
              indexExistsOnNonJoinOp = true;
            }
            operandsList.add(expndOperand);
          }
        }
      }
    }
    /*
     * Asif : If there exists a SingleGroupJunction & no other Filter operand ,
     * then all remaining eval operands ( even if they are CompiledJunction which
     * are not evaluable as Filter) & composite operands should become part of
     * GroupJunction. If there exists only SingleGroupJunction & at least one
     * Filter operand, then all conditions of GroupJunction which are not filter
     * evaluable should become iter operand of CompiledJunction. If there
     * exists an AllGroupJunction & no Filter operand, then all remaining iter
     * operands of CompiledJunction along with Composite operands should be part
     * of AllGroupJunction. If there exists at least one Filter operand , all
     * remaining operands of AllGroupJunction ( including those GroupJunction not
     * filter evaluable should become part of eval operand of
     * CompiledJunction) The index count will give the idea of number of filter
     * operands
     */
    if (isJunctionNeeded) {
      // There exists at least one condition which must have an index available.
      Filter junction = createJunction(compositeIterOperands,
          compositeFilterOpsMap, iterToOperands, context, indexCount,
          evalOperands, indexExistsOnNonJoinOp);
      // Asif Ensure that independent operands are always at the start
      evalOperands.add(indexCount++, junction);
    }
    else {
      // if(compositeIterOperands != null) {
      if (!compositeIterOperands.isEmpty()) {
        evalOperands.addAll(compositeIterOperands);
      }
      Iterator itr = iterToOperands.values().iterator();
      while (itr.hasNext()) {
        evalOperands.addAll((List) itr.next());
      }
    }
    OrganizedOperands result = new OrganizedOperands();
    // Group the operands into those that use indexes and those that don't,
    // so we can evaluate all the operands that don't use indexes together in
    // one iteration first get filter operands
    Filter filterOperands = null;
    // there must be at least one filter operand or else we wouldn't be here
    Support.Assert(indexCount > 0);
    if (indexCount == 1) {
      filterOperands = (Filter) evalOperands.get(0);
      result.isSingleFilter = true;
    }
    else {
      CompiledValue[] newOperands = new CompiledValue[indexCount];
      for (int i = 0; i < indexCount; i++)
        newOperands[i] = (CompiledValue) evalOperands.get(i);
      filterOperands = new CompiledJunction(newOperands, _operator);
    }
    // get iterating operands
    // INVARIANT: the number of iterating operands when the _operator is
    // LITERAL_or must be zero. This is an invariant on filter evaluation
    CompiledValue iterateOperands = null;
    //int numIterating = _operands.length - indexCount;
    int numIterating = evalOperands.size() - indexCount;
    Support.Assert(_operator == LITERAL_and || numIterating == 0);
    if (numIterating > 0) {
      if (numIterating == 1)
        iterateOperands = (CompiledValue) evalOperands.get(indexCount);
      else {
        CompiledValue[] newOperands = new CompiledValue[numIterating];
        for (int i = 0; i < numIterating; i++)
          newOperands[i] = (CompiledValue) evalOperands.get(i + indexCount);
        iterateOperands = new CompiledJunction(newOperands, _operator);
      }
    }
    result.filterOperand = filterOperands;
    result.iterateOperand = iterateOperands;
    return result;
  }
  
  /**
   * Creates a GroupJunction or a RangeJunction based on the operands passed.
   * The operands are either Filter Operands belonging to single independent
   * RuntimeIterator or are iter evaluable on the RuntimeIterator[] passed as
   * parameter. If all the filter evaluable operands use a single Index , than
   * a RangeJunction is created with iter operand as part of RangeJunction. If
   * there exists operands using multiple indexes, then a GroupJunction is
   * created. In that case , the operands using the same index are grouped in a
   * RangeJunction & that Range Junction becomes part of the GroupJunction. A
   * GroupJunction may contain one or more RangeJunction
   * 
   * @param needsCompacting
   *                boolean indicating if there is a possibility of
   *                RangeJunction or not. If needsCompacting is false , then a
   *                GroupJunction will be formed, without any RangeJunction
   * @param grpIndpndntItr
   *                Array of RuntimeIterator which represents the independent
   *                iterator for the Group. for all cases , except a
   *                CompositeGroupJunction containing a single GroupJunction ,
   *                will have a single RuntimeIterator in this array.
   * @param completeExpnsn
   *                boolean indicating whether the Group or Range Junction needs
   *                to be expanded to the query from clause level or the group
   *                level
   * @param cv
   *                Array of CompiledValue containing operands belonging to a
   *                group
   * @param sameIndexOperands
   *                Map object containing Index as the key & as a value an
   *                Integer object or a List. If it contains an Integer as value
   *                against Index, it implies that there is only one operand
   *                which uses that type of index & hence cannot form a
   *                RangeJunction. If it contains a List then the Lis contains
   *                the operands which use same index & thus can form a
   *                RangeJunction
   * @return Object of GroupJunction or RangeJunction as the case may be
   */
  private AbstractGroupOrRangeJunction createGroupJunctionOrRangeJunction(
      boolean needsCompacting, RuntimeIterator[] grpIndpndntItr,
      boolean completeExpnsn, CompiledValue[] cv, Map sameIndexOperands) {
    AbstractGroupOrRangeJunction junction;
    if (needsCompacting) {
      Iterator itr = sameIndexOperands.values().iterator();
      if (sameIndexOperands.size() == 1) {
        // This means that there exists only one type of Index for the whole
        // Group. Thus overall we should just create a RangeJunction. It also
        // means that the rest of the operands are the Iter Operands ( they may
        // contain those operands which are not index evaluable but evaluate to
        // true/false ( not dependent on current scope)
        List sameIndexOps = (List)itr.next();
        Iterator sameIndexOpsItr = sameIndexOps.iterator();
        for (int i = 0; i < cv.length; ++i) {
          if (cv[i] == null) {
            cv[i] = (CompiledValue)sameIndexOpsItr.next();
          }
        }
        junction = new RangeJunction(this._operator, grpIndpndntItr,
            completeExpnsn, cv);
      }
      else {
        int numRangeJunctions = 0;
        CompiledValue[] rangeJunctions = new CompiledValue[sameIndexOperands
            .size()];
        int nullifiedFields = 0;
        while (itr.hasNext()) {
          Object listOrPosition = itr.next();
          if (listOrPosition instanceof List) {
            List ops = (List)listOrPosition;
            nullifiedFields += ops.size();
            CompiledValue operands[] = (CompiledValue[])ops
                .toArray(new CompiledValue[ops.size()]);
            rangeJunctions[numRangeJunctions++] = new RangeJunction(
                this._operator, grpIndpndntItr, completeExpnsn, operands);
          }
        }
        int totalOperands = cv.length - nullifiedFields + numRangeJunctions;
        CompiledValue[] allOperands = new CompiledValue[totalOperands];
        // Fill the Non RangeJunction operands first
        int k = 0;
        for (int i = 0; i < cv.length; ++i) {
          CompiledValue tempCV = cv[i];
          if (tempCV != null) {
            allOperands[k++] = tempCV;
          }
        }
        for (int i = 0; i < numRangeJunctions; ++i) {
          allOperands[k++] = rangeJunctions[i];
        }
        junction = new GroupJunction(this._operator, grpIndpndntItr,
            completeExpnsn, allOperands);
      }
    }
    else {
      junction = new GroupJunction(this._operator, grpIndpndntItr,
          completeExpnsn, cv);
    }
    return junction;
  }
  
  private boolean sortSameIndexOperandsForGroupJunction(CompiledValue cv[],
      List operandsList, Map sameIndexOperands, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException,
      NameResolutionException, FunctionDomainException,
      QueryInvocationTargetException {
    int size = operandsList.size();
    CompiledValue tempOp = null;
    boolean needsCompacting = false;
    for (int i = 0; i < size; ++i) {
      tempOp = (CompiledValue)operandsList.get(i);
      IndexInfo[] indx = null;
      Object listOrPosition = null;
      boolean evalAsFilter = tempOp.getPlanInfo(context).evalAsFilter;
      //TODO:Do not club Like predicate in an existing range
      if (evalAsFilter ) {
        indx = ((Indexable)tempOp).getIndexInfo(context);
        //We are now sorting these for joins, therefore we need to weed out the join indexes
        if (!IndexManager.JOIN_OPTIMIZATION || indx.length == 1) {
          Assert.assertTrue(indx.length == 1,
              "There should have been just one index for the condition");
          listOrPosition = sameIndexOperands.get(indx[0]._index);
        }
      }
    
     if (listOrPosition != null) {
        if (listOrPosition instanceof Integer) {
          int position = ((Integer)listOrPosition).intValue();
          List operands = new ArrayList(size);
          operands.add(cv[position]);
          operands.add(tempOp);
          cv[position] = null;
          sameIndexOperands.put(indx[0]._index, operands);
          needsCompacting = true;
        }
        else if (listOrPosition instanceof List){
          List operands = (List)listOrPosition;
          operands.add(tempOp);
        }
        else {
          //a join was present here, let's now occupy that spot and remove the placeholder
          listOrPosition = null;
        }
     }
      if (listOrPosition == null) {
        cv[i] = tempOp;
        if (indx != null && indx.length == 1) {
          // TODO: Enable only for AND junction for now
          if (evalAsFilter && this._operator == OQLLexerTokenTypes.LITERAL_and) {
            sameIndexOperands.put(indx[0]._index, Integer.valueOf(i));
          }
        } else if (indx != null && indx.length == 2) {
          if (evalAsFilter && this._operator == OQLLexerTokenTypes.LITERAL_and) {
            if (!sameIndexOperands.containsKey(indx[0]._index)) {
              sameIndexOperands.put(indx[0]._index, PLACEHOLDER_FOR_JOIN);
            }
            if (!sameIndexOperands.containsKey(indx[1]._index)) {
              sameIndexOperands.put(indx[1]._index, PLACEHOLDER_FOR_JOIN);
            }
          }
        }
      }
    }
    return needsCompacting;
  }
  //This is called only if the CompiledJunction was either independent or filter evaluable.
  public int getSizeEstimate(ExecutionContext context)throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException  {
    if( this.isDependentOnCurrentScope(context)) {    
  return Integer.MAX_VALUE;
    }else {
      return 0;
    }
  }

  // TODO: Optmize this function further in terms of creation of Arrays &
  // Lists
  private Filter createJunction(List compositeIterOperands,
      Map compositeFilterOpsMap, Map iterToOperands, ExecutionContext context,
      int indexCount, List evalOperands, boolean indexExistsOnNonJoinOp) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    Support.Assert(!(iterToOperands.isEmpty() && compositeFilterOpsMap
        .isEmpty()),
        " There should not have been any need to create a Junction");
    CompiledValue junction = null;
    int size;
    /*---------- Create only a  GroupJunction */
    if (iterToOperands.size() == 1 && (compositeFilterOpsMap.isEmpty()
        || (indexExistsOnNonJoinOp && IndexManager.JOIN_OPTIMIZATION))) {
      if ((indexExistsOnNonJoinOp && IndexManager.JOIN_OPTIMIZATION)) {
        // For the optimization we will want to add the compositeFilterOpsMap 848
        // without the optimization we only fall into here if it's empty anyways, but have not tested the removal of this if clause
        evalOperands.addAll(compositeFilterOpsMap.keySet());
      }
      // Asif :Create only a GroupJunction. The composite conditions can be
      // evaluated as iter operands inside GroupJunction.
      Map.Entry entry = (Map.Entry) iterToOperands.entrySet().iterator().next();
      List operandsList = (List) entry.getValue();
      // Asif: If there exists no other filter operand, transfer all the
      // remaining iter operands to the Group Junction. Else transfer all the
      // remaining operands to eval operands of CompiledJunctio. This means that
      // we are following the logic that as far as possible minimize the
      // result-set used for iter evaluation of conditions without index.
      // This way we also save on extra iterations for eval conditions if they
      // existed both in GroupJunction as well as CompiledJunction. The only
      // exception in this logic is the iter evaluable conditions belonging to a
      // GroupJunction.
      // If a condition is dependent on a Single Group & is not filter evaluable
      // we do not push it out to iter operands of AllGroupJunction or Compiled
      // Junction , but make it part of GroupJunction itself. This helps bcoz we
      // are sure that the condition will be completely evaluable by the
      // iterators of that group itself.
      if (indexCount == 0) {
        operandsList.addAll(evalOperands);
        evalOperands.clear();
        if (!compositeIterOperands.isEmpty()) {
          operandsList.addAll(compositeIterOperands);
        }
      }
      else {
        if (!compositeIterOperands.isEmpty()) {
          evalOperands.addAll(compositeIterOperands);
        }
        Iterator itr = operandsList.iterator();
        while (itr.hasNext()) {
          CompiledValue cv = (CompiledValue) itr.next();
          // Asif : Those conditions not filter evaluable are transferred to
          // eval operand of CompiledJunction.
          if (!cv.getPlanInfo(context).evalAsFilter) {
            itr.remove();
            evalOperands.add(cv);
          }
        }
      }
      size = operandsList.size();      
      Map sameIndexOperands = new HashMap(size);
      CompiledValue cv[] = new CompiledValue[size];
      //Enable only for AND junction
      boolean needsCompacting = sortSameIndexOperandsForGroupJunction(cv,operandsList, sameIndexOperands, context);      
      junction = createGroupJunctionOrRangeJunction(needsCompacting, new RuntimeIterator[] { (RuntimeIterator)entry.getKey()},  true /* need a Complete expansion */, cv, sameIndexOperands);      
    }
    /*
     * Asif : An AllGroupJunction or a CompositeGroupJunction can get created.
     * An AllGroupJunction will get created , if there exists a need to create
     * more than one CompositeGroupJunction AND/OR more than one GroupJunctions (
     * not part of CompositeGroupJunction) If suppose the where clause contained
     * two sets of conditions each dependent exclusively on a separate
     * independent group ( implying two independent groups) then we should
     * create an AllGroupJunction iff the two Groups are both filter
     * evaluable. If an AllGroupJunction contains only one filter evaluable
     * Group & no CompositeGroupJunction then we should just create a
     * singleGroupJunction with complete expansion as true
     */
    else {
      // Asif : First create the required amount of CompositeGroupJunctions
      // A map which keeps track of no. of independent iterators pointing to the
      // same CompositeGroupJunction. The integer count will keep track of
      // number of different CompositeGroupJunctions which got created.
      Map tempMap = new HashMap();
      Set entries = compositeFilterOpsMap.entrySet();
      Set cgjs = new HashSet(compositeFilterOpsMap.size());
      Iterator entryItr = entries.iterator();
      while (entryItr.hasNext()) {
        Map.Entry entry = (Map.Entry) entryItr.next();
        CompiledValue op = (CompiledValue) entry.getKey();
        Set indpndtItrSet = (Set) entry.getValue();
        Iterator itr = indpndtItrSet.iterator();
        CompositeGroupJunction cgj = null;
        RuntimeIterator[] absentItrs = new RuntimeIterator[2];
        int k = 0;
        while (itr.hasNext()) {
          RuntimeIterator ritr = (RuntimeIterator) itr.next();
          CompositeGroupJunction temp = (CompositeGroupJunction) tempMap
              .get(ritr);
          if (temp == null) {
            absentItrs[k++] = ritr;
          }
          if (cgj == null && temp != null) {
            cgj = temp;
            cgj.addFilterableCompositeCondition(op);
          }
          else if (cgj != null && temp != null && cgj != temp) {
            // Asif A peculiar case wherein we have some thing like four regions
            // with relation ship conditions like r1 = r2 and r3 = r4 and r4 =
            // r1. In such situations we should just have a single
            // CompositeJunction containing r1 r2 r3 and r4 We will merge temp
            // into CompositeGroupJunction & change all the values in tempMap
            // which contained temp object to CompositeGroupJunction.
            // Also we will remove the entry of temp from CompositeGroupJunctions.
            cgj.mergeFilterableCCsAndIndependentItrs(temp);
            cgjs.remove(temp);
            Iterator entriesItr = tempMap.entrySet().iterator();
            Map.Entry tempEntry = null;
            while (entriesItr.hasNext()) {
              tempEntry = (Map.Entry) entriesItr.next();
              if (tempEntry.getValue() == temp) {
                tempEntry.setValue(cgj);
              }
            }
          }
        }
        if (cgj == null) {
          // Asif Create a new CompositeGroupJunction
          cgj = new CompositeGroupJunction(this._operator, op /* CompositeFilterOperand */);
          cgjs.add(cgj);
        }
        for (int i = 0; i < k; ++i) {
          tempMap.put(absentItrs[i], cgj);
          cgj.addIndependentIterators(absentItrs[i]);
        }
      }
      // Asif :If no of cgj is zero definitely there will be an AllGroupJunction
      // If no. of cgj is more than 1 , then also there will be an AllGroupJunction.
      // But if no. of cgj is excactly one & all the GroupJunctions can be
      // accomodated within the CGJ , then we will have CompsiteGroupJunction.
      // Also , those composite conditions which are not filter evaluable will
      // be added to CompositeGroupJunction as iter operands only if we don't
      // have any additional filter & CompositeGroupJunction is the only
      // junction which will get created. If there exists any other filter then
      // it will be part of CompiledJunction's iter operand. Basically we will
      // not distinguish between various CompositeConditions which are not
      // filter evaluable & will not try to place it in various
      // ComspoiteGroupJunctions to which they belong.
      List gjs = new ArrayList(iterToOperands.size());
      Iterator itr = iterToOperands.entrySet().iterator();
      Map.Entry entry;
      while (itr.hasNext()) {
        entry = (Map.Entry)itr.next();
        List operandsList = (List)entry.getValue();
        CompiledValue cv[] = new CompiledValue[size = operandsList.size()];
        int j = 0;
        // Asif:An individual GroupJunction is Filter evaluable if at least one
        // operand is filter evaluable.
        boolean evalAsFilter = false;
        boolean needsCompacting = false;
        CompiledValue tempOp = null;
        Map sameIndexOperands = new HashMap(size);

        for (; j < size; ++j) {
          tempOp = (CompiledValue)operandsList.get(j);
          boolean isFilterevaluable = tempOp.getPlanInfo(context).evalAsFilter;
          evalAsFilter = evalAsFilter || isFilterevaluable;
          IndexInfo[] indx = null;
          Object listOrPosition = null;
          if (isFilterevaluable) {
            indx = ((Indexable)tempOp).getIndexInfo(context);
            Assert.assertTrue(indx.length == 1,
                "There should have been just one index for the condition");
            listOrPosition = sameIndexOperands.get(indx[0]._index);
          }
          if (listOrPosition != null) {
            if (listOrPosition instanceof Integer) {
              int position = ((Integer)listOrPosition).intValue();
              List operands = new ArrayList(size);
              operands.add(cv[position]);
              operands.add(tempOp);
              cv[position] = null;
              sameIndexOperands.put(indx[0]._index, operands);
              needsCompacting = true;
            }
            else {
              List operands = (List)listOrPosition;
              operands.add(tempOp);
            }
          }
          else {
            cv[j] = tempOp;
            if (isFilterevaluable
                && this._operator == OQLLexerTokenTypes.LITERAL_and) {
              sameIndexOperands.put(indx[0]._index, Integer.valueOf(j));
            }
          }
        }
        if (!evalAsFilter) {
          // Asif : If there exists at least one Filter then those GroupJunction
          // which are not Filter evaluable should become part of eval operands
          // of CompiledJunction else add the Group operands which is not filter
          // evaluable to the Composite iterator oprands
          List toAddTo = null;
          if (indexCount > 0) {
            toAddTo = evalOperands;
          }
          else {
            toAddTo = compositeIterOperands;
          }
          for (int i = 0; i < size; ++i) {
            toAddTo.add(cv[i]);
          }
        }
        else {
          RuntimeIterator grpIndpndtItr = (RuntimeIterator)entry.getKey();
          AbstractGroupOrRangeJunction gj = createGroupJunctionOrRangeJunction(
              needsCompacting, new RuntimeIterator[] { (RuntimeIterator)entry
                  .getKey() }, false/* Expand only to Group Level */, cv,
              sameIndexOperands);
          CompositeGroupJunction cgj = null;
          if ((cgj = (CompositeGroupJunction)tempMap.get(grpIndpndtItr)) != null) {
            cgj.addGroupOrRangeJunction(gj);
          }
          else {
            gjs.add(gj);
          }
        }
      }
      if (indexCount == 0) {
        // Asif : all the eval operands of CompiledJunction should be part of
        // AllGroupJunction
        compositeIterOperands.addAll(evalOperands);
        evalOperands.clear();
      }
      else {
        // Asif : Add the composite operands to the eval operands so that they
        // are evaluated as part of CompiledJunction.
        if (!compositeIterOperands.isEmpty()) {
          evalOperands.addAll(compositeIterOperands);
        }
      }
      // Asif : An AllGroupJunction will be created iff either the List
      // containing GroupJunctions is not empty ot there exists multiple
      // CompositeGroupJunctions Else a CompositeGroupJunction will be created
      // Convert the List of Indpendent Runtime Iterators of
      // CompositeGroupJunction into Array of Indpendent RuntimeIterators
      Iterator cgjItr = cgjs.iterator();
      while (cgjItr.hasNext()) {
        ((CompositeGroupJunction) cgjItr.next()).setArrayOfIndependentItrs();
      }
      int cgjSize = cgjs.size();
      if (gjs.isEmpty() && cgjSize == 1) {
        CompositeGroupJunction compGrpJnc = (CompositeGroupJunction) cgjs
            .iterator().next();
        compGrpJnc.addIterOperands(compositeIterOperands);
        compGrpJnc.setCompleteExpansionOn();
        junction = compGrpJnc;
      }
      else if (gjs.size() == 1 && cgjSize == 0) {
        // Asif: There exists only a single Filterable GroupJunction & no
        // CompositeGroupJunction so we should just create a GroupJunction
        // instead of an AllGroupJunction . ( In this way the remaining iter
        // operands will get evaluated during the filter evaluation of
        // GroupJunction) For such GroupJunction complete expansion needs to be
        // true & the list of composite iter operands ( which at this point
        // contains the iter operands, should be added to the operands of
        // GroupJunction where they will get orgainzed as iter operands during
        // the call of organizeOperands in GroupJunction
        AbstractGroupOrRangeJunction gjTemp = (AbstractGroupOrRangeJunction) gjs.get(0);
        compositeIterOperands.addAll(gjTemp.getOperands());
        CompiledValue newOps[] = new CompiledValue[compositeIterOperands.size()];
        compositeIterOperands.toArray(newOps);
        // Asif : If gjTemp is a RangeJunction, we will get an instance of
        // RangeJunction else we will get an instance of GroupJunction.
        junction = gjTemp.createNewOfSameType(this._operator,gjTemp.getIndependentIteratorForGroup(), true/* The expansion needs to be complete */, newOps);   
         
      }
      else {
        gjs.addAll(cgjs);
        junction = new AllGroupJunction(gjs, this._operator, compositeIterOperands);
      }
    }
    return (Filter) junction;
  }

  // Asif : This function provides package visbility for testing the
  // output of organizeOperands function.
  OrganizedOperands testOrganizedOperands(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    return organizeOperands(context);
  }

  @Override
  public boolean isProjectionEvaluationAPossibility(ExecutionContext context) throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException
  {
    for (int i=0; i<this._operands.length; ++i) {
      // LIKE gives rise to a JUNCTION in CompiledLike whether wildcard is present or not
      if ((this._operands[i].getType() == JUNCTION || this._operands[i]
          .getType() == LIKE)
          && this._operands[i].getPlanInfo(context).evalAsFilter) {
        return false;
      }
    }
    return true;
  }
  
  /* 
   * This method checks the limit applicability on index intermediate results for
   * junctions and optimizes the limit on index intermediate results ONLY if ONE index
   * is used for whole query and all conditions in where clause use that index. Look at
   * the call hierarchy of this function. There are two cases:
   * 
   * Literal_OR: A junction with OR will contain operands which are CompiledComparison or
   * CompiledJunction (or subjunction). we recursively check if all of those use the same index
   * and if ANY one of those comparisons or subjunctions does not use the index, it retruns false.
   * 
   * Literal_AND: If we get combination of comparisons and subjunctions then limit is NOT
   * applicable on index results. Like, "where ID != 10 AND (ID < 5 OR ID > 10) LIMIT 5". 
   * If we get only comparisons ONLY then if all comparisons use the same index then limit
   * is applicable and this returns true. Like, "where ID != 10 AND (ID < 5 AND ID > 10) LIMIT 5".
   * 
   */
  @Override
  public boolean isLimitApplicableAtIndexLevel(ExecutionContext context) throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException  {
    if(this._operator == LITERAL_or) {
      //There is a slight inefficiency in the sense that if the subjunction ( say AND) cannot apply limit,
      // then limit would not be applied at remaining conditions of OR. But since we have single flag
      //governing the behaviour of applying limit at index level, we cannot make it true for specific clauses
      for (int i=0; i<this._operands.length; ++i) {
        if( !this._operands[i].getPlanInfo(context).evalAsFilter || ((Filter)this._operands[i]).isLimitApplicableAtIndexLevel(context)) {
          return false;
        }
      }
      return true;      
    }else {
      //For limit to be applicable on a AND junction, there should be only one type of index used and rest iter evaluable
      //But since the selection of which index to use happens in GroupJunction created at runtimke, we do not have that
      //information yet, and at this point there would be multiple indexes. Ideally we should invoke this function in GroupJunction,
      //in case we want to support multi index usage again at some point. Till then since it is hard coded to use 1 index
      // we can for the time being return true if there exists atleast one indexable condition
      boolean foundIndex = false;
      for (int i = 0; i < this._operands.length; ++i) {
        if( this._operands[i].getPlanInfo(context).evalAsFilter &&  this._operands[i].getType() == JUNCTION) {
          return false;
        } else if (this._operands[i].getPlanInfo(context).evalAsFilter) {
          foundIndex = true;
        }
      }
      return foundIndex;
    }
  }
  
  @Override
  public boolean isOrderByApplicableAtIndexLevel(ExecutionContext context, String canonicalizedOrderByClause) throws FunctionDomainException, TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    if(this._operator == LITERAL_and) {
      //Set<IndexProtocol> usedIndex = new HashSet<IndexProtocol>();
      boolean foundRightIndex = false;
      for (int i=0; i<this._operands.length; ++i) {
        PlanInfo pi =this._operands[i].getPlanInfo(context); 
        if( pi.evalAsFilter &&  this._operands[i].getType() == JUNCTION) {
          return false;
        }else if(pi.evalAsFilter) {
          if(!foundRightIndex) {
            IndexProtocol ip = (IndexProtocol)this._operands[i].getPlanInfo(context).indexes.get(0);           
            if(ip.getCanonicalizedIndexedExpression().equals(canonicalizedOrderByClause) && pi.isPreferred ) {
              foundRightIndex = true;
            }
          }
          //usedIndex.addAll(this._operands[i].getPlanInfo(context).indexes);
        }
      }
      return foundRightIndex;
//      if(usedIndex.size() == 1 && usedIndex.iterator().next().getCanonicalizedIndexedExpression().equals(canonicalizedOrderByClause)) {
//        return true;
//      }
    }
    return false;
  }
  
  /*
   * class CGJData { RuntimeIterator [] indpndItrs = null; List iterOperands =
   * null; List groupJunctions = null; CompiledValue []
   * compositeCompiledComparisons = null;
   * 
   * CGJData(RuntimeIterator [] indpndItrs, List iterOperands,List
   * groupJunctions, CompiledValue [] compositeCompiledComparisons) {
   * this.indpndItrs = indpndItrs; this.iterOperands = iterOperands;
   * this.groupJunctions = groupJunctions; this.compositeCompiledComparisons =
   * compositeCompiledComparisons; } }
   */
}
