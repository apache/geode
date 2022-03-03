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
 * Created on Oct 25, 2005
 */
package org.apache.geode.cache.query.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;

/**
 * An object of this class gets created during the organization of operands in a CompiledJunction.
 * It gets created if there exists multiple filter evaluable conditions in where clause of the query
 * with those conditions dependent on more than one group of independent iterators. Thus presence of
 * more than one region in a query is a prerequisite for the creation of this object. However,
 * actual creation will occur iff there exists more than one GroupJunction or more than one
 * CompositeGroupJunction or a combination of one or more GroupJunctions & one or more
 * CompositeGroupJunctions
 *
 */
public class AllGroupJunction extends AbstractCompiledValue implements Filter, OQLLexerTokenTypes {

  private List abstractGroupOrRangeJunctions = null;
  private int operator = 0;
  private List iterOperands = null;

  AllGroupJunction(List abstractGroupOrRangeJunctions, int operator, List iterOperands) {
    this.operator = operator;
    this.abstractGroupOrRangeJunctions = abstractGroupOrRangeJunctions;
    if (operator != LITERAL_and) {
      Support.Assert(iterOperands.size() == 0,
          "For OR Junction all operands need to be filterOperands");
    }
    this.iterOperands = iterOperands;
  }

  @Override
  public List getChildren() {
    List children = new ArrayList();
    children.addAll(abstractGroupOrRangeJunctions);
    children.addAll(iterOperands);
    return children;
  }

  @Override
  public Object evaluate(ExecutionContext context) {
    Support.assertionFailed("Should not have come here");
    return null;
  }

  @Override
  public int getType() {
    return ALLGROUPJUNCTION;
  }

  @Override
  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    if (operator == LITERAL_and) {
      return evaluateAndJunction(context);
    } else {
      return evaluateOrJunction(context);
    }
  }

  /**
   * Asif:Evaluates the individual GroupJunctions and CompositeGroupJunctions and does a cartesian
   * of the results so obtained and simultaneously expanding it to the query from clause level as
   * well as evaluating any iter evaluatable conditions. The evaluated result of an AllGroupJunction
   * will always be of the query from clause level which can be ORed or ANDd with filter evaluatable
   * subtree CompiledJunction
   *
   * @param context ExecutionContext object
   */
  // Asif : For doing the Cartesian first evaluate the result of all Group
  // Junction. Doing Cartesian of all the Results together is better than doing
  // in pair
  private SelectResults evaluateAndJunction(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    int len = abstractGroupOrRangeJunctions.size();
    // Asif : Create an array of SelectResults for each of the GroupJunction
    // For each Group Junction there will be a corresponding array of
    // RuntimeIterators which will map to the fields of the ResultSet obtained
    // from Group Junction.
    SelectResults[] results = new SelectResults[len];
    List finalList = context.getCurrentIterators();
    List expansionList = new LinkedList(finalList);
    RuntimeIterator[][] itrsForResultFields = new RuntimeIterator[len][];
    CompiledValue gj = null;
    Iterator junctionItr = abstractGroupOrRangeJunctions.iterator();
    List grpItrs = null;
    int j = 0;
    RuntimeIterator tempItr = null;
    while (junctionItr.hasNext()) {
      gj = (CompiledValue) junctionItr.next();
      SelectResults filterResults = ((Filter) gj).filterEvaluate(context, null);
      Support.Assert(filterResults != null, "FilterResults cannot be null here");
      if (filterResults.isEmpty()) {
        if (finalList.size() > 1) {
          StructType type = QueryUtils.createStructTypeForRuntimeIterators(finalList);
          return QueryUtils.createStructCollection(context, type);
        } else {
          ObjectType type = ((RuntimeIterator) finalList.iterator().next()).getElementType();
          if (type instanceof StructType) {
            return QueryUtils.createStructCollection(context, (StructTypeImpl) type);
          } else {
            return QueryUtils.createResultCollection(context, type);
          }
        }
      } else {
        results[j] = filterResults;
        grpItrs = (gj instanceof CompositeGroupJunction)
            ? QueryUtils.getDependentItrChainForIndpndntItrs(
                ((CompositeGroupJunction) gj).getIndependentIteratorsOfCJ(), context)
            : context.getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(
                ((AbstractGroupOrRangeJunction) gj).getIndependentIteratorForGroup()[0]);
        itrsForResultFields[j] = new RuntimeIterator[grpItrs.size()];
        Iterator grpItr = grpItrs.iterator();
        int k = 0;
        while (grpItr.hasNext()) {
          tempItr = (RuntimeIterator) grpItr.next();
          itrsForResultFields[j][k++] = tempItr;
          expansionList.remove(tempItr);
        }
        ++j;
      }
    }
    SelectResults resultsSet = null;
    // Asif : Do the Cartesian of the different group junction results.
    CompiledValue iterOperandsToSend = null;
    if (!iterOperands.isEmpty()) {
      // TODO ASIF : Avoid creation of CompiledJunction by providing
      // functionality in AllGroupJunction for evaluating condition
      int size = iterOperands.size();
      CompiledValue[] cv = new CompiledValue[size];
      for (int k = 0; k < size; ++k) {
        cv[k] = (CompiledValue) iterOperands.get(k);
      }
      if (cv.length == 1) {
        iterOperandsToSend = cv[0];
      } else {
        iterOperandsToSend = new CompiledJunction(cv, operator);
      }
    }
    QueryObserver observer = QueryObserverHolder.getInstance();
    observer.beforeCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND(results);
    resultsSet = QueryUtils.cartesian(results, itrsForResultFields, expansionList, finalList,
        context, iterOperandsToSend);
    observer.afterCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND();
    Support.Assert(resultsSet != null, "ResultsSet obtained was NULL in AllGroupJunction");
    return resultsSet;
  }

  /**
   * Evaluates the individual GroupJunctions and CompositeGroupJunctions and expands the individual
   * results so obtained to the query from clause iterator level ( i.e top level iterators). The
   * expanded results so obtained are then merged (union) to get the ORed results.The evaluated
   * result of an AllGroupJunction will always be of the query from clause iterator level (top
   * level) which can be ORed or ANDd with filter evaluable subtree CompiledJunction.
   *
   * @param context ExecutionContext object
   * @return SelectResults object
   */
  private SelectResults evaluateOrJunction(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    // int len = this.abstractGroupOrRangeJunctions.size();
    // Asif : Create an array of SelectResults for each of the GroupJunction
    // For each Group Junction there will be a corresponding array of
    // RuntimeIterators which will map to the fields of the ResultSet obtained
    // from Group Junction
    SelectResults[] grpResults = new SelectResults[1];
    List finalList = context.getCurrentIterators();
    RuntimeIterator[][] itrsForResultFields = new RuntimeIterator[1][];
    CompiledValue gj = null;
    Iterator junctionItr = abstractGroupOrRangeJunctions.iterator();
    List grpItrs = null;
    RuntimeIterator tempItr = null;
    SelectResults intermediateResults = null;
    while (junctionItr.hasNext()) {
      List expansionList = new LinkedList(finalList);
      gj = (CompiledValue) junctionItr.next();
      grpResults[0] = ((Filter) gj).filterEvaluate(context, null);
      grpItrs = (gj instanceof CompositeGroupJunction)
          ? QueryUtils.getDependentItrChainForIndpndntItrs(
              ((CompositeGroupJunction) gj).getIndependentIteratorsOfCJ(), context)
          : context.getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(
              ((AbstractGroupOrRangeJunction) gj).getIndependentIteratorForGroup()[0]);
      itrsForResultFields[0] = new RuntimeIterator[grpItrs.size()];
      Iterator grpItr = grpItrs.iterator();
      int k = 0;
      while (grpItr.hasNext()) {
        tempItr = (RuntimeIterator) grpItr.next();
        itrsForResultFields[0][k++] = tempItr;
        expansionList.remove(tempItr);
      }
      SelectResults expandedResult =
          QueryUtils.cartesian(grpResults, itrsForResultFields, expansionList, finalList, context,
              null/*
                   * Iter oprenad for OR Junction evaluation should be null
                   */);
      intermediateResults = (intermediateResults == null) ? expandedResult
          : QueryUtils.union(expandedResult, intermediateResults, context);
    }
    return intermediateResults;
  }

  List getGroupOperands() {
    // return unmodifiable copy
    return Collections.unmodifiableList(abstractGroupOrRangeJunctions);
  }

  List getIterOperands() {
    // return unmodifiable copy
    return Collections.unmodifiableList(iterOperands);
  }

  @Override
  public int getSizeEstimate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return 1;
  }
}
