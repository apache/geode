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

import java.util.Collection;
import java.util.Set;

import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;

/**
 * This class provides 'do-nothing' implementations of all of the methods of interface
 * QueryObserver. See the documentation for class QueryObserverHolder for details.
 *
 * @version $Revision: 1.2 $
 */
public class QueryObserverAdapter implements QueryObserver {

  /**
   * Called when a query begins, after any mutex locks have been acquired, but before any other
   * processing has taken place.
   */
  @Override
  public void startQuery(Query query) {}

  /**
   * Called immediately before the query expression is evaluated. (After it was compiled)
   *
   * @param expression The query Expression
   * @param context The execution context that will be used to evaluate the expression.
   */
  @Override
  public void beforeQueryEvaluation(CompiledValue expression, ExecutionContext context) {}

  /**
   * Called once right before iteration of a 'select' statement begins.
   *
   * @param collection The collection being iterated over
   * @param whereClause The 'where' clause of the select statement
   */
  @Override
  public void startIteration(Collection collection, CompiledValue whereClause) {}

  /**
   * Called once before the evaluation of an expression for a single iteration of a 'select'
   * statment. The RuntimeIterator that was passed to startIteration should already be referencing
   * the current iteration value.
   *
   * @param executer the object performing the iteration.
   * @param currentObject The current object in the iteration.
   */
  @Override
  public void beforeIterationEvaluation(CompiledValue executer, Object currentObject) {}

  /**
   * Called once after the evaluation of an expression for a single iteration of a 'select'
   * statment.
   *
   * @param result The result of evaluating the where clause. Should be either a Boolean, NULL, or
   *        UNDEFINED. If evaluating the where clause threw an exception, should be NULL.
   */
  @Override
  public void afterIterationEvaluation(Object result) {}

  /**
   * Called once right before iteration of a 'select' statement ends.
   *
   * @param results The set of results returned so far by the iteration.
   */
  @Override
  public void endIteration(SelectResults results) {}

  /**
   * Called once right before the query subsystem has requested that the indexing subsystem attempt
   * an index lookup.
   *
   * @param index The index being used for the lookup
   * @param oper The operation being attemped on the index. AbstractIndex
   */
  @Override
  public void beforeIndexLookup(Index index, int oper, Object key) {}

  /**
   * Called once right after the query subsystem has requested that the indexing subsystem attempt
   * an index lookup.
   *
   * @param results The results of the index lookup, or null if an exception was thrown.
   */
  @Override
  public void afterIndexLookup(Collection results) {}

  /**
   * Called immediately after the query expression is evaluated.
   *
   * @param result The results of the evaluation, or null if an exception was thrown.
   */
  @Override
  public void afterQueryEvaluation(Object result) {}

  /**
   * Called when a query ends, after all processing has taken place but before any mutex locks have
   * been released.
   */
  @Override
  public void endQuery() {}

  /**
   * Asif : Called just before IndexManager executes the function rerunIndexCreationQuery. After
   * this function gets invoked, IndexManager will iterate over all the indexes of the region making
   * the data maps null & re running the index creation query on the region. The method of Index
   * Manager gets executed from the clear function of the Region
   */
  @Override
  public void beforeRerunningIndexCreationQuery() {}

  /**
   * Asif : Invoked just before the cartesian of the SelectResults obtained from GroupJunctions, is
   * done so as to expand the final resultset to the level of iterators in the query. This function
   * will get invoked from AllGroupJunction during the evaluation of AND condition
   *
   * @param grpResults An array of intermediate SelectResults obtained by evaluation of
   *        GroupJunctions
   *
   */
  @Override
  public void beforeCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND(
      Collection[] grpResults) {}

  /**
   * Asif : Invoked just after the cartesian of the SelectResults obtained from GroupJunctions, is
   * done so as to expand the final resultset to the level of iterators in the query. This function
   * will get invoked from AllGroupJunction during the evaluation of AND condition
   *
   *
   */
  @Override
  public void afterCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND() {}

  /**
   * Asif :Invoked just before the cartesian of the SelectResults obtained from GroupJunctions, is
   * done so as to expand the final resultset to the level of Iterators for the
   * CompositeGroupJunction. This function will get invoked from CompositeGroupJunction during the
   * evaluation of AND condition and only if there exists more than one GroupJunction objects in the
   * CompositeGroupJunction
   *
   * @param grpResults An array of intermediate SelectResults obtained by evaluation of
   *        GroupJunctions
   *
   */
  @Override
  public void beforeCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND(
      Collection[] grpResults) {}

  /**
   * Asif :Invoked just before the cartesian of the SelectResults obtained from GroupJunctions, is
   * done so as to expand the final resultset to the level of Iterators for the
   * CompositeGroupJunction. This function will get invoked from CompositeGroupJunction during the
   * evaluation of AND condition and only if there exists more than one GroupJunction objects in the
   * CompositeGroupJunction
   */
  @Override
  public void afterCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND() {}

  /**
   * Asif : Invoked just before doing expansion or cutdown of results obtained from index usage for
   * a filter evaluatable , non composite condition i.e for a single base collection.
   *
   * @param index The index used by the filter evaluatable condition
   * @param initialResult The raw resultset obtained for the condition
   *
   */
  @Override
  public void beforeCutDownAndExpansionOfSingleIndexResult(Index index, Collection initialResult) {}

  /**
   * Asif :Invoked just after doing expansion or cutdown of results obtained from index usage for a
   * filter evaluatable , non composite condition i.e for a single base collection.
   *
   * @param finalResult The final conditioned resultset obtained from use of index on the condition
   *
   */
  @Override
  public void afterCutDownAndExpansionOfSingleIndexResult(Collection finalResult) {}

  /**
   * Asif :Invoked before merging the results of the two indexes, identified for evaluation of a
   * filter evalua table equi join condition across two regions ( Composite Condition). During this
   * merging the appropriate expansion & cutdown of the individual index results to the final query
   * level or CompositeGroupJunction level also occurs. Also for AND conditions, the cartesian with
   * the intermediate resultset will also occur.
   *
   * @param index1 Range Index identified for one of the operand, to be used in equi join condition
   * @param index2 Range Index identified for the other operand , to be used in equi join condition
   * @param initialResult A list in which each element contains a TWO dimensional Object array
   *        containing two rows. First row contains Index Resultset obtained from 1st index & second
   *        row contains results obtained from second index. Each element of the list basically
   *        represents an equi join satisfying value ( i.e something like 1 = 1, 2 = 2 , 3 = 3 )
   *        etc. Since multiple objects of a region can have same value , so there exits rows of
   *        Object Array for each Index.
   *
   */
  @Override
  public void beforeMergeJoinOfDoubleIndexResults(Index index1, Index index2,
      Collection initialResult) {}

  /**
   * Asif :Invoked after merging the results of the two indexes, identified for evaluation of a
   * filter evalua table equi join condition across two regions ( Composite Condition). During this
   * merging the appropriate expansion & cutdown of the individual index results to the final query
   * level or CompositeGroupJunction level also occurs. Also for AND conditions, the cartesian with
   * the intermediate resultset will also occur.
   *
   * @param finalResult A SelectResults object created after expansion cutdown of two index results
   *        for a filter evaluatable CompositeCondition ( which may have been cartesianed with the
   *        intermediate Results. The Results obtained may or may not be to the Top Level/
   *        CompositeGroupJunction level. The number & positions of iterators present will depend on
   *        whether it is invoked for an OR Junction or if Complete Expansion is true or not & the
   *        stage of evaluation it is in for an AND junction.
   *
   */
  @Override
  public void afterMergeJoinOfDoubleIndexResults(Collection finalResult) {}

  /**
   * Asif :Invoked before intermediate resultset is iter evaluated to be used with the usable index
   * of the operand for filter evaluatable Composite CompiledComparison for an AND type
   * CompositeGroupJunction containing more than one filter evaluatable composite condition. This
   * invocation implies that the indpendent group of iterators of one of the operands of the
   * condition is also present in the intermediate resultset which means that index for that operand
   * canot be used. Thus the intermediate Resultset is iterated & for each tuple , the values set in
   * the respective RuntimeIterators. The operand without usable index is evaluated & the Index on
   * the other operand is used to get resultset for that value. Thus for each tuple we query the
   * index on other operand .
   *
   * @param usedIndex : The Index which will be used to obtain the ResultSet
   * @param unusedIndex : The Index which does not get used as the relevant iterators are present in
   *        the intermediate resultset
   *
   *
   */
  @Override
  public void beforeIterJoinOfSingleIndexResults(Index usedIndex, Index unusedIndex) {}

  /**
   * Asif :Invoked after the intermediate resultset is iter evaluated to be used with the usable
   * index of the operand for filter evaluatable Composite CompiledComparison for an AND type
   * CompositeGroupJunction containing more than one filter evaluatable composite condition. This
   * invocation implies that the indpendent group of iterators of one of the operands of the
   * condition is also present in the intermediate resultset which means that index for that operand
   * canot be used. Thus the intermediate Resultset is iterated & for each tuple , the values set in
   * the respective RuntimeIterators. The operand without usable index is evaluated & the Index on
   * the other operand is used to get resultset for that value. Thus for each tuple we query the
   * index on other operand .
   *
   * @param finalResult : A SelectResults object created after expansion / cutdown of mergred
   *        results where one operand uses Index Results & other operand though has an Index
   *        available but cannot be used. The value of that operand is calculated by iteration of
   *        intermediate resultset for each tuple. This function gets invoked only for an AND
   *        junction. The resultset may or may not be up to the level of top level query iterators
   *        or CompositeGroupJunction iterators level.
   *
   */
  @Override
  public void afterIterJoinOfSingleIndexResults(Collection finalResult) {}

  /**
   * @see org.apache.geode.cache.query.internal.QueryObserver#beforeIndexLookup(org.apache.geode.cache.query.Index,
   *      int, java.lang.Object, int, java.lang.Object, java.util.Set)
   */
  @Override
  public void beforeIndexLookup(Index index, int lowerBoundOperator, Object lowerBoundKey,
      int upperBoundOperator, Object upperBoundKey, Set NotEqualKeys) {}

  @Override
  public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied) {

  }

  @Override
  public void invokedQueryUtilsIntersection(SelectResults sr1, SelectResults sr2) {}

  @Override
  public void invokedQueryUtilsUnion(SelectResults sr1, SelectResults sr2) {}

  @Override
  public void limitAppliedAtIndexLevel(Index index, int limit, Collection indexResult) {

  }

  @Override
  public void orderByColumnsEqual() {

  }
}
