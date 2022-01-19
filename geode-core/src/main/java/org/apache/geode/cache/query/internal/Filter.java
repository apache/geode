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

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;

/**
 * Class Description
 *
 * @version $Revision: 1.1 $
 */
public interface Filter {

  /**
   * Evaluates as a filter taking advantage of indexes if appropriate.
   *
   */
  SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException;

  /**
   *
   * Asif : Evaluates as a filter taking advantage of indexes if appropriate. This function has a
   * meaningful implementation only in CompiledComparison & CompiledUndefined . It is unsupported in
   * other classes. The additional parameters which it takes are a boolean which is used to indicate
   * whether the index result set needs to be expanded to the top level or not. The second is a
   * CompiledValue representing the operands which are only iter evaluatable. In case of a
   * GroupJunction, the CompiledValue passed will be null except if a GroupJunction has only one
   * filter evaluatable condition & rest are iter operands. In such cases , the iter operands will
   * be evaluated while expanding/cutting down the index resultset. In case of
   * CompositeGroupJunction, the iter operand may be passed as not null while evaluating the last
   * Composite condition iff the number of GroupJunctions present is zero. If there exists one
   * GroupJunction, then since the GroupJunction gets expanded to the CompositeGroupJunction level ,
   * the iter operand can be evaluated along with the iter operands of the GroupJunction, else the
   * iter operand can get evaluated during cartesian/expansion of GroupJunctions. Ofcourse, the iter
   * operands will get pushed to CompositeGroupJunction level, in the first place , only if a single
   * CompositeGroupJunction gets created. *
   *
   * @param context ExecutionContext object
   * @param iterationLimit SelectResults object representing the intermediate ResultSet. It is
   *        mostly passed as null or the value is ignored except when evaluating a filter
   *        evaluatable composite condition ( equi join condition across the regions) for an AND
   *        junction inside a CompositeGroupJunction
   * @param completeExpansionNeeded A boolean which indicates whether a GroupJunction or a
   *        CompositeGroupJunction needs to be expanded to the query from clause iterator level (
   *        i.e to the top level)
   * @param iterOperands CompiledComparison or CompiledJunction representing the operands which are
   *        iter evaluatable
   * @param indpndntItrs An Array of RuntimeIterators . This is passed as null in case of a where
   *        clause containing single condition. It is used to identify the order of iterators to be
   *        present in the Select Results ( StructBag or ResultBag) so that their is consistency in
   *        the Object Type of the SelectResults which enables union or intersection to occur
   *        correctly. It also helps in identifying the mapping of SelectResults fields and their
   *        RuntimeIterators during the cartesian/expansion of results in the AllGroupJunction. The
   *        Independent For GroupJunction , it is normally a single Iterator representing the
   *        independent iterator for the Group. But when a GroupJunction is part of a
   *        CompositeGroupJunction & there exists only a single GroupJunction in it , the Array will
   *        contain more than one independent iterators for the group, with the independent
   *        iterators being that of CompositeGroupJunction. If the completeExpansion flag is true ,
   *        then this will be null as in that case , the position of iterators in the Results is
   *        decided by the actual order of iterators in the Query from clause .
   * @return Object of type SelectResults representing the Results obtained from evaluation of the
   *         condition
   */
  SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit,
      boolean completeExpansionNeeded, CompiledValue iterOperands, RuntimeIterator[] indpndntItrs,
      boolean isIntersection, boolean conditioningNeeded, boolean evaluateProjection)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException;

  /**
   * This method gets invoked from the filterEvaluate of CompiledJunction and GroupJunction if the
   * boolean isSingleFilter of OrganizedOperands happens to be false
   *
   * @param context ExecutionContext object
   * @param intermediateResults SelectResults Object which will usually be null or will mostly be
   *        ignored
   * @return Object of type SelectResults representing the Results obtained from evaluation of the
   *         condition
   */
  SelectResults auxFilterEvaluate(ExecutionContext context, SelectResults intermediateResults)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException;

  int getSizeEstimate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException;

  boolean isProjectionEvaluationAPossibility(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException;

  /**
   * Only used by single base collection index
   *
   */
  boolean isConditioningNeededForIndex(RuntimeIterator independentIter, ExecutionContext context,
      boolean completeExpnsNeeded)
      throws TypeMismatchException, NameResolutionException;

  /**
   *
   * @return boolean true if this is the better filter as compared to the Filter passed as parameter
   */
  boolean isBetterFilter(Filter comparedTo, ExecutionContext context, int thisSize)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException;

  int getOperator();

  /**
   * This method returns the boolean indicating whether limit can be applied at index level. It
   * works only if it has been ensured that the query is dependent on single iterator and for Group
   * junction so created in case of AND clause, would use exactly one index & rest to be iter
   * evaluated
   *
   * @return true if limit can be applied at index level
   */
  boolean isLimitApplicableAtIndexLevel(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException;

  boolean isOrderByApplicableAtIndexLevel(ExecutionContext context,
      String canonicalizedOrderByClause) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException;
}
