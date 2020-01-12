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
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.cache.RegionEntry;

public interface IndexProtocol extends Index {
  /**
   * Constants that indicate three situations where we are removing a mapping. Some implementations
   * of index handle some of the cases and not others.
   */
  int OTHER_OP = 0; // not an UPDATE but some other operation
  int BEFORE_UPDATE_OP = 1; // handled when there is no reverse map
  int AFTER_UPDATE_OP = 2; // handled when there is a reverse map
  int REMOVE_DUE_TO_GII_TOMBSTONE_CLEANUP = 3;
  int CLEAN_UP_THREAD_LOCALS = 4;

  boolean addIndexMapping(RegionEntry entry) throws IMQException;

  boolean addAllIndexMappings(Collection<RegionEntry> c) throws IMQException;

  /**
   * @param opCode one of OTHER_OP, BEFORE_UPDATE_OP, AFTER_UPDATE_OP.
   */
  boolean removeIndexMapping(RegionEntry entry, int opCode) throws IMQException;

  boolean removeAllIndexMappings(Collection<RegionEntry> c) throws IMQException;

  boolean clear() throws QueryException;

  /**
   * This method gets invoked only if the where clause contains multiple 'NOT EQUAL' conditions only
   * , for a given index expression ( i.e something like a != 5 and a != 7 ). This will be invoked
   * only from RangeJunction.NotEqualConditionEvaluator
   *
   * @param results The Collection object used for fetching the index results
   * @param keysToRemove Set containing Index key which should not be included in the results being
   *        fetched
   * @param context ExecutionContext object
   */
  void query(Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException;


  /**
   * This can be invoked only from RangeJunction.SingleCondnEvaluator
   *
   * @param key Index Key Object for which the Index results need to be fetched
   * @param operator Integer specifying the criteria relative to the key , which defines the index
   *        resultset
   * @param results The Collection object used for fetching the index results
   * @param keysToRemove Set containing the index keys whose Index results should not included in
   *        the index results being fetched. In case of 'NOT EQUAL' operator, the key as well as the
   *        keysToRemove Set ( if it is not null & not empty) containing 'NOT EQUAL' keys mean that
   *        both should not be present in the index results being fetched ( Fetched results should
   *        not contain index results corresponding to key + keys of keysToRemove set )
   * @param context ExecutionContext object
   */
  void query(Object key, int operator, Collection results, Set keysToRemove,
      ExecutionContext context) throws TypeMismatchException, FunctionDomainException,
      NameResolutionException, QueryInvocationTargetException;

  /**
   *
   * @param key Index Key Object for which the Index results need to be fetched
   * @param operator Integer specifying the criteria relative to the key , which defines the index
   *        resultset
   * @param results The Collection object used for fetching the index results
   * @param context ExecutionContext object
   */
  void query(Object key, int operator, Collection results, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException;

  void query(Object key, int operator, Collection results, CompiledValue iterOp,
      RuntimeIterator indpndntItr, ExecutionContext context, List projAttrib,
      SelectResults intermediateResults, boolean isIntersection) throws TypeMismatchException,
      FunctionDomainException, NameResolutionException, QueryInvocationTargetException;

  /**
   * This will be queried from RangeJunction.DoubleCondnRangeJunctionEvaluator
   *
   * @param lowerBoundKey Object representing the lower Bound Key
   * @param lowerBoundOperator integer identifying the lower bound ( > or >= )
   * @param upperBoundKey Object representing the Upper Bound Key
   * @param upperBoundOperator integer identifying the upper bound ( < or <= )
   * @param results The Collection object used for fetching the index results
   * @param keysToRemove Set containing Index key which should not be included in the results being
   *        fetched
   * @param context ExecutionContext object
   */
  void query(Object lowerBoundKey, int lowerBoundOperator, Object upperBoundKey,
      int upperBoundOperator, Collection results, Set keysToRemove, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException;

  /**
   * Asif: Gets the data for an equi join condition across the region. The function iterates over
   * this Range Index's keys & also runs an inner iteration for the Range Index passed as parameter.
   * If the two keys match the resultant data of both the indexes is collected.
   *
   * @param index RangeIndex object for the other operand
   * @param context TODO
   * @return A List object whose elements are a two dimensional Object Array. Each element of the
   *         List represents a value (key of Range Index) which satisfies the equi join condition.
   *         The Object array will have two rows. Each row ( one dimensional Object Array ) will
   *         contain objects or type Struct or Object.
   */
  List queryEquijoinCondition(IndexProtocol index, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException;

  void markValid(boolean b);

  void initializeIndex(boolean loadEntries) throws IMQException;

  void destroy();

  boolean containsEntry(RegionEntry entry);

  /**
   * Asif : This function returns the canonicalized definitions of the from clauses used in Index
   * creation
   *
   * @return String array containing canonicalized definitions
   */
  String[] getCanonicalizedIteratorDefinitions();

  /**
   * Asif: Object type of the data ( tuple ) contained in the Index Results . It will be either of
   * type ObjectType or StructType
   *
   */
  ObjectType getResultSetType();

  int getSizeEstimate(Object key, int op, int matchLevel) throws TypeMismatchException;

  boolean isMatchingWithIndexExpression(CompiledValue condnExpr, String condnExprStr,
      ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException;
}
