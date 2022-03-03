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

import java.util.List;
import java.util.Set;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.util.internal.GeodeGlossary;

public interface CompiledValue {

  // extra node types: use negative numbers so they don't collide with token types
  int COMPARISON = -1;
  int FUNCTION = -2;
  int JUNCTION = -3;
  int LITERAL = -4;
  int PATH = -5;
  int CONSTRUCTION = -6;
  int GROUPJUNCTION = -7;
  int ALLGROUPJUNCTION = -8;
  int COMPOSITEGROUPJUNCTION = -10;
  int RANGEJUNCTION = -11;
  int NOTEQUALCONDITIONEVALUATOR = -12;
  int SINGLECONDNEVALUATOR = -13;
  int DOUBLECONDNRANGEJUNCTIONEVALUATOR = -14;
  int LIKE = -15;
  int FIELD = -16;
  int GROUP_BY_SELECT = -17;
  int MOD = -18;
  int ADDITION = -19;
  int SUBTRACTION = -20;
  int DIVISION = -21;
  int MULTIPLICATION = -22;
  int INDEX_RESULT_THRESHOLD_DEFAULT = 100;
  String INDX_THRESHOLD_PROP_STR = GeodeGlossary.GEMFIRE_PREFIX + "Query.INDEX_THRESHOLD_SIZE";
  String INDEX_INFO = "index_info";
  int indexThresholdSize =
      Integer.getInteger(INDX_THRESHOLD_PROP_STR, INDEX_RESULT_THRESHOLD_DEFAULT);
  String RESULT_TYPE = "result_type";
  String PROJ_ATTRIB = "projection";
  String ORDERBY_ATTRIB = "orderby";
  @Immutable("O size array cannot be modified")
  IndexInfo[] NO_INDEXES_IDENTIFIER = new IndexInfo[0];
  String RESULT_LIMIT = "limit";
  String CAN_APPLY_LIMIT_AT_INDEX = "can_apply_limit_at_index";
  String CAN_APPLY_ORDER_BY_AT_INDEX = "can_apply_orderby_at_index";
  String PREF_INDEX_COND = "preferred_index_condition";
  String QUERY_INDEX_HINTS = "query_index_hints";

  @Immutable
  CompiledValue MAP_INDEX_ALL_KEYS = new AbstractCompiledValue() {
    @Override
    public void generateCanonicalizedExpression(StringBuilder clauseBuffer,
        ExecutionContext context)
        throws TypeMismatchException, NameResolutionException {
      throw new QueryInvalidException(
          "* cannot be used with index operator. To use as key for map lookup, "
              + "it should be enclosed in ' '");
    }

    @Override
    public Object evaluate(ExecutionContext context) {
      throw new QueryInvalidException(
          "* cannot be used with index operator. To use as key for map lookup, "
              + "it should be enclosed in ' '");
    }

    @Override
    public CompiledValue getReceiver() {
      return super.getReceiver();
    }

    @Override
    public int getType() {
      return OQLLexerTokenTypes.TOK_STAR;
    }

    @Override
    void setTypecast(ObjectType objectType) {
      throw new UnsupportedOperationException("Cannot modify singleton");
    }
  };

  int getType();

  ObjectType getTypecast();

  Object evaluate(ExecutionContext context) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException;

  /**
   * returns null if N/A
   */
  List getPathOnIterator(RuntimeIterator itr, ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException;

  PlanInfo getPlanInfo(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException;

  Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, NameResolutionException;

  boolean isDependentOnIterator(RuntimeIterator itr, ExecutionContext context);

  boolean isDependentOnCurrentScope(ExecutionContext context);

  /**
   * general-purpose visitor (will be used for extracting region path)
   */
  void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws TypeMismatchException, NameResolutionException;

  /**
   * Populates the Set passed with the name of the Region which, if any , will be the bottommost
   * object (CompiledRegion). The default implementation is provided in the AbstractCompiledValue &
   * overridden in the CompiledSelect as it can contain multiple iterators
   */
  void getRegionsInQuery(Set regionNames, Object[] parameters);

  /** Get the CompiledValues that this owns */
  List getChildren();

  void visitNodes(NodeVisitor visitor);

  interface NodeVisitor {
    /** @return true to continue or false to stop */
    boolean visit(CompiledValue node);
  }

  CompiledValue getReceiver();

  default boolean hasIdentifierAtLeafNode() {
    return false;
  }
}
