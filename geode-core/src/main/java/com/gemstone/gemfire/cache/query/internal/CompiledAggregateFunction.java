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

import com.gemstone.gemfire.cache.query.AmbiguousNameException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.aggregate.AvgBucketNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.AvgDistinct;
import com.gemstone.gemfire.cache.query.internal.aggregate.AvgDistinctPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.AvgPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.Count;
import com.gemstone.gemfire.cache.query.internal.aggregate.CountDistinct;
import com.gemstone.gemfire.cache.query.internal.aggregate.CountDistinctPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.SumDistinctPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.CountPRQueryNode;
import com.gemstone.gemfire.cache.query.internal.aggregate.DistinctAggregator;
import com.gemstone.gemfire.cache.query.internal.aggregate.MaxMin;
import com.gemstone.gemfire.cache.query.internal.aggregate.Avg;
import com.gemstone.gemfire.cache.query.internal.aggregate.Sum;
import com.gemstone.gemfire.cache.query.internal.aggregate.SumDistinct;
import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;

/**
 * 
 *
 */
public class CompiledAggregateFunction extends AbstractCompiledValue {

  private final CompiledValue expr;
  private final int aggFuncType;
  private final boolean distinctOnly;

  public CompiledAggregateFunction(CompiledValue expr, int aggFunc) {
    this(expr, aggFunc, false);
  }

  public CompiledAggregateFunction(CompiledValue expr, int aggFunc,
      boolean distinctOnly) {
    this.expr = expr;
    this.aggFuncType = aggFunc;
    this.distinctOnly = distinctOnly;
  }

  @Override
  public int getType() {

    return AGG_FUNC;
  }

  @Override
  public Object evaluate(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    boolean isPRQueryNode = context.getIsPRQueryNode();
    boolean isBucketNode = context.getBucketList() != null;
    switch (this.aggFuncType) {

    case OQLLexerTokenTypes.SUM:
      if (isPRQueryNode) {
        return this.distinctOnly ? new SumDistinctPRQueryNode() : new Sum();
      } else {
        return this.distinctOnly ? (isBucketNode ? new DistinctAggregator()
            : new SumDistinct()) : new Sum();
      }

    case OQLLexerTokenTypes.MAX:
      return new MaxMin(true);

    case OQLLexerTokenTypes.MIN:
      return new MaxMin(false);

    case OQLLexerTokenTypes.AVG:
      if (isPRQueryNode) {
        return this.distinctOnly ? new AvgDistinctPRQueryNode()
            : new AvgPRQueryNode();
      } else {
        return this.distinctOnly ? (isBucketNode ? new DistinctAggregator()
            : new AvgDistinct()) : (isBucketNode ? new AvgBucketNode()
            : new Avg());
      }

    case OQLLexerTokenTypes.COUNT:
      if (isPRQueryNode) {
        return this.distinctOnly ? new CountDistinctPRQueryNode()
            : new CountPRQueryNode();
      } else {
        return this.distinctOnly ? (isBucketNode ? new DistinctAggregator()
            : new CountDistinct()) : new Count();
      }

    default:
      throw new UnsupportedOperationException(
          "Aggregate function not implemented");

    }

  }

  private String getStringRep() {
    switch (this.aggFuncType) {

    case OQLLexerTokenTypes.SUM:
      return "sum";

    case OQLLexerTokenTypes.MAX:
      return "max";

    case OQLLexerTokenTypes.MIN:
      return "min";

    case OQLLexerTokenTypes.AVG:
      return "avg";
    case OQLLexerTokenTypes.COUNT:
      return "count";
    default:
      throw new UnsupportedOperationException(
          "Aggregate function not implemented");

    }
  }

  public int getFunctionType() {
    return this.aggFuncType;
  }

  public CompiledValue getParameter() {
    return this.expr;
  }

  public ObjectType getObjectType() {
    switch (this.aggFuncType) {

    case OQLLexerTokenTypes.SUM:
    case OQLLexerTokenTypes.MAX:
    case OQLLexerTokenTypes.MIN:
    case OQLLexerTokenTypes.AVG:
      return new ObjectTypeImpl(Number.class);

    case OQLLexerTokenTypes.COUNT:
      return new ObjectTypeImpl(Integer.class);

    default:
      throw new UnsupportedOperationException(
          "Aggregate function not implemented");

    }
  }

  @Override
  public void generateCanonicalizedExpression(StringBuffer clauseBuffer,
      ExecutionContext context) throws AmbiguousNameException,
      TypeMismatchException, NameResolutionException {
    clauseBuffer.insert(0, ')');
    if (this.expr != null) {
      this.expr.generateCanonicalizedExpression(clauseBuffer, context);
    } else {
      clauseBuffer.insert(0, '*');
    }
    if (this.distinctOnly) {
      clauseBuffer.insert(0, "distinct ");
    }
    clauseBuffer.insert(0, '(');
    clauseBuffer.insert(0, getStringRep());
  }
}
