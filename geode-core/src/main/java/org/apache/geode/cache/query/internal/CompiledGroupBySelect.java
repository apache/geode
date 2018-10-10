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

import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.query.Aggregator;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.internal.utils.PDXUtils;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;

public class CompiledGroupBySelect extends CompiledSelect {

  private final BitSet aggregateColsPos;
  private final CompiledAggregateFunction[] aggregateFunctions;
  private final boolean isDistinct;
  private final List<CompiledSortCriterion> originalOrderByClause;
  private final CompiledValue limit;

  @Override
  public int getType() {
    return GROUP_BY_SELECT;
  }

  public CompiledGroupBySelect(boolean distinct, boolean count, CompiledValue whereClause,
      List iterators, List projAttrs, List<CompiledSortCriterion> orderByAttrs, CompiledValue limit,
      List<String> hints, List<CompiledValue> groupByClause,
      LinkedHashMap<Integer, CompiledAggregateFunction> aggMap) {
    super(false, false, whereClause, iterators, projAttrs, null, null, hints, groupByClause);
    this.aggregateFunctions = new CompiledAggregateFunction[aggMap != null ? aggMap.size() : 0];
    this.aggregateColsPos = new BitSet(this.projAttrs.size());
    if (aggMap != null) {
      int i = 0;
      for (Map.Entry<Integer, CompiledAggregateFunction> entry : aggMap.entrySet()) {
        this.aggregateColsPos.set(entry.getKey());
        this.aggregateFunctions[i++] = entry.getValue();
      }
    }
    this.originalOrderByClause = orderByAttrs;
    this.isDistinct = distinct;
    this.limit = limit;
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    if (!this.transformationDone) {
      replaceAggregateFunctionInProjection();
    }
    return super.computeDependencies(context);

  }

  private void replaceAggregateFunctionInProjection() {
    // Extract the parameter compiledValues out of aggregate functions &
    // modify the projection
    // attributes to have that instead. Empty out the groupByList
    // Create orderby attribute out of group by

    int bitStart = 0;
    for (CompiledAggregateFunction aggFunc : this.aggregateFunctions) {
      int index = this.aggregateColsPos.nextSetBit(bitStart);
      bitStart = index + 1;
      CompiledValue param = aggFunc.getParameter();
      if (param == null && aggFunc.getFunctionType() == OQLLexerTokenTypes.COUNT) {
        // * case of *, substitue a dummy parameter of compiled literal = 0 to
        // satisfy the code
        param = new CompiledLiteral(0);

      } else if (param == null) {
        throw new QueryInvalidException("aggregate function passed invalid parameter");
      }
      Object[] projAtt = (Object[]) this.projAttrs.get(index);
      projAtt[1] = param;
    }
  }

  private void revertAggregateFunctionInProjection() {
    // Extract the parameter compiledValues out of aggregate functions &
    // modify the projection
    // attributes to have that instead. Empty out the groupByList
    // Create orderby attribute out of group by

    int bitStart = 0;
    for (CompiledAggregateFunction aggFunc : this.aggregateFunctions) {
      int index = this.aggregateColsPos.nextSetBit(bitStart);
      bitStart = index + 1;
      Object[] projAtt = (Object[]) this.projAttrs.get(index);
      projAtt[1] = aggFunc;
    }
  }

  @Override
  protected void doTreeTransformation(ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    if (!this.transformationDone) {
      checkAllProjectedFieldsInGroupBy(context);
      this.cachedElementTypeForOrderBy = prepareResultType(context);
      if (this.groupBy != null && !this.groupBy.isEmpty()) {
        this.modifyGroupByToOrderBy(false, context);
      }
      if (this.originalOrderByClause != null) {
        this.mapOriginalOrderByColumns(context);
      }
    }
    this.transformationDone = true;
  }

  private void mapOriginalOrderByColumns(ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    this.revertAggregateFunctionInProjection();
    Iterator<CompiledSortCriterion> iter = this.originalOrderByClause.iterator();
    while (iter.hasNext()) {
      CompiledSortCriterion csc = iter.next();
      if (!csc.mapExpressionToProjectionField(this.projAttrs, context)) {
        throw new QueryInvalidException(
            "Query contains atleast one order by field which is not present in projected fields.");
      }
    }
    this.replaceAggregateFunctionInProjection();

  }

  @Override
  public SelectResults evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    SelectResults sr = super.evaluate(context);
    return this.applyAggregateAndGroupBy(sr, context);

  }

  public SelectResults applyAggregateAndGroupBy(SelectResults baseResults, ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    ObjectType elementType = baseResults.getCollectionType().getElementType();
    boolean isStruct = elementType != null && elementType.isStructType();
    boolean isBucketNodes = context.getBucketList() != null;
    boolean createOrderedResultSet = isBucketNodes && this.orderByAttrs != null;
    boolean[] objectChangedMarker = new boolean[] {false};
    int limitValue = evaluateLimitValue(context, limit);
    SelectResults newResults =
        createResultSet(context, elementType, isStruct, createOrderedResultSet);
    Aggregator[] aggregators = new Aggregator[this.aggregateFunctions.length];
    refreshAggregators(aggregators, context);
    if (this.orderByAttrs != null) {
      applyGroupBy(baseResults, context, isStruct, newResults, aggregators, !createOrderedResultSet,
          objectChangedMarker, limitValue);
    } else {
      Iterator iter = baseResults.iterator();
      Object current = null;
      boolean unterminated = iter.hasNext();
      while (iter.hasNext()) {
        current = iter.next();
        accumulate(isStruct, aggregators, current, objectChangedMarker);
      }
      if (unterminated) {
        this.terminateAndAddToResults(isStruct, newResults, aggregators, current, context,
            !createOrderedResultSet, limitValue);
      }
    }

    return newResults;
  }

  private SelectResults createResultSet(ExecutionContext context, ObjectType elementType,
      boolean isStruct, boolean createOrderedResults) {
    elementType = createNewElementType(elementType, isStruct);
    SelectResults newResults;

    // boolean isBucketNodes = context.getBucketList() != null;
    boolean isPrQueryNode = context.getIsPRQueryNode();
    // If it is bucket nodes query, we need to return ordered data
    if (isStruct) {
      if (createOrderedResults) {
        newResults = new SortedResultsBag((StructTypeImpl) elementType, true);
      } else {
        if (this.originalOrderByClause != null) {
          Comparator comparator =
              new OrderByComparator(this.originalOrderByClause, elementType, context);
          newResults = new SortedStructBag(comparator, (StructType) elementType,
              !this.originalOrderByClause.get(0).getCriterion());
        } else {
          newResults =
              QueryUtils.createStructCollection(this.isDistinct, (StructType) elementType, context);
        }
      }
    } else {
      if (createOrderedResults) {
        newResults = new SortedResultsBag(elementType, true);
      } else {
        if (this.originalOrderByClause != null) {
          Comparator comparator =
              new OrderByComparator(this.originalOrderByClause, elementType, context);
          newResults = new SortedResultsBag(comparator, elementType,
              !this.originalOrderByClause.get(0).getCriterion());
        } else {
          newResults = QueryUtils.createResultCollection(this.isDistinct, elementType, context);
        }

      }

    }
    return newResults;
  }

  private ObjectType createNewElementType(ObjectType elementType, boolean isStruct) {
    if (isStruct) {
      StructType oldType = (StructType) elementType;
      if (this.aggregateFunctions.length > 0) {
        ObjectType[] oldFieldTypes = oldType.getFieldTypes();
        ObjectType[] newFieldTypes = new ObjectType[oldFieldTypes.length];
        int i = 0;
        int aggFuncIndex = 0;
        for (ObjectType oldFieldType : oldFieldTypes) {
          if (this.aggregateColsPos.get(i)) {
            newFieldTypes[i] = this.aggregateFunctions[aggFuncIndex++].getObjectType();
          } else {
            newFieldTypes[i] = oldFieldType;
          }
          ++i;
        }
        return new StructTypeImpl(oldType.getFieldNames(), newFieldTypes);
      } else {
        return oldType;
      }
    } else {
      return this.aggregateFunctions.length > 0 ? this.aggregateFunctions[0].getObjectType()
          : elementType;
    }
  }

  private void applyGroupBy(SelectResults baseResults, ExecutionContext context, boolean isStruct,
      SelectResults newResults, Aggregator[] aggregators, boolean isStructFields,
      boolean[] objectChangedMarker, int limitValue) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    Iterator iter = baseResults.iterator();
    Object[] orderByTupleHolderCurrent = null;
    Object[] orderByTupleHolderPrev = null;
    Object orderByCurrent = null;
    Object orderByPrev = null;

    boolean isSingleOrderBy = this.orderByAttrs.size() <= 1;
    if (!isSingleOrderBy) {
      orderByTupleHolderPrev = new Object[orderByAttrs.size()];
      orderByTupleHolderCurrent = new Object[orderByAttrs.size()];
    }
    boolean isFirst = true;
    Object prev = null;
    boolean unterminated = false;
    boolean keepAdding = true;
    while (iter.hasNext() && keepAdding) {
      Object current = iter.next();
      if (isSingleOrderBy) {
        orderByCurrent = this.getOrderByEvaluatedTuple(context, isSingleOrderBy, null,
            isStruct ? ((Struct) current).getFieldValues() : current, objectChangedMarker);
      } else {
        orderByTupleHolderCurrent = (Object[]) this.getOrderByEvaluatedTuple(context,
            isSingleOrderBy, orderByTupleHolderCurrent,
            isStruct ? ((Struct) current).getFieldValues() : current, objectChangedMarker);
      }
      if (isFirst || areOrderByTupleEqual(isSingleOrderBy, orderByPrev, orderByCurrent,
          orderByTupleHolderPrev, orderByTupleHolderCurrent)) {
        accumulate(isStruct, aggregators, current, objectChangedMarker);
        unterminated = true;
        isFirst = false;
      } else {
        keepAdding = terminateAndAddToResults(isStruct, newResults, aggregators, prev, context,
            isStructFields, limitValue);
        this.accumulate(isStruct, aggregators, current, objectChangedMarker);
        unterminated = true;
      }
      // swap the holder arrays
      Object[] temp = orderByTupleHolderCurrent;
      orderByTupleHolderCurrent = orderByTupleHolderPrev;
      orderByTupleHolderPrev = temp;
      orderByPrev = orderByCurrent;
      prev = current;
    }
    if (unterminated && keepAdding) {
      this.terminateAndAddToResults(isStruct, newResults, aggregators, prev, context,
          isStructFields, limitValue);
    }

    if (this.originalOrderByClause != null && limitValue > 0
        && (context.getIsPRQueryNode() || context.getBucketList() == null)) {
      ((Bag) newResults).applyLimit(limitValue);
    }
  }

  private boolean terminateAndAddToResults(boolean isStruct, SelectResults newResults,
      Aggregator[] aggregators, Object prev, ExecutionContext context, boolean isStrucFields,
      int limitValue) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    Object[] newRowArray = isStruct ? copyStruct((Struct) prev) : null;
    Object newObject = null;
    int bitstart = 0;
    if (limitValue == 0) {
      return false;
    }

    for (Aggregator aggregator : aggregators) {
      if (isStruct) {
        int pos = this.aggregateColsPos.nextSetBit(bitstart);
        bitstart = pos + 1;
        Object scalarResult = aggregator.terminate();
        newRowArray[pos] = scalarResult;
      } else {
        newObject = aggregator.terminate();
      }
    }

    if (isStruct) {
      if (isStrucFields) {
        ((StructFields) newResults).addFieldValues(newRowArray);
      } else {
        newResults
            .add(new StructImpl((StructTypeImpl) ((Struct) prev).getStructType(), newRowArray));
      }
    } else {
      newResults.add(newObject);
    }
    boolean keepAdding = true;
    if (this.originalOrderByClause == null && limitValue > 0
        && (context.getIsPRQueryNode() || context.getBucketList() == null)
        && newResults.size() == limitValue) {
      keepAdding = false;
    }
    // rfresh the aggregators
    refreshAggregators(aggregators, context);
    return keepAdding;
  }

  private void refreshAggregators(Aggregator[] aggregators, ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    int i = 0;
    for (CompiledAggregateFunction aggFunc : this.aggregateFunctions) {
      Aggregator agg = (Aggregator) aggFunc.evaluate(context);
      aggregators[i++] = agg;
    }
  }

  private Object[] copyStruct(Struct struct) {
    Object[] prevValues = struct.getFieldValues();
    Object[] newRow = new Object[prevValues.length];
    System.arraycopy(prevValues, 0, newRow, 0, prevValues.length);
    return newRow;
  }

  private void accumulate(boolean isStruct, Aggregator[] aggregators, Object current,
      boolean[] objectChangedMarker) {
    int bitstart = 0;
    for (Aggregator aggregator : aggregators) {
      if (isStruct) {
        int pos = this.aggregateColsPos.nextSetBit(bitstart);
        bitstart = pos + 1;
        Struct struct = (Struct) current;
        Object scalar = PDXUtils.convertPDX(struct.getFieldValues()[pos], false, true, true, true,
            objectChangedMarker, isStruct);

        aggregator.accumulate(scalar);
      } else {
        current =
            PDXUtils.convertPDX(current, false, true, true, true, objectChangedMarker, isStruct);
        aggregator.accumulate(current);
      }
    }
  }

  private boolean areOrderByTupleEqual(boolean isSingleOrderBy, Object prev, Object current,
      Object[] prevHolder, Object[] currentHolder) {
    if (isSingleOrderBy) {
      if (prev == null && current == null) {
        return true;
      } else if (prev != null) {
        return prev.equals(current);
      } else {
        return current.equals(prev);
      }
    } else {
      return Arrays.equals(prevHolder, currentHolder);
    }

  }

  private Object getOrderByEvaluatedTuple(ExecutionContext context, boolean isOrderByTupleSingle,
      Object[] holder, Object data, boolean[] objectChangedMarker) {
    if (isOrderByTupleSingle) {
      return PDXUtils.convertPDX(this.orderByAttrs.get(0).evaluate(data, context), false, true,
          true, true, objectChangedMarker, false);
    } else {
      int i = 0;
      for (CompiledSortCriterion csc : this.orderByAttrs) {
        holder[i++] = PDXUtils.convertPDX(csc.evaluate(data, context), false, true, true, true,
            objectChangedMarker, false);
      }
      return holder;
    }
  }

  @Override
  public boolean isGroupBy() {
    return true;
  }

  private void checkAllProjectedFieldsInGroupBy(ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    int index = 0;
    for (Object o : this.projAttrs) {
      Object[] projElem = (Object[]) o;
      // if the projection is aggregate expression skip validating
      if (!this.aggregateColsPos.get(index)) {
        if (!checkProjectionInGroupBy(projElem, context)) {
          throw new QueryInvalidException(
              "Query contains projected column not present in group by clause");
        }
      }
      ++index;
    }

    // check if all the group by fields are present in projected columns
    if (this.groupBy != null) {
      int numGroupCols = this.groupBy != null ? this.groupBy.size() : 0;
      int numColsInProj = this.projAttrs.size();
      numColsInProj -= this.aggregateFunctions.length;
      if (numGroupCols != numColsInProj) {
        throw new QueryInvalidException(
            "Query contains group by columns not present in projected fields");
      }
    }
  }

  private boolean checkProjectionInGroupBy(Object[] projElem, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    boolean found = false;
    StringBuilder projAttribBuffer = new StringBuilder();
    CompiledValue cvProj = (CompiledValue) TypeUtils.checkCast(projElem[1], CompiledValue.class);
    cvProj.generateCanonicalizedExpression(projAttribBuffer, context);
    String projAttribStr = projAttribBuffer.toString();
    if (this.groupBy != null) {
      for (CompiledValue grpBy : this.groupBy) {
        if (grpBy.getType() == OQLLexerTokenTypes.Identifier) {
          if (projElem[0] != null && projElem[0].equals(((CompiledID) grpBy).getId())) {
            found = true;
            break;
          }
        }

        // the grpup by expr is not an alias check for path
        StringBuilder groupByExprBuffer = new StringBuilder();
        grpBy.generateCanonicalizedExpression(groupByExprBuffer, context);
        final String grpByExprStr = groupByExprBuffer.toString();

        if (projAttribStr.equals(grpByExprStr)) {

          found = true;
          break;
        }
      }
    }
    return found;
  }

}
