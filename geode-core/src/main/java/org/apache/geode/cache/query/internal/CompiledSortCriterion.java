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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.ObjectType;

/**
 * This class represents a compiled form of sort criterian present in order by clause
 */
public class CompiledSortCriterion extends AbstractCompiledValue {

  // criterion true indicates descending order
  private boolean criterion = false;

  private CompiledValue expr = null;

  int columnIndex = -1;

  private CompiledValue originalCorrectedExpression = null;

  @Override
  public List getChildren() {
    return Collections.singletonList(originalCorrectedExpression);
  }

  @Override
  public int getType() {
    return SORT_CRITERION;
  }

  /**
   * evaluates sort criteria in order by clause
   */
  public Object evaluate(Object data, ExecutionContext context) {
    Object value;
    if (columnIndex > 0) {
      value = ((Object[]) data)[columnIndex];
    } else if (columnIndex == 0) {
      if (data instanceof Object[]) {
        value = ((Object[]) data)[columnIndex];
      } else {
        value = data;
      }
    } else {
      throw new IllegalStateException(" Order By Column attribute unmapped");
    }
    context.setCurrentProjectionField(value);
    try {
      return expr.evaluate(context);
    } catch (Exception e) {
      // TODO: never throw an anonymous inner class
      throw new CacheException(e) {};
    }
  }

  CompiledSortCriterion(boolean criterion, CompiledValue cv) {
    expr = cv;
    this.criterion = criterion;
    originalCorrectedExpression = expr;
  }

  public boolean getCriterion() {
    return criterion;
  }

  public CompiledValue getExpr() {
    return originalCorrectedExpression;
  }

  public int getColumnIndex() {
    return columnIndex;
  }

  @Override
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {

    return expr.evaluate(context);
  }

  private void substituteExpression(CompiledValue newExpression, int columnIndex) {
    expr = newExpression;
    this.columnIndex = columnIndex;
  }

  private void substituteExpressionWithProjectionField(int columnIndex) {
    expr = ProjectionField.getProjectionField();
    this.columnIndex = columnIndex;
  }

  private CompiledValue getReconstructedExpression(String projAttribStr, ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {
    List<CompiledValue> expressions = PathUtils.collectCompiledValuesInThePath(expr, context);
    StringBuilder tempBuff = new StringBuilder();
    ListIterator<CompiledValue> listIter = expressions.listIterator(expressions.size());
    while (listIter.hasPrevious()) {
      listIter.previous().generateCanonicalizedExpression(tempBuff, context);
      if (tempBuff.toString().equals(projAttribStr)) {
        // we have found our match from where we have to replace the expression
        break;
      } else {
        tempBuff.delete(0, tempBuff.length());
      }
    }

    // Now we need to create a new CompiledValue which terminates with
    // ProjectionField
    CompiledValue cvToRetainTill = listIter.previous();

    CompiledValue prevCV = null;
    List<Object> reconstruct = new ArrayList<Object>();
    CompiledValue cv = expressions.get(0);
    int index = 0;
    do {
      prevCV = cv;

      switch (cv.getType()) {
        case OQLLexerTokenTypes.METHOD_INV:
          reconstruct.add(0, ((CompiledOperation) cv).getArguments());
          reconstruct.add(0, ((CompiledOperation) cv).getMethodName());
          break;
        case CompiledValue.PATH:
          reconstruct.add(0, ((CompiledPath) cv).getTailID());
          break;
        case OQLLexerTokenTypes.TOK_LBRACK:
          reconstruct.add(0, ((CompiledIndexOperation) cv).getExpression());
          break;
        default:
          throw new IllegalStateException("Unexpected CompiledValue in order by clause");
      }
      reconstruct.add(0, prevCV.getType());
      cv = expressions.get(++index);
    } while (prevCV != cvToRetainTill);

    // Now reconstruct back
    Iterator<Object> iter = reconstruct.iterator();
    CompiledValue currentValue = ProjectionField.getProjectionField();
    while (iter.hasNext()) {
      int type = (Integer) iter.next();
      switch (type) {
        case CompiledValue.PATH:
          currentValue = new CompiledPath(currentValue, (String) iter.next());
          break;
        case OQLLexerTokenTypes.METHOD_INV:
          currentValue =
              new CompiledOperation(currentValue, (String) iter.next(), (List) iter.next());
          break;
        case OQLLexerTokenTypes.TOK_LBRACK:
          currentValue = new CompiledIndexOperation(currentValue, (CompiledValue) iter.next());
          break;
      }

    }

    return currentValue;
  }

  boolean mapExpressionToProjectionField(List projAttrs, ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {
    boolean mappedColumn = false;
    originalCorrectedExpression = expr;
    if (projAttrs != null) {
      // if expr is CompiledID , check for alias
      if (expr.getType() == OQLLexerTokenTypes.Identifier) {

        for (int i = 0; i < projAttrs.size() && !mappedColumn; ++i) {
          Object[] prj = (Object[]) TypeUtils.checkCast(projAttrs.get(i), Object[].class);
          if (prj[0] != null && prj[0].equals(((CompiledID) expr).getId())) {
            // set the field index
            substituteExpressionWithProjectionField(i);
            originalCorrectedExpression = (CompiledValue) prj[1];
            mappedColumn = true;

          }
        }

      }
      if (!mappedColumn) {
        // the order by expr is not an alias check for path
        StringBuilder orderByExprBuffer = new StringBuilder(),
            projAttribBuffer = new StringBuilder();
        expr.generateCanonicalizedExpression(orderByExprBuffer, context);
        final String orderByExprStr = orderByExprBuffer.toString();
        for (int i = 0; i < projAttrs.size(); ++i) {
          Object[] prj = (Object[]) TypeUtils.checkCast(projAttrs.get(i), Object[].class);
          CompiledValue cvProj = (CompiledValue) TypeUtils.checkCast(prj[1], CompiledValue.class);
          cvProj.generateCanonicalizedExpression(projAttribBuffer, context);
          final String projAttribStr = projAttribBuffer.toString();
          if (projAttribStr.equals(orderByExprStr)) {
            // set the field index
            substituteExpressionWithProjectionField(i);
            mappedColumn = true;
            break;
          } else if (orderByExprStr.startsWith(projAttribStr)) {
            CompiledValue newExpr = getReconstructedExpression(projAttribStr, context);
            substituteExpression(newExpr, i);
            mappedColumn = true;
            break;
          }
          projAttribBuffer.delete(0, projAttribBuffer.length());
        }
      }
    } else {
      RuntimeIterator rIter = context.findRuntimeIterator(expr);
      List currentIters = context.getCurrentIterators();
      for (int i = 0; i < currentIters.size(); ++i) {
        RuntimeIterator runtimeIter = (RuntimeIterator) currentIters.get(i);
        if (runtimeIter == rIter) {
          /* this.substituteExpressionWithProjectionField( i); */
          StringBuilder temp = new StringBuilder();
          rIter.generateCanonicalizedExpression(temp, context);
          // this.correctedCanonicalizedExpression = temp.toString();
          /* mappedColumn = true; */
          String projAttribStr = temp.toString();
          temp = new StringBuilder();
          expr.generateCanonicalizedExpression(temp, context);
          String orderbyStr = temp.toString();
          if (projAttribStr.equals(orderbyStr)) {
            substituteExpressionWithProjectionField(i);
            mappedColumn = true;
            break;
          } else {
            CompiledValue newExpr = getReconstructedExpression(projAttribStr, context);
            substituteExpression(newExpr, i);
            mappedColumn = true;
            break;
          }
        }
      }
    }
    return mappedColumn;

  }

  static class ProjectionField extends AbstractCompiledValue {

    @Immutable
    private static final ProjectionField singleton = new ProjectionField();

    private ProjectionField() {}

    @Override
    public Object evaluate(ExecutionContext context) throws FunctionDomainException,
        TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
      return context.getCurrentProjectionField();
    }

    @Override
    public int getType() {
      return FIELD;
    }

    public static ProjectionField getProjectionField() {
      return singleton;
    }

    @Override
    void setTypecast(ObjectType objectType) {
      throw new UnsupportedOperationException("Cannot modify singleton ProjectionField");
    }
  }
}
