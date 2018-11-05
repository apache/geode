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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;

/**
 * Class Description
 *
 * @version $Revision: 1.1 $
 */
public abstract class AbstractCompiledValue implements CompiledValue, Filter, OQLLexerTokenTypes {

  ObjectType typecast = null;

  public ObjectType getTypecast() {
    return this.typecast;
  }

  // used for typecasts
  void setTypecast(ObjectType objectType) {
    this.typecast = objectType;
  }

  /** Default impl returns null as N/A */
  public List getPathOnIterator(RuntimeIterator itr, ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException {
    return null;
  }

  /**
   * Asif : This function has a meaningful implementaion only in CompiledComparison & Compiled
   * Undefined
   */

  public SelectResults filterEvaluate(ExecutionContext context, SelectResults iterationLimit,
      boolean completeExpansionNeeded, CompiledValue iterOperands, RuntimeIterator[] indpndntItrs,
      boolean isIntersection, boolean conditioningNeeded, boolean evalProj)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    Support.assertionFailed(
        "This method should not have invoked as CompieldComparison & CompiledUndefined are the only classes on which this invocation should have occurred ");
    return null;
  }

  // determine whether should evaluate as filters, and what indexes will
  // be used
  // default is true for evalAsFilter if independent of iterator, otherwise
  // calls protGetPlanInfo (which defaults to false for evalAsFilter)
  public PlanInfo getPlanInfo(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    PlanInfo planInfo;
    // RuntimeIterator itr = context.getCurrentIterator();
    // Support.Assert(itr != null);
    if (!isDependentOnCurrentScope(context)) {
      // Asif :If the current junction or CompliedComparison
      // happens to be independent , then its basic nature to be
      // evaluatable as Filter depends upon its boolean value.
      // If the operand is part of a Junction , then if it evaluates
      // to false, it can always support filterEvaluatable. But if it
      // evaluates to true, its nature is not to support filterEvaluatable
      // The gist being that if an independent operand evaluates to
      // false & if the junction is AND , the whole junction can be
      // filter evaluatable. But if it evaluates to true, then by itself
      // it cannot make junction filterEvaluatable, but if there exists
      // any other operand which had an index or is independent ( with value
      // as false), then the current operand can get AuxFilterEvaluated
      // along with the other auxFilterEvaluatable operand.
      // But if it is an OR junction , a true value would mean that the
      // Junction cannot be evaluated as filter , but if it is false then
      // junction can be evaluated as a filter , provided all the operands
      // are also filterEvaluatable.
      planInfo = new PlanInfo();
      Object result = evaluate(context);
      if (!(result instanceof Boolean))
        throw new TypeMismatchException(
            String.format("boolean value expected, not type ' %s '",
                result.getClass().getName()));
      boolean b = ((Boolean) result).booleanValue();
      planInfo.evalAsFilter = !b;
      return planInfo;
    }
    planInfo = protGetPlanInfo(context);
    return planInfo;
  }

  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    // default implementation has no dependencies
    // override in subclasses to add dependencies
    return Collections.EMPTY_SET;
  }

  public boolean isDependentOnIterator(RuntimeIterator itr, ExecutionContext context) {
    return context.isDependentOn(this, itr);
  }

  /**
   * Return true if this value is dependent on any iterator in the current scope
   */
  public boolean isDependentOnCurrentScope(ExecutionContext context) {
    return context.isDependentOnCurrentScope(this);
  }

  // Invariant: the receiver is dependent on the current iterator.
  protected PlanInfo protGetPlanInfo(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return new PlanInfo(); // default behavior
  }

  // Utility methods
  /**
   * return operator used when terms are reversed, maintaining the semantics e.g salary < 100000
   * swapped to 100000 > salary, salary >= 100000 swapped to 100000 <= salary or salary = 100000
   * swapped to salary = 100000
   */
  protected int reflectOperator(int op) {
    switch (op) {
      case TOK_EQ:
        return TOK_EQ;
      case TOK_NE:
        return TOK_NE;
      case TOK_LT:
        return TOK_GT;
      case TOK_GE:
        return TOK_LE;
      case TOK_LE:
        return TOK_GE;
      case TOK_GT:
        return TOK_LT;
      default:
        Support.assertionFailed("unknown operator: " + op);
        throw new Error("this line of code can never be executed");
    }
  }

  /**
   * return operator to invert the value of the result. e.g. salary < 100000 will become salary >=
   * 100000 or salary = 100000 becomes salary != 100000
   */
  protected int inverseOperator(int op) {
    switch (op) {
      case LITERAL_and:
        return LITERAL_or;
      case LITERAL_or:
        return LITERAL_and;
      case TOK_EQ:
        return TOK_NE;
      case TOK_NE:
        return TOK_EQ;
      case TOK_LT:
        return TOK_GE;
      case TOK_GE:
        return TOK_LT;
      case TOK_LE:
        return TOK_GT;
      case TOK_GT:
        return TOK_LE;
      default:
        Support.assertionFailed("unknown operator: " + op);
        throw new Error("this line of code can never be executed");
    }
  }

  /**
   * this is a lower level filter evaluation call. Most Filters do nothing different here than a
   * normal filterEvaluate. This is here for benefit of nested CompiledJunctions: filterEvaluate is
   * called first, then auxFilterEvaluate is called on the operands that have been organized to be
   * filters
   *
   * @see CompiledJunction#filterEvaluate
   */
  public SelectResults auxFilterEvaluate(ExecutionContext context,
      SelectResults intermediateResults) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    return filterEvaluate(context, intermediateResults);
  }

  public SelectResults filterEvaluate(ExecutionContext context, SelectResults intermediateResults)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    // for the general case of filter evaluation, most compiled values
    // can only be evaluated in this way if they are independent of the current
    // iterator,
    // This method is also only applicable in places where a boolean value is
    // expected.
    // RuntimeIterator itr = context.getCurrentIterator();
    // Support.Assert(itr != null);
    // if it is possible for the following assertion to be false,
    // then this method as well as protGetPlanInfo must be overridden
    Support.Assert(!isDependentOnCurrentScope(context));
    Object result = evaluate(context);
    if (result == null || result == QueryService.UNDEFINED)
      return new ResultsBag(intermediateResults.getCollectionType().getElementType(), 0,
          context.getCachePerfStats());
    if (!(result instanceof Boolean))
      throw new TypeMismatchException(
          String.format("boolean value expected, not type ' %s '",
              result.getClass().getName()));
    boolean b = ((Boolean) result).booleanValue();
    // Asif : Boolean true, means the cartesian of all the RuntimeIterators
    // indicated by null value. A false means an empty ResultSet
    if (b) {
      return null;
    } else {
      // Asif : We need to return either an empty ResultSet or an empty
      // StructSet based
      // on the number of iterators in the current scope.
      SelectResults emptySet = null;
      List iterators = context.getCurrentIterators();
      int len = iterators.size();
      if (len == 1) {
        ObjectType elementType = ((RuntimeIterator) iterators.get(0)).getElementType();
        emptySet = context.isDistinct() ? new ResultsSet(elementType)
            : new ResultsBag(elementType, 0, context.getCachePerfStats());
      } else {
        String fieldNames[] = new String[len];
        ObjectType fieldTypes[] = new ObjectType[len];
        for (int i = 0; i < len; i++) {
          RuntimeIterator iter = (RuntimeIterator) iterators.get(i);
          fieldNames[i] = iter.getInternalId();
          fieldTypes[i] = iter.getElementType();
        }
        emptySet = context.isDistinct() ? new StructSet(new StructTypeImpl(fieldNames, fieldTypes))
            : new StructBag(0, new StructTypeImpl(fieldNames, fieldTypes),
                context.getCachePerfStats());
      }
      return emptySet;
    }
  }

  // This function needs to be appropriately overridden in the derived classes
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    clauseBuffer.insert(0, System.currentTimeMillis());
    clauseBuffer.insert(0, this.getClass());
    clauseBuffer.insert(0, '.');
  }

  public void getRegionsInQuery(Set regionsInQuery, Object[] parameters) {
    for (Iterator itr = getChildren().iterator(); itr.hasNext();) {
      CompiledValue v = (CompiledValue) itr.next();
      if (v == null) {
        throw new NullPointerException(
            String.format("Got null as a child from %s",
                this));
      }
      v.getRegionsInQuery(regionsInQuery, parameters);
    }
  }

  /** Get the CompiledValues that this owns */
  public List getChildren() {
    return Collections.EMPTY_LIST;
  }

  public int getSizeEstimate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }

  public void visitNodes(NodeVisitor visitor) {
    visitor.visit(this);
    for (Iterator itr = getChildren().iterator(); itr.hasNext();) {
      if (!visitor.visit((CompiledValue) itr.next())) {
        break;
      }
    }
  }

  public boolean isProjectionEvaluationAPossibility(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }

  public boolean isLimitApplicableAtIndexLevel(ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }

  public boolean isOrderByApplicableAtIndexLevel(ExecutionContext context,
      String canonicalizedOrderByClause) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }

  public boolean isConditioningNeededForIndex(RuntimeIterator independentIter,
      ExecutionContext context, boolean completeExpnsNeeded)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }

  public int getOperator() {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }

  public boolean isBetterFilter(Filter comparedTo, ExecutionContext context, int thisSize)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }

  public CompiledValue getReceiver() {
    throw new UnsupportedOperationException("This method should not have been invoked");
  }
}
