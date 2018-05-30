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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.CollectionType;
import org.apache.geode.cache.query.types.MapType;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.NotAuthorizedException;

public class CompiledIteratorDef extends AbstractCompiledValue {
  private static final Logger logger = LogService.getLogger();

  private String name;
  private ObjectType elementType;
  private CompiledValue collectionExpr;

  /**
   * Creates a new instance of CompiledIteratorDef name and type can be null
   */
  public CompiledIteratorDef(String name, ObjectType elementType, CompiledValue collectionExpr) {
    this.name = name;
    this.elementType = elementType == null ? TypeUtils.OBJECT_TYPE : elementType;
    this.collectionExpr = collectionExpr;
  }

  @Override
  public List getChildren() {
    return Collections.singletonList(collectionExpr);
  }

  /**
   * Returns a RuntimeIterator (or null if evaluates to null or UNDEFINED); the collection expr is
   * evaluated lazily after dependencies are known
   */
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    throw new UnsupportedOperationException(
        LocalizedStrings.CompiledIteratorDef_NOT_TO_BE_EVALUATED_DIRECTLY.toLocalizedString());
  }

  public RuntimeIterator getRuntimeIterator(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    RuntimeIterator rIter = null;
    // check cached in context
    rIter = (RuntimeIterator) context.cacheGet(this);
    if (rIter != null) {
      return rIter;
    }
    ObjectType type = this.elementType;
    if (type.equals(TypeUtils.OBJECT_TYPE)) {
      // check to see if there's a typecast for this collection
      ObjectType typc = getCollectionElementTypeCast();
      if (typc != null) {
        type = typc;
      } else {
        // Try to determine better type
        // Now only works for CompiledPaths
        // Does not determine type of nested query
        if (!(this.collectionExpr instanceof CompiledSelect)) {
          type = computeElementType(context);
        }
      }
    }
    rIter = new RuntimeIterator(this, type);
    // generate from clause should take care of bucket region substitution if
    // necessary and then set the definition.
    String fromClause = genFromClause(context);
    rIter.setDefinition(fromClause);
    /*
     * If the type of RunTimeIterator is still ObjectType & if the RuneTimeIterator is independent
     * of any iterator of the scopes less than or equal to its own scope, we can evaluate the
     * collection via RuntimeIterator. This will initialize the Collection of RuntimeIterator ,
     * which is OK. The code in RuntimeIterator will be rectified such that the ElementType of that
     * RuntimeIterator is taken from the collection
     */
    if (type.equals(TypeUtils.OBJECT_TYPE)
        && !this.isDependentOnAnyIteratorOfScopeLessThanItsOwn(context)) {
      // The current Iterator definition is independent , so lets evaluate
      // the collection
      evaluateCollectionForIndependentIterator(context, rIter);
    }
    // cache in context
    context.cachePut(this, rIter);
    return rIter;
  }

  protected void evaluateCollectionForIndependentIterator(ExecutionContext context,
      RuntimeIterator rIter)
      throws RegionNotFoundException, TypeMismatchException {
    try {
      rIter.evaluateCollection(context);
    } catch (QueryExecutionTimeoutException qet) {
      throw qet;
    } catch (RegionNotFoundException re) {
      throw re;
    } catch (NotAuthorizedException e) {
      throw e;
    } catch (QueryExecutionCanceledException e) {
      throw e;
    } catch (CacheClosedException e) {
      throw e;
    } catch (Exception e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Exception while getting runtime iterator.", e);
      }
      throw new TypeMismatchException(
          LocalizedStrings.CompiledIteratorDef_EXCEPTION_IN_EVALUATING_THE_COLLECTION_EXPRESSION_IN_GETRUNTIMEITERATOR_EVEN_THOUGH_THE_COLLECTION_IS_INDEPENDENT_OF_ANY_RUNTIMEITERATOR
              .toLocalizedString(),
          e);
    }
  }

  ObjectType getCollectionElementTypeCast() throws TypeMismatchException {
    ObjectType typ = this.collectionExpr.getTypecast();
    if (typ != null) {
      if (!(typ instanceof CollectionType)) {
        throw new TypeMismatchException(
            LocalizedStrings.CompiledIteratorDef_AN_ITERATOR_DEFINITION_MUST_BE_A_COLLECTION_TYPE_NOT_A_0
                .toLocalizedString(typ));
      }
      if (typ instanceof MapType) { // we iterate over map entries
        return ((MapType) typ).getEntryType();
      }
      return ((CollectionType) typ).getElementType();
    }
    return null;
  }

  /** Evaluate just the collectionExpr */
  SelectResults evaluateCollection(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return evaluateCollection(context, null);
  }

  /**
   * Evaluate just the collectionExpr
   *
   * @param stopAtIter the RuntimeIterator associated with this iterator defn -- don't use this or
   *        any subsequent runtime iterators to evaluate.
   */
  SelectResults evaluateCollection(ExecutionContext context, RuntimeIterator stopAtIter)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    Object coll;

    // Check if query execution on this thread is Canceled.
    // QueryMonitor.isQueryExecutionCanceled();

    context.currentScope().setLimit(stopAtIter);
    try {
      coll = this.collectionExpr.evaluate(context);
    } finally {
      context.currentScope().setLimit(null);
    }
    // if we don't have an elementType and there's a typecast, apply the
    // element type here
    if (TypeUtils.OBJECT_TYPE.equals(this.elementType)) {
      ObjectType elmTypc = getCollectionElementTypeCast();
      if (elmTypc != null) {
        this.elementType = elmTypc;
      }
    }

    // PR bucketRegion substitution should have already happened
    // at the expression evaluation level

    return prepareIteratorDef(coll, this.elementType, context);
  }

  public int getType() {
    return OQLLexerTokenTypes.ITERATOR_DEF;
  }

  // for test purposes...
  public String getName() {
    return this.name;
  }

  public ObjectType getElementType() {
    return this.elementType;
  }

  public CompiledValue getCollectionExpr() {
    return this.collectionExpr;
  }

  public void setCollectionExpr(CompiledValue collectionExpr) {
    this.collectionExpr = collectionExpr;
  }

  /**
   * TODO: We need to implement the belwo method of computeDependencies Once we come to implement
   * changes for partitioned region querying, as in that case if first iterator itself is a Select
   * Query , then ideally we cannot call that CompiledIteratorDef independent ( which will be the
   * case at present). When we use this commented function we will also need to take care of
   * correctly implementing the function isDependentOnCurrentScope etc functions.
   */
  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    return context.addDependencies(this, this.collectionExpr.computeDependencies(context));
  }

  // TODO: this method is overly complex, duplicating logic already
  // in query evaluation itself. It is overly complex ==> It will not be
  // necessary once we have full typing support.
  // There is a limitation here that it assumes that the collectionExpr is some
  // path on either a RuntimeIterator or a Region.
  private ObjectType computeElementType(ExecutionContext context) throws AmbiguousNameException {
    ObjectType type = PathUtils.computeElementTypeOfExpression(context, this.collectionExpr);
    // if it's a Map, we want the Entry type, not the value type
    if (type.isMapType()) {
      return ((MapType) type).getEntryType();
    }
    if (type.isCollectionType()) { // includes Regions and arrays
      return ((CollectionType) type).getElementType();
    }
    return type;
  }

  /**
   * Convert the given object to a SelectResults. Must be a collection of some sort. The obj passed
   * in must be unmodified, but the resulting SelectResults may or may not be modifiable. Return
   * null if obj is null or UNDEFINED.
   */
  private SelectResults prepareIteratorDef(Object obj, ObjectType elementType,
      ExecutionContext context) throws TypeMismatchException {
    if (obj == null) {
      return null;
    }
    if (obj == QueryService.UNDEFINED) {
      return null;
    }
    if (obj instanceof SelectResults) {
      // probably came from nested query or is a QRegion already from region
      // path
      SelectResults sr = (SelectResults) obj;
      // override the elementType if not Object.class (does not apply to
      // StructBags)
      if (!elementType.equals(TypeUtils.OBJECT_TYPE)) {
        sr.setElementType(elementType);
      }
      return sr;
    }
    if (obj instanceof Region) {
      // this can happen if region passed in as parameter
      QRegion qRegion = new QRegion((Region) obj, false, context);
      if (!elementType.equals(TypeUtils.OBJECT_TYPE)) {
        // override the valueConstraint, if any
        qRegion.setElementType(elementType);
      }
      return qRegion;
    }
    // if this is a domain collection, it should be unmodifiable
    // if obj is a Collection but not a SelectResults, it must be from the
    // domain, otherwise it would be a SelectResults.
    if (obj instanceof Collection) {
      // do not lose ordering and duplicate information,
      ResultsCollectionWrapper res = new ResultsCollectionWrapper(elementType, (Collection) obj);
      res.setModifiable(false);
      return res;
    }
    // Object[] is wrapped and considered a domain object so unmodifiable
    if (obj instanceof Object[]) {
      // the element type is specified in the array itself, unless we have
      // something more specific
      if (elementType.equals(TypeUtils.OBJECT_TYPE)) { // if we don't have
        // constraint info
        elementType = TypeUtils.getObjectType(obj.getClass().getComponentType());
      }
      // do not lose ordering and duplicate information,
      ResultsCollectionWrapper res =
          new ResultsCollectionWrapper(elementType, Arrays.asList((Object[]) obj));
      res.setModifiable(false);
      return res;
    }
    // TODO: primitive arrays?
    if (obj instanceof Map) {
      if (elementType.equals(TypeUtils.OBJECT_TYPE)) { // if we don't have more
        // specific type info,
        // use Map.Entry
        elementType = TypeUtils.getObjectType(Map.Entry.class);
      }
      ResultsCollectionWrapper res =
          new ResultsCollectionWrapper(elementType, ((Map) obj).entrySet());
      res.setModifiable(false);
      return res;
    } else {
      obj = new Object[] {obj};
      // the element type is specified in the array itself, unless we have
      // something more specific
      if (elementType.equals(TypeUtils.OBJECT_TYPE)) { // if we don't have
        // constraint info
        elementType = TypeUtils.getObjectType(obj.getClass().getComponentType());
      }
      // do not lose ordering and duplicate information,
      ResultsCollectionWrapper res =
          new ResultsCollectionWrapper(elementType, Arrays.asList((Object[]) obj));
      res.setModifiable(false);
      return res;
    }
  }

  String genFromClause(ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    StringBuilder sbuff = new StringBuilder();
    collectionExpr.generateCanonicalizedExpression(sbuff, context);
    return sbuff.toString();
  }

  /**
   * Checks if the iterator in question is dependent on any other RuntimeIterator of its own or
   * lesser scope.
   */
  boolean isDependentOnAnyIteratorOfScopeLessThanItsOwn(ExecutionContext context) {
    // Get the list of all iterators on which the colelction expression
    // is ultimately dependent on

    // If dependent on self then also assume it to be dependent
    boolean isDep = false;
    // Get the list of all iterators on which the colelction expression
    // is dependent on
    Set dependencySet = context.getDependencySet(this, true);
    Iterator itr = dependencySet.iterator();
    int currScope = context.currentScope().getScopeID();// context.getScopeCount();
    while (itr.hasNext()) {
      RuntimeIterator ritr = (RuntimeIterator) itr.next();
      if (ritr.getScopeID() <= currScope) {
        isDep = true;
        break;
      }
    }
    return isDep;
  }
}
