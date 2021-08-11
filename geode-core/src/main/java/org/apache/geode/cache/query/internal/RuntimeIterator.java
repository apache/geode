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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.IndexCreationHelper;
import org.apache.geode.cache.query.internal.types.TypeUtils;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.query.types.StructType;

/**
 * Value representing a current iteration element. This is the representation used during
 * evaluation.
 *
 * A RuntimeIterator can be in one of two states. If it is independent of the current context scope
 * then its collection is evaluated lazily, in which case collection is initialized and knows its
 * elementType. The elementType field is the same value as in the collection. Otherwise, collection
 * is UNINITIALIZED and the elementType is set in any case.
 *
 * A RuntimeIterator can also be named or anonymous (name is null).
 *
 */
public class RuntimeIterator extends AbstractCompiledValue {

  // token to differentiate null from uninitialized
  @Immutable
  private static final SelectResults UNINITIALIZED = new ResultsBag(0, null);
  private Object current = UNINITIALIZED;
  private final String name;
  private SelectResults collection = UNINITIALIZED;
  private final CompiledIteratorDef cmpIteratorDefn;
  /** may be more specific than that in cmpIteratorDefn */
  private ObjectType elementType;
  /** for canonicalization */
  private String internalId = null;
  private String definition = null;
  private String index_internal_id = null;
  private int scopeID = -1;

  @Override
  public int getType() {
    return ITERATOR_DEF;
  }

  public ObjectType getElementType() {
    return elementType;
  }

  RuntimeIterator(CompiledIteratorDef cmpIteratorDefn, ObjectType elementType) {
    if (elementType == null || cmpIteratorDefn == null) {
      throw new IllegalArgumentException(
          "elementType and/or cmpIteratorDefn should not be null");
    }
    this.name = cmpIteratorDefn.getName();
    this.elementType = elementType;
    this.cmpIteratorDefn = cmpIteratorDefn;
  }

  CompiledIteratorDef getCmpIteratorDefn() {
    return cmpIteratorDefn;
  }

  public String getName() {
    return name;
  }

  /**
   * (Re)evaluate in the context of the current iterations through the cross-product. If this
   * iterator is not dependent on the current iteration, then just return the previously evaluated
   * collection. Otherwise, re-evaluate. Returns null if the collection itself is null or UNDEFINED
   */
  public SelectResults evaluateCollection(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    if (collection != UNINITIALIZED
        && !cmpIteratorDefn.isDependentOnAnyIteratorOfScopeLessThanItsOwn(context)
        && scopeID != IndexCreationHelper.INDEX_QUERY_SCOPE_ID) {
      return collection;
    }
    // limit the scope for evaluation to this RuntimeIterator:
    // we don't want to use this iterator or subsequent ones in the from clause
    // to evaluate this collection.
    collection = cmpIteratorDefn.evaluateCollection(context, this);
    if (collection == null) {
      return null;
    }
    // if we already have a more specific elementType, set it in the collection
    if (!elementType.equals(TypeUtils.OBJECT_TYPE)) {
      collection.setElementType(elementType);
    } else {
      // The elementType in the Collection obtained is more specific . So use that type.
      elementType = collection.getCollectionType().getElementType();
    }
    return collection;
  }

  @Override
  public Set computeDependencies(ExecutionContext context) {
    // called as the receiver of Path or Operation
    return Collections.singleton(this);
  }

  @Override
  public boolean isDependentOnIterator(RuntimeIterator itr, ExecutionContext context) {
    if (itr == this) {
      return true; // never true(?)
    }
    return cmpIteratorDefn.isDependentOnIterator(itr, context);
  }

  @Override
  public boolean isDependentOnCurrentScope(ExecutionContext context) {
    return cmpIteratorDefn.isDependentOnCurrentScope(context);
  }

  public void setCurrent(Object current) {
    this.current = current;
  }

  @Override
  public Object evaluate(ExecutionContext context) {
    Support.Assert(current != UNINITIALIZED,
        "error to evaluate RuntimeIterator without setting current first");
    return current;
  }

  boolean containsProperty(ExecutionContext context, String name, int numArgs,
      boolean mustBeMethod) {
    // first handle structs
    if ((elementType instanceof StructType) && !mustBeMethod) {
      // check field names
      String[] fieldName = ((StructType) elementType).getFieldNames();
      for (String s : fieldName) {
        if (name.equals(s)) {
          return true;
        }
      }
    }
    Class<?> clazz = elementType.resolveClass();
    if (numArgs > 0 || mustBeMethod) {
      // if numArgs==0, then just look up method directly
      // instead of sifting through all methods
      if (numArgs == 0) {
        try {
          clazz.getMethod(name, (Class[]) null);
          return true;
        } catch (NoSuchMethodException e) {
          return false;
        }
      }
      // enumerate methods and match with name
      // here, only check for a method with the same number of arguments.
      // we'll check for ambiguous method invocation when the method is
      // actually fully resolved and invoked
      Method[] methods = clazz.getMethods();
      for (Method m : methods) {
        if (m.getName().equals(name) && m.getParameterTypes().length == numArgs) {
          return true;
        }
      }
      return false;
    }
    // if there are zero arguments and it's an attribute, then defer to
    // AttributeDescriptor to see if there's a match
    return new AttributeDescriptor(context.getCache().getPdxRegistry(), name)
        .validateReadType(clazz);
  }

  @Override
  public String toString() {
    return getClass().getName() + " (name=" + name + " collection expr=" + cmpIteratorDefn + ")";
  }

  // Canonicalization
  void setInternalId(String id) {
    // it's okay for this to be set more than once; a RuntimeIterator
    // can be bound to a scope to compute dependencies, then
    // re-bound to a different scope later at eval time.
    // Support.Assert((internalId == null), "Internal ID is already set");
    Support.Assert((id != null), "Internal ID can not be null");
    internalId = id;
  }

  public String getInternalId() {
    // Support.Assert((internalId != null), "Internal ID is not yet set");
    return internalId;
  }

  public void setDefinition(String def) {
    Support.Assert((definition == null), "Definition is already set");
    Support.Assert((def != null), "Definition can not be null");
    definition = def;
  }

  public void setIndexInternalID(String index_id) {
    this.index_internal_id = index_id;
  }

  public String getIndexInternalID() {
    return index_internal_id;
  }

  public String getDefinition() {
    Support.Assert((definition != null), "Definition is not yet set");
    return definition;
  }

  @Override
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer,
      ExecutionContext context) {
    // prepend the internal iterator variable name for this RunTimeIterator
    int currScopeID = context.currentScope().getScopeID();
    if (currScopeID == scopeID) {
      // should have been set at this point
      clauseBuffer.insert(0, index_internal_id == null ? internalId : index_internal_id);
    } else {
      clauseBuffer.insert(0, internalId).insert(0, '_').insert(0, scopeID).insert(0, "scope");
    }
  }

  void setScopeID(int scopeID) {
    this.scopeID = scopeID;
  }

  int getScopeID() {
    return scopeID;
  }
}
