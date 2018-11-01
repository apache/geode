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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.internal.PdxString;

/**
 * Class Description
 *
 * @version $Revision: 1.1 $
 */


public class CompiledOperation extends AbstractCompiledValue {
  private final CompiledValue receiver; // may be null if implicit to scope
  private final String methodName;
  private final List args;
  private static final ConcurrentMap cache = new ConcurrentHashMap();


  // receiver is an ID or PATH that contains the operation name
  public CompiledOperation(CompiledValue receiver, String methodName, List args) {
    this.receiver = receiver;
    this.methodName = methodName;
    this.args = args;
  }

  @Override
  public List getChildren() {
    List list = new ArrayList();
    if (this.receiver != null) {
      list.add(this.receiver);
    }
    list.addAll(this.args);
    return list;
  }

  public String getMethodName() {
    return this.methodName;
  }

  public List getArguments() {
    return this.args;
  }


  public int getType() {
    return METHOD_INV;
  }

  public CompiledValue getReceiver(ExecutionContext cxt) {
    // receiver may be cached in execution context
    if (this.receiver == null && cxt != null) {
      return (CompiledValue) cxt.cacheGet(this);
    }
    return this.receiver;
  }

  @Override
  public CompiledValue getReceiver() {
    return this.getReceiver(null);
  }

  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    CompiledValue rcvr = getReceiver(context);

    Object result;
    Object evalRcvr;

    if (rcvr == null) { // must be intended as implicit iterator operation
      // see if it's an implicit operation name
      RuntimeIterator rcvrItr =
          context.resolveImplicitOperationName(this.methodName, this.args.size(), true);
      evalRcvr = rcvrItr.evaluate(context);
      /*
       * // evaluate on current iteration of collection if (rcvrItr != null) { result =
       * eval0(rcvrItr.evaluate(context), rcvrItr.getElementType().resolveClass(), context); }
       *
       * // function call: no functions implemented except keywords in the grammar throw new
       * TypeMismatchException("Could not resolve method named 'xyz'")
       */
    } else {
      // if not null, then explicit receiver
      evalRcvr = rcvr.evaluate(context);
    }

    // short circuit null immediately
    if (evalRcvr == null) {
      return QueryService.UNDEFINED;
    }

    if (context.isCqQueryContext() && evalRcvr instanceof Region.Entry) {
      Region.Entry re = (Region.Entry) evalRcvr;
      if (re.isDestroyed()) {
        return QueryService.UNDEFINED;
      }
      try {
        evalRcvr = re.getValue();
      } catch (EntryDestroyedException ede) {
        // Even though isDestory() check is made, the entry could
        // throw EntryDestroyedException if the value becomes null.
        return QueryService.UNDEFINED;
      }
    }

    // check if the receiver is the iterator, in which
    // case we resolve the method on the constraint rather
    // than the runtime type of the receiver
    Class resolveClass = null;
    // commented out because we currently always resolve the method
    // on the runtime types

    // CompiledValue rcvrVal = rcvrPath.getReceiver();
    // if (rcvrVal.getType() == ID)
    // {
    // CompiledValue resolvedID = context.resolve(((CompiledID)rcvrVal).getId());
    // if (resolvedID.getType() == ITERATOR)
    // {
    // resolveClass = ((RuntimeIterator)resolvedID).getBaseCollection().getConstraint();
    // }
    // }
    // if (resolveClass == null)
    if (evalRcvr instanceof PdxInstance) {
      String className = ((PdxInstance) evalRcvr).getClassName();
      try {
        resolveClass = InternalDataSerializer.getCachedClass(className);
      } catch (ClassNotFoundException cnfe) {
        throw new QueryInvocationTargetException(cnfe);
      }
    } else if (evalRcvr instanceof PdxString) {
      resolveClass = String.class;
    } else {
      resolveClass = evalRcvr.getClass();
    }

    result = eval0(evalRcvr, resolveClass, context);
    // }
    // check for PR substitution
    // check for BucketRegion substitution
    PartitionedRegion pr = context.getPartitionedRegion();
    if (pr != null && (result instanceof Region)) {
      if (pr.getFullPath().equals(((Region) result).getFullPath())) {
        result = context.getBucketRegion();
      }
    }
    return result;
  }



  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    List args = this.args;
    Iterator i = args.iterator();
    while (i.hasNext()) {
      context.addDependencies(this, ((CompiledValue) i.next()).computeDependencies(context));
    }

    CompiledValue rcvr = getReceiver(context);
    if (rcvr == null) // implicit iterator operation
    {
      // see if it's an implicit operation name
      RuntimeIterator rcvrItr =
          context.resolveImplicitOperationName(this.methodName, this.args.size(), true);
      if (rcvrItr == null) { // no receiver resolved
        // function call: no functions implemented except keywords in the grammar
        throw new TypeMismatchException(
            String.format("Could not resolve method named ' %s '",
                this.methodName));
      }
      // cache the receiver so we don't have to resolve it again
      context.cachePut(this, rcvrItr);
      return context.addDependency(this, rcvrItr);
    }

    // receiver is explicit
    return context.addDependencies(this, rcvr.computeDependencies(context));
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "RV_RETURN_VALUE_OF_PUTIFABSENT_IGNORED",
      justification = "Does not matter if the methodDispatch that isn't stored in the map is used")
  private Object eval0(Object receiver, Class resolutionType, ExecutionContext context)
      throws TypeMismatchException, FunctionDomainException, NameResolutionException,
      QueryInvocationTargetException {
    if (receiver == null || receiver == QueryService.UNDEFINED)
      return QueryService.UNDEFINED;

    List args = new ArrayList();
    List argTypes = new ArrayList();
    Iterator i = this.args.iterator();
    while (i.hasNext()) {
      CompiledValue arg = (CompiledValue) i.next();
      Object o = arg.evaluate(context);

      // undefined arg produces undefines method result
      if (o == QueryService.UNDEFINED)
        return QueryService.UNDEFINED;

      args.add(o);
      // pass in null for the type if the runtime value is null
      if (o == null)
        argTypes.add(null);
      // commented out because we currently always use the runtime type for args
      // else if (arg.getType() == Identifier)
      // {
      // CompiledValue resolved = context.resolve(((CompiledID)arg).getId());
      // if (resolved != null && resolved.getType() == ITERATOR)
      // argTypes.add(((RuntimeIterator)resolved).getBaseCollection().getConstraint());
      // else
      // argTypes.add(o.getClass());
      // }
      else
        argTypes.add(o.getClass()); // otherwise use the runtime type
    }

    // see if in cache
    MethodDispatch methodDispatch;
    List key = Arrays.asList(new Object[] {resolutionType, this.methodName, argTypes});
    methodDispatch = (MethodDispatch) CompiledOperation.cache.get(key);
    if (methodDispatch == null) {
      try {
        methodDispatch =
            new MethodDispatch(context.getCache().getQueryService().getMethodInvocationAuthorizer(),
                resolutionType, this.methodName, argTypes);
      } catch (NameResolutionException nre) {
        if (!org.apache.geode.cache.query.Struct.class.isAssignableFrom(resolutionType)
            && (DefaultQueryService.QUERY_HETEROGENEOUS_OBJECTS
                || DefaultQueryService.TEST_QUERY_HETEROGENEOUS_OBJECTS)) {
          return QueryService.UNDEFINED;
        } else {
          throw nre;
        }
      }
      // cache
      CompiledOperation.cache.putIfAbsent(key, methodDispatch);
    }
    if (receiver instanceof PdxInstance) {
      try {
        if (receiver instanceof PdxInstanceImpl) {
          receiver = ((PdxInstanceImpl) receiver).getCachedObject();
        } else {
          receiver = ((PdxInstance) receiver).getObject();
        }
      } catch (PdxSerializationException ex) {
        throw new QueryInvocationTargetException(ex);
      }
    } else if (receiver instanceof PdxString) {
      receiver = ((PdxString) receiver).toString();
    }
    return methodDispatch.invoke(receiver, args);
  }

  // Asif :Function for generating from clause
  @Override
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    // Asif: if the method name starts with getABC & argument list is empty
    // then canonicalize it to aBC
    int len;
    if (this.methodName.startsWith("get") && (len = this.methodName.length()) > 3
        && (this.args == null || this.args.isEmpty())) {
      clauseBuffer.insert(0, len > 4 ? this.methodName.substring(4) : "");
      clauseBuffer.insert(0, Character.toLowerCase(this.methodName.charAt(3)));
    } else if (this.args == null || this.args.isEmpty()) {
      clauseBuffer.insert(0, "()").insert(0, this.methodName);
    } else {
      // The method contains arguments which need to be canonicalized
      clauseBuffer.insert(0, ')');
      CompiledValue cv = null;
      for (int j = this.args.size(); j > 0;) {
        cv = (CompiledValue) this.args.get(--j);
        cv.generateCanonicalizedExpression(clauseBuffer, context);
        clauseBuffer.insert(0, ',');
      }
      clauseBuffer.deleteCharAt(0).insert(0, '(').insert(0, this.methodName);

    }
    clauseBuffer.insert(0, '.');
    CompiledValue rcvr = this.receiver;
    if (rcvr == null) {
      // must be intended as implicit iterator operation
      // see if it's an implicit operation name. The receiver will now be RuntimeIterator
      rcvr = context.resolveImplicitOperationName(this.methodName, this.args.size(), true);
    }
    rcvr.generateCanonicalizedExpression(clauseBuffer, context);
  }

}
