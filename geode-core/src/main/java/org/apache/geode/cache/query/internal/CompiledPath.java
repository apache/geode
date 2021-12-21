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
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.internal.cache.PartitionedRegion;


/**
 * Represents an identifier that follows a dot operator.
 *
 * @version $Revision: 1.1 $
 */



public class CompiledPath extends AbstractCompiledValue {
  private final CompiledValue _receiver; // the value represented by the expression before the dot
  private final String _tailID; // the identifier after the dot.

  public CompiledPath(CompiledValue rcvr, String id) {
    _receiver = rcvr;
    _tailID = id;
  }

  @Override
  public List getChildren() {
    return Collections.singletonList(_receiver);
  }

  @Override
  public int getType() {
    return PATH;
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {
    return context.addDependencies(this, _receiver.computeDependencies(context));
  }



  @Override
  public List getPathOnIterator(RuntimeIterator itr, ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException {
    if (!isDependentOnIterator(itr, context)) {
      return null;
    }

    List list = new ArrayList();
    list.add(getTailID());
    CompiledValue v = getReceiver();
    int type = v.getType();
    while (type == PATH) {
      CompiledPath p = (CompiledPath) v;
      list.add(0, p.getTailID());
      v = p.getReceiver();
      type = v.getType();
    }

    if (type == Identifier) {
      List path = v.getPathOnIterator(itr, context);
      if (path == null) {
        return null;
      }
      list.addAll(0, path);
      return list;
    }

    return null;
  }


  @Override
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    CompiledValue rcvr = getReceiver();
    Object evalRcvr = rcvr.evaluate(context);

    if (context.isCqQueryContext()
        && (evalRcvr instanceof Region.Entry || evalRcvr instanceof CqEntry)) {
      try {
        if (evalRcvr instanceof Region.Entry) {
          Region.Entry re = (Region.Entry) evalRcvr;
          if (re.isDestroyed()) {
            return QueryService.UNDEFINED;
          }
          evalRcvr = re.getValue();
        } else if (evalRcvr instanceof CqEntry) {
          CqEntry re = (CqEntry) evalRcvr;
          evalRcvr = re.getValue();
        }
      } catch (EntryDestroyedException ede) {
        // Even though isDestory() check is made, the entry could
        // throw EntryDestroyedException if the value becomes null.
        return QueryService.UNDEFINED;
      }

    }

    // if the receiver is an iterator, then use the contrained type
    // for attribute evaluation instead of the runtime type

    // RuntimeIterator cmpItr = null;

    // if (rcvr.getType() == ID)
    // {
    // CompiledValue resolvedRcvr = context.resolve(((CompiledID)rcvr).getId());

    // if (resolvedRcvr != null && resolvedRcvr.getType() == ITERATOR)
    // cmpItr = ((RuntimeIterator)resolvedRcvr);
    // }

    // if (rcvr.getType() == ITERATOR)
    // cmpItr = (RuntimeIterator)rcvr;

    // if (cmpItr != null)
    // {
    // Class constraint = cmpItr.getBaseCollection().getConstraint();
    // return PathUtils.evaluateAttribute(evalRcvr,
    // constraint,
    // getTailID());
    // }

    Object obj = PathUtils.evaluateAttribute(context, evalRcvr, getTailID());
    // check for BucketRegion substitution
    PartitionedRegion pr = context.getPartitionedRegion();
    if (pr != null && (obj instanceof Region)) {
      if (pr.getFullPath().equals(((Region) obj).getFullPath())) {
        obj = context.getBucketRegion();
      }
    }
    return obj;
  }

  public String getTailID() {
    return _tailID;
  }

  @Override
  public CompiledValue getReceiver() {
    return _receiver;
  }

  @Override
  public boolean hasIdentifierAtLeafNode() {
    if (_receiver.getType() == Identifier) {
      return true;
    } else {
      return _receiver.hasIdentifierAtLeafNode();
    }
  }

  @Override
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {
    // Asif: Canonicalize the tail ID. If the tail ID contains
    // something like getX ,convert it into x.
    int len;
    if (_tailID.startsWith("get") && (len = _tailID.length()) > 3) {
      clauseBuffer.insert(0, len > 4 ? _tailID.substring(4) : "");
      clauseBuffer.insert(0, Character.toLowerCase(_tailID.charAt(3)));
    } else {
      clauseBuffer.insert(0, _tailID);
    }
    clauseBuffer.insert(0, '.');
    _receiver.generateCanonicalizedExpression(clauseBuffer, context);
  }
}
