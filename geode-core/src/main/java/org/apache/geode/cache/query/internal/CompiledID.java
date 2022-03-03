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
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.internal.cache.PartitionedRegion;


/**
 * Class Description
 *
 * @version $Revision: 1.2 $
 */



public class CompiledID extends AbstractCompiledValue {
  private final String _id;


  public CompiledID(String id) {
    _id = id;
  }

  @Override
  public List getPathOnIterator(RuntimeIterator itr, ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException {
    CompiledValue val = context.resolve(getId());
    if (val == itr) {
      return new ArrayList(); // empty path
    }
    if (val.getType() == PATH && val.getReceiver() == itr) {
      List list = new ArrayList();
      list.add(_id);
      return list;
    }
    return null;
  }

  public String getId() {
    return _id;
  }

  @Override
  public boolean hasIdentifierAtLeafNode() {
    return true;
  }

  @Override
  public int getType() {
    return Identifier;
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {
    CompiledValue v = context.resolve(getId());
    return context.addDependencies(this, v.computeDependencies(context));
  }

  @Override
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    CompiledValue v = context.resolve(getId());
    Object obj = v.evaluate(context);
    // check for BucketRegion substitution
    PartitionedRegion pr = context.getPartitionedRegion();
    if (pr != null && (obj instanceof Region)) {
      if (pr.getFullPath().equals(((Region) obj).getFullPath())) {
        obj = context.getBucketRegion();
      }
    }
    return obj;
  }

  @Override
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {
    // The compiled ID can be an iterator variable or it can be a path variable.
    // So first resolve the type of variable using ExecutionContext
    // A compiledID will get resolved either to a RunTimeIterator or a CompiledPath
    context.resolve(_id).generateCanonicalizedExpression(clauseBuffer, context);

  }


}
