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

import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.internal.Assert;


/**
 * Class Description
 *
 * @version $Revision: 1.1 $
 */


public class CompiledConstruction extends AbstractCompiledValue {
  private final Class objectType;
  private final List args;

  public CompiledConstruction(Class objectType, List args) {
    this.objectType = objectType;
    this.args = args;
  }

  @Override
  public List getChildren() {
    return args;
  }


  @Override
  public int getType() {
    return CONSTRUCTION;
  }

  @Override
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    // we only support ResultsSet now
    Assert.assertTrue(objectType == ResultsSet.class);
    ResultsSet newSet = new ResultsSet(args.size());
    for (final Object arg : args) {
      CompiledValue cv = (CompiledValue) arg;
      Object eval = cv.evaluate(context);
      if (eval == QueryService.UNDEFINED) {
        return QueryService.UNDEFINED;
      }
      newSet.add(eval);
    }
    return newSet;
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {
    for (final Object arg : args) {
      CompiledValue cv = (CompiledValue) arg;
      context.addDependencies(this, cv.computeDependencies(context));
    }
    return context.getDependencySet(this, true);
  }
}
