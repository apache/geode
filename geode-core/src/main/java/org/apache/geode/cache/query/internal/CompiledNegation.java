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
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.TypeMismatchException;


/**
 * Class Description
 *
 * @version $Revision: 1.1 $
 */


public class CompiledNegation extends AbstractCompiledValue {
  private CompiledValue _value;

  public CompiledNegation(CompiledValue value) {
    _value = value;
  }

  @Override
  public List getChildren() {
    return Collections.singletonList(this._value);
  }

  public int getType() {
    return LITERAL_not;
  }

  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return negateObject(_value.evaluate(context));
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    return context.addDependencies(this, this._value.computeDependencies(context));
  }

  private Object negateObject(Object obj) throws TypeMismatchException {
    if (obj instanceof Boolean)
      return Boolean.valueOf(!((Boolean) obj).booleanValue());
    if (obj == null || obj == QueryService.UNDEFINED)
      return QueryService.UNDEFINED;
    throw new TypeMismatchException(
        String.format("%s cannot be negated", obj.getClass()));
  }

  @Override
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    clauseBuffer.insert(0, ')');
    _value.generateCanonicalizedExpression(clauseBuffer, context);
    clauseBuffer.insert(0, "NOT(");
  }

}
