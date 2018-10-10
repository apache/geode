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
import java.util.List;
import java.util.Set;

import org.apache.geode.cache.query.AmbiguousNameException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;


/**
 * Predefined functions
 *
 * @version $Revision: 1.1 $
 */


public class CompiledFunction extends AbstractCompiledValue {
  private CompiledValue[] _args;
  private int _function;

  public CompiledFunction(CompiledValue[] args, int function) {
    _args = args;
    _function = function;
  }

  @Override
  public List getChildren() {
    return Arrays.asList(this._args);
  }


  public int getType() {
    return FUNCTION;
  }

  public int getFunction() {
    return this._function;
  }

  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    if (this._function == LITERAL_element) {
      Object arg = _args[0].evaluate(context);
      return call(arg, context);
    } else if (this._function == LITERAL_nvl) {
      return Functions.nvl(_args[0], _args[1], context);
    } else if (this._function == LITERAL_to_date) {
      return Functions.to_date(_args[0], _args[1], context);
    } else {
      throw new QueryInvalidException(
          "UnSupported function was used in the query");
    }
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    int len = this._args.length;
    for (int i = 0; i < len; i++) {
      context.addDependencies(this, this._args[i].computeDependencies(context));
    }
    return context.getDependencySet(this, true);
  }

  private Object call(Object arg, ExecutionContext context)
      throws FunctionDomainException, TypeMismatchException {
    Support.Assert(_function == LITERAL_element);
    return Functions.element(arg, context);
  }

  public CompiledValue[] getArguments() {
    return this._args;
  }

  @Override
  public void generateCanonicalizedExpression(StringBuilder clauseBuffer, ExecutionContext context)
      throws AmbiguousNameException, TypeMismatchException, NameResolutionException {
    clauseBuffer.insert(0, ')');
    int len = this._args.length;
    for (int i = len - 1; i > 0; i--) {
      _args[i].generateCanonicalizedExpression(clauseBuffer, context);
      clauseBuffer.insert(0, ',');
    }
    _args[0].generateCanonicalizedExpression(clauseBuffer, context);
    switch (this._function) {
      case LITERAL_nvl:
        clauseBuffer.insert(0, "NVL(");
        break;
      case LITERAL_element:
        clauseBuffer.insert(0, "ELEMENT(");
        break;
      case LITERAL_to_date:
        clauseBuffer.insert(0, "TO_DATE(");
        break;
      default:
        super.generateCanonicalizedExpression(clauseBuffer, context);
    }
  }
}
