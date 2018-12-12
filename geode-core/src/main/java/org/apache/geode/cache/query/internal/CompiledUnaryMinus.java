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

public class CompiledUnaryMinus extends AbstractCompiledValue {

  private CompiledValue _value;

  public CompiledUnaryMinus(CompiledValue value) {
    _value = value;
  }


  @Override
  public List getChildren() {
    return Collections.singletonList(this._value);
  }

  public int getType() {
    return LITERAL_sum;
  }

  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    return minus(_value.evaluate(context));
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, AmbiguousNameException, NameResolutionException {
    return context.addDependencies(this, this._value.computeDependencies(context));
  }

  private Object minus(Object obj) throws TypeMismatchException {

    if (obj instanceof Number) {
      if (obj instanceof Integer)
        return Integer.valueOf(((Integer) obj).intValue() * -1);
      if (obj instanceof Long)
        return Long.valueOf(((Long) obj).longValue() * -1);
      if (obj instanceof Double)
        return Double.valueOf(((Double) obj).doubleValue() * -1);
      if (obj instanceof Float)
        return Float.valueOf(((Float) obj).floatValue() * -1);
      if (obj instanceof Byte)
        return Byte.valueOf((byte) (((Byte) obj).byteValue() * -1));
      if (obj instanceof Short)
        return Short.valueOf((short) (((Short) obj).shortValue() * -1));
    } else if (obj == null || obj == QueryService.UNDEFINED)
      return QueryService.UNDEFINED;
    throw new TypeMismatchException(String.format("%s cannot be unary minus",
        obj.getClass()));
  }

}
