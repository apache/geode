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
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.pdx.PdxInstance;

/**
 * Comparison value: <, >, <=, >=, <>, =
 *
 */
public abstract class CompiledArithmetic extends AbstractCompiledValue
    implements OQLLexerTokenTypes {

  // persistent inst vars
  public final CompiledValue _left;
  public final CompiledValue _right;

  CompiledArithmetic(CompiledValue left, CompiledValue right) {
    // invariant:
    _left = left;
    _right = right;
  }

  /* ******** CompiledValue Methods **************** */
  @Override
  public List getChildren() {
    List list = new ArrayList();
    list.add(_left);
    list.add(_right);
    return list;
  }

  @Override
  public abstract int getType();

  @Override
  public Object evaluate(ExecutionContext context) throws FunctionDomainException,
      TypeMismatchException, NameResolutionException, QueryInvocationTargetException {
    Object left = _left.evaluate(context);
    Object right = _right.evaluate(context);

    if (context.isCqQueryContext() && left instanceof Region.Entry) {
      left = ((Region.Entry) left).getValue();
    }
    if (context.isCqQueryContext() && right instanceof Region.Entry) {
      right = ((Region.Entry) right).getValue();
    }

    if (left == null || right == null) {
      throw new TypeMismatchException("Cannot evaluate arithmetic operations on null values");
    }

    // if read-serialized is not set, deserialize the pdx instance for comparison
    // only if it is of the same class as that of the other object
    if (!context.getCache().getPdxReadSerialized()) {
      if (left instanceof PdxInstance && !(right instanceof PdxInstance)
          && ((PdxInstance) left).getClassName().equals(right.getClass().getName())) {
        left = ((PdxInstance) left).getObject();
      } else if (right instanceof PdxInstance && !(left instanceof PdxInstance)
          && ((PdxInstance) right).getClassName().equals(left.getClass().getName())) {
        right = ((PdxInstance) right).getObject();
      }
    }

    if (!(left instanceof Number && right instanceof Number)) {
      throw new TypeMismatchException("Arithmetic Operations can only be applied to numbers");
    }

    Number num1 = (Number) left;
    Number num2 = (Number) right;
    try {
      if (num1 instanceof Double) {
        if (num2 instanceof Double) {
          return evaluateArithmeticOperation((Double) num1, ((Double) num2));
        } else {
          return evaluateArithmeticOperation((Double) num1, num2.doubleValue());
        }
      } else if (num2 instanceof Double) {
        return evaluateArithmeticOperation(num1.doubleValue(), (Double) num2);
      }

      if (num1 instanceof Float) {
        if (num2 instanceof Float) {
          return evaluateArithmeticOperation((Float) num1, (Float) num2);
        } else {
          return evaluateArithmeticOperation((Float) num1, new Float(num2.doubleValue()));
        }
      } else if (num2 instanceof Float) {
        return evaluateArithmeticOperation(new Float(num1.doubleValue()), (Float) num2);
      }

      if (num1 instanceof Long) {
        if (num2 instanceof Long) {
          return evaluateArithmeticOperation((Long) num1, ((Long) num2));
        } else {
          long l1 = num1.longValue();
          long l2 = num2.longValue();
          return evaluateArithmeticOperation(l1, l2);
        }
      } else if (num2 instanceof Long) {
        long l1 = num1.longValue();
        long l2 = num2.longValue();
        return evaluateArithmeticOperation(l1, l2);
      }

      if (num1 instanceof Integer) {
        if (num2 instanceof Integer) {
          return evaluateArithmeticOperation((Integer) num1, (Integer) num2);
        } else {
          int i1 = num1.intValue();
          int i2 = num2.intValue();
          return evaluateArithmeticOperation(i1, i2);
        }
      } else if (num2 instanceof Integer) {
        int i1 = num1.intValue();
        int i2 = num2.intValue();
        return evaluateArithmeticOperation(i1, i2);
      }

      if (num1 instanceof Short) {
        if (num2 instanceof Short) {
          return evaluateArithmeticOperation((Short) num1, ((Short) num2));
        } else {
          short s1 = num1.shortValue();
          short s2 = num2.shortValue();
          return evaluateArithmeticOperation(s1, s2);
        }
      } else if (num2 instanceof Short) {
        short s1 = num1.shortValue();
        short s2 = num2.shortValue();
        return evaluateArithmeticOperation(s1, s2);
      }
    } catch (ArithmeticException e) {
      throw new QueryInvocationTargetException(e);
    }
    throw new TypeMismatchException("Unable to apply arithmetic operation on non number");
  }

  @Override
  public Set computeDependencies(ExecutionContext context)
      throws TypeMismatchException, NameResolutionException {
    context.addDependencies(this, _left.computeDependencies(context));
    return context.addDependencies(this, _right.computeDependencies(context));
  }

  public abstract Double evaluateArithmeticOperation(Double n1, Double n2);

  public abstract Float evaluateArithmeticOperation(Float n1, Float n2);

  public abstract Long evaluateArithmeticOperation(Long n1, Long n2);

  public abstract Integer evaluateArithmeticOperation(Integer n1, Integer n2);

  public abstract Integer evaluateArithmeticOperation(Short n1, Short n2);

}
