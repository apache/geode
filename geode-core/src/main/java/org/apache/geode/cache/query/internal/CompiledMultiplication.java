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

import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;

public class CompiledMultiplication extends CompiledArithmetic implements OQLLexerTokenTypes {

  CompiledMultiplication(CompiledValue left, CompiledValue right, int op) {
    super(left, right, op);
  }

  @Override
  public int getType() {
    return MULTIPLICATION;
  }

  @Override
  public Double evaluateArithmeticOperation(Double n1, Double n2) {
    return n1 * n2;
  }

  @Override
  public Float evaluateArithmeticOperation(Float n1, Float n2) {
    return n1 * n2;
  }

  @Override
  public Long evaluateArithmeticOperation(Long n1, Long n2) {
    return n1 * n2;
  }

  @Override
  public Integer evaluateArithmeticOperation(Integer n1, Integer n2) {
    return n1 * n2;
  }

  @Override
  public Integer evaluateArithmeticOperation(Short n1, Short n2) {
    return n1 * n2;
  }

}
