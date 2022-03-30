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
package org.apache.geode.cache.execute;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.condition.JRE.JAVA_14;
import static org.junit.jupiter.api.condition.JRE.JAVA_15;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;

class FunctionTest {
  @Test
  void forFunctionWithNonLocalConcreteClass_defaultIdIsClassCanonicalName() {
    Function<Object> function = new NonLocalConcreteFunctionClass();

    assertThat(function.getId())
        .isEqualTo(NonLocalConcreteFunctionClass.class.getCanonicalName());
  }

  // Before Java 15, the class of a lambda expression has a canonical name.
  @EnabledForJreRange(max = JAVA_14)
  @Test
  void forFunctionWithNonHiddenLambdaClass_defaultIdIsClassCanonicalName() {
    Function<Object> function = context -> {
    };

    assertThat(function.getId())
        .isEqualTo(function.getClass().getCanonicalName());
  }

  // An anonymous class does not have a canonical name.
  @Test
  void forFunctionWithAnonymousClass_defaultIdIsClassName() {
    Function<Object> function = new Function<Object>() {
      @Override
      public void execute(FunctionContext<Object> context) {}
    };

    assertThat(function.getId())
        .isEqualTo(function.getClass().getName());
  }

  // A local class does not have a canonical name.
  @Test
  void forFunctionWithLocalConcreteClass_defaultIdIsClassName() {
    class LocalConcreteFunctionClass implements Function<Object> {
      @Override
      public void execute(FunctionContext<Object> context) {}
    }
    Function<Object> function = new LocalConcreteFunctionClass();

    assertThat(function.getId())
        .isEqualTo(LocalConcreteFunctionClass.class.getName());
  }

  // Starting with Java 15, some classes (including those of lambda expressions) are hidden.
  // A hidden class does not have a canonical name.
  @EnabledForJreRange(min = JAVA_15)
  @Test
  void forFunctionWithHiddenClass_defaultIdIsClassName() {
    Function<Object> function = context -> {
    };

    assertThat(function.getId())
        .isEqualTo(function.getClass().getName());
  }

  static class NonLocalConcreteFunctionClass implements Function<Object> {
    @Override
    public void execute(FunctionContext<Object> context) {}
  }
}
