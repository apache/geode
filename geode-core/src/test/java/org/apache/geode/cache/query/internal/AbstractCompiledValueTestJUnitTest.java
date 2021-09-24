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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.query.internal.types.CollectionTypeImpl;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class AbstractCompiledValueTestJUnitTest {

  private CompiledValue[] getCompiledValuesWhichDoNotImplementGetReceiver() {
    CompiledValue compiledValue1 = new CompiledID("testString");
    CompiledValue compiledValue2 = new CompiledID("testString");
    return new CompiledValue[] {
        new CompiledAddition(compiledValue1, compiledValue2),
        new CompiledAggregateFunction(compiledValue1, 13),
        new CompiledAddition(compiledValue1, compiledValue2),
        new CompiledBindArgument(1),
        new CompiledComparison(compiledValue1, compiledValue2, 13),
        new CompiledConstruction(Object.class, new ArrayList()),
        new CompiledDivision(compiledValue1, compiledValue2),
        new CompiledFunction(new CompiledValue[] {compiledValue1, compiledValue2}, 13),
        new CompiledGroupBySelect(true, true, compiledValue1, new ArrayList(), new ArrayList(),
            new ArrayList<>(), compiledValue2, new ArrayList<>(), new ArrayList<>(),
            new LinkedHashMap<>()),
        new CompiledIn(compiledValue1, compiledValue2),
        new CompiledIteratorDef("test", new CollectionTypeImpl(), compiledValue1),
        new CompiledJunction(new CompiledValue[] {compiledValue1, compiledValue2}, 89),
        new CompiledLike(compiledValue1, compiledValue2),
        new CompiledLiteral(compiledValue1),
        new CompiledMod(compiledValue1, compiledValue2),
        new CompiledMultiplication(compiledValue1, compiledValue2),
        new CompiledNegation(compiledValue1),
        new CompiledRegion("test"),
        new CompiledSortCriterion(true, compiledValue1),
        new CompiledSubtraction(compiledValue1, compiledValue2),
        new CompiledUnaryMinus(compiledValue1),
        new CompiledUndefined(compiledValue1, true),
        new CompositeGroupJunction(13, compiledValue1),
        new GroupJunction(13, new RuntimeIterator[] {}, true,
            new CompiledValue[] {compiledValue1, compiledValue2})
    };
  }

  private CompiledValue[] getCompiledValuesWhichImplementGetReceiver() {
    CompiledValue compiledValue1 = new CompiledID("testString");
    CompiledValue compiledValue2 = new CompiledID("testString");
    return new CompiledValue[] {
        new CompiledID("testString"),
        new CompiledIndexOperation(compiledValue1, compiledValue2),
        new CompiledOperation(compiledValue1, "test", new ArrayList()),
        new CompiledPath(compiledValue1, "test")
    };
  }

  @Test
  @Parameters(method = "getCompiledValuesWhichDoNotImplementGetReceiver")
  public void whenGetReceiverIsNotImplementedThenHasIdentifierAtLeafMustReturnFalse(
      CompiledValue compiledValue) {
    assertFalse(compiledValue.hasIdentifierAtLeafNode());
  }

  @Test
  @Parameters(method = "getCompiledValuesWhichImplementGetReceiver")
  public void whenGetReceiverIsImplementedThenHasIdentifierAtLeafMustReturnTrue(
      CompiledValue compiledValue) {
    assertTrue(compiledValue.hasIdentifierAtLeafNode());
  }

  @Test
  public void whenLeafIsIdentifierAtTheLeafThenHasIdentifierAtLeafMustReturnTrue() {
    CompiledValue compiledValue1 = new CompiledID("testString");
    CompiledValue compiledValue2 = new CompiledID("testString");
    CompiledIndexOperation compiledIndexOperation =
        new CompiledIndexOperation(compiledValue1, compiledValue2);
    CompiledPath compiledPath = new CompiledPath(compiledIndexOperation, "test");
    assertTrue(compiledPath.hasIdentifierAtLeafNode());
  }

  @Test
  public void whenLeafIsNotIndentifierThenHasIdentifierAtLeafMustReturnFalse() {
    CompiledValue compiledValue1 = new CompiledBindArgument(1);
    CompiledValue compiledValue2 = new CompiledBindArgument(1);
    CompiledIndexOperation compiledIndexOperation =
        new CompiledIndexOperation(compiledValue1, compiledValue2);
    CompiledPath compiledPath = new CompiledPath(compiledIndexOperation, "test");
    assertFalse(compiledPath.hasIdentifierAtLeafNode());
  }

}
