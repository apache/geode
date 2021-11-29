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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.EnumInfo.PdxInstanceEnumInfo;

public class CompiledInJUnitTest {

  ExecutionContext context;
  CompiledValue elm;
  CompiledValue colln;

  @Before
  public void setup() {
    context = mock(ExecutionContext.class);
    elm = mock(CompiledValue.class);
    colln = mock(CompiledValue.class);
    when(context.getObserver()).thenReturn(new QueryObserverAdapter());
  }

  @Test
  public void testEnumsShouldCompareCorrectlyToACollectionOfOnePdxEnumInfo() throws Exception {
    Object[] objectValues = new Object[] {createPdxInstanceEnumInfo(EnumForTest.ONE, 1)};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(EnumForTest.ONE);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(objectValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEnumsShouldNotCompareCorrectlyIfNotInCollectionOfPdxInstanceEnum()
      throws Exception {
    Object[] objectValues = new Object[] {createPdxInstanceEnumInfo(EnumForTest.ONE, 1)};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(EnumForTest.TWO);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(objectValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEnumsShouldCompareCorrectlyToACollectionOfPdxEnums() throws Exception {
    Object[] objectValues = new Object[] {createPdxInstanceEnumInfo(EnumForTest.ONE, 1),
        createPdxInstanceEnumInfo(EnumForTest.TWO, 1)};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(EnumForTest.ONE);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(objectValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testPdxEnumsShouldCompareCorrectlyToACollectionOfOneEnum() throws Exception {
    Object[] objectValues = new Object[] {EnumForTest.ONE};
    when(elm.evaluate(isA(ExecutionContext.class)))
        .thenReturn(createPdxInstanceEnumInfo(EnumForTest.ONE, 1));
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(objectValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testShouldNotThrowTypeMismatchWithNullElementAndObjectArray() throws Exception {
    Object[] objectValues = new Object[] {true, true};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(null);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(objectValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
  }

  @Test
  public void testTypeMismatchWithNullElementAndPrimitiveArray() throws Exception {
    boolean[] booleanValues = new boolean[] {true, true};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(null);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(booleanValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    try {
      Object result = compiledIn.evaluate(context);
      fail("TypeMismatchException should be thrown");
    } catch (TypeMismatchException e) {

    }
  }

  @Test
  public void testEvaluatesFalseForStringAgainstShortArray() throws Exception {
    short[] shortValues = new short[] {1, 1};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("1");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(shortValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueForFloatAgainstShortArray() throws Exception {
    short[] shortValues = new short[] {1, 1};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(new Float(1.0));
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(shortValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueForShortAgainstShortArray() throws Exception {
    short[] shortValues = new short[] {1, 2};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(shortValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueForIntegerAgainstShortArray() throws Exception {
    short[] shortValues = new short[] {1, 2};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(new Integer(1));
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(shortValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesFalseForStringAgainstBooleanArray() throws Exception {
    boolean[] booleanValues = new boolean[] {true, true};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("true");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(booleanValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesFalseWithBooleanArrayNotMatchingBooleanElement() throws Exception {
    boolean[] booleanValues = new boolean[] {true, true};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(false);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(booleanValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithBooleanArrayMatchingBooleanElement() throws Exception {
    boolean[] booleanValues = new boolean[] {true, true};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(true);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(booleanValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithBooleanArrayMatchingBooleanFalseElement() throws Exception {
    boolean[] booleanValues = new boolean[] {false, false};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(false);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(booleanValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithCharArrayMatchingCharElement() throws Exception {
    char[] charValues = new char[] {'a', 'b', '1'};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn('a');
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(charValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWitCharArrayNotMatchingCharElement() throws Exception {
    char[] charValues = new char[] {'a', 'b', '1'};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn('c');
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(charValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testStringDoesNotMatchCharElements() throws Exception {
    char[] charValues = new char[] {'a', 'b', '1'};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("a");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(charValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testIntegerDoesNotMatchCharElements() throws Exception {
    char[] charValues = new char[] {'a', 'b', '1'};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(97);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(charValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesFalseWithByteArrayNotMatchingIntegerElement() throws Exception {
    byte[] byteValues = new byte[] {127, 2, 3};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(127);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(byteValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesFalseWithByteArrayNotMatchingLargerIntegerElement() throws Exception {
    byte[] byteValues = new byte[] {127, 2, 3};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(128);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(byteValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithByteArrayMatchingByteElement() throws Exception {
    byte[] byteValues = new byte[] {1, 2, 3};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn((Byte.valueOf("1")));
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(byteValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluateNotMatchingLongElement() throws Exception {
    long[] longValues = new long[] {1, 2, 3};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(4L);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithLongArrayMatchingLongElement() throws Exception {
    long[] longValues = new long[] {1, 2, 3};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1L);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithSecondElementOfLongArrayMatchingLongElement() throws Exception {
    long[] longValues = new long[] {1, 2, 3};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(2L);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithLongArrayMatchingAndDoubleElement() throws Exception {
    long[] longValues = new long[] {1, 2, 3};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1D);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithLongArrayMatchingAndIntegerElement() throws Exception {
    long[] longValues = new long[] {1, 2, 3};
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(new Integer(1));
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testExceptionThrownWhenEvaluateAgainstANonCollection() throws Exception {
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn("NotACollection");

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    try {
      Object result = compiledIn.evaluate(context);
      fail("should throw a TypeMismatchException");
    } catch (TypeMismatchException e) {
      // expected
    }
  }

  @Test
  public void testEvaluatesFalseWhenIntegerNotInCollection() throws Exception {
    Collection collection = new ArrayList();
    collection.add(1);
    collection.add(2);
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(3);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(collection);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWhenIntegerInIntegerCollection() throws Exception {
    Collection collection = new ArrayList();
    collection.add(1);
    collection.add(2);
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(collection);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWhenIntegerInSingleElementIntegerCollection() throws Exception {
    Collection collection = new ArrayList();
    collection.add(1);
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(collection);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  // String form
  @Test
  public void testEvaluatesFalseWhenIntegerNotInStringCollection() throws Exception {
    Collection collection = new ArrayList();
    collection.add("1");
    collection.add("2");
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(collection);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesFalseWhenStringNotInStringCollection() throws Exception {
    Collection collection = new ArrayList();
    collection.add("1");
    collection.add("2");
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("3");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(collection);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWhenStringInStringCollection() throws Exception {
    Collection collection = new ArrayList();
    collection.add("1");
    collection.add("2");
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("1");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(collection);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWhenStringInSingleElementStringCollection() throws Exception {
    Collection collection = new ArrayList();
    collection.add("1");
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("1");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(collection);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertTrue((Boolean) result);
  }

  @Test
  public void testCompiledInCanEvaluate() throws Exception {
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(new ArrayList());
    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    assertNotNull(result);
  }

  @Test
  public void whenPassingIntegerArrayToSingleCollectionFilterEvaluateDoesNotThrowTypeMismatchException()
      throws Exception {
    int[] intCollection = new int[] {1, 2};
    when(colln.evaluate(any())).thenReturn(intCollection);
    callSingleCollectionFilterEvaluateBehavior(intCollection);
  }

  @Test
  public void whenPassingLongArrayToSingleCollectionFilterEvaluateDoesNotThrowTypeMismatchException()
      throws Exception {
    long[] longCollection = new long[] {1, 2};
    callSingleCollectionFilterEvaluateBehavior(longCollection);
  }

  @Test
  public void whenPassingDoubleArrayToSingleCollectionFilterEvaluateDoesNotThrowTypeMismatchException()
      throws Exception {
    double[] doubleCollection = new double[] {1, 2};
    callSingleCollectionFilterEvaluateBehavior(doubleCollection);
  }

  @Test
  public void whenPassingCharArrayToSingleCollectionFilterEvaluateDoesNotThrowTypeMismatchException()
      throws Exception {
    char[] charCollection = new char[] {1, 2};
    callSingleCollectionFilterEvaluateBehavior(charCollection);
  }

  @Test
  public void whenPassingFloatArrayToSingleCollectionFilterEvaluateDoesNotThrowTypeMismatchException()
      throws Exception {
    float[] floatCollection = new float[] {1, 2};
    callSingleCollectionFilterEvaluateBehavior(floatCollection);
  }

  @Test
  public void whenPassingShortArrayToSingleCollectionFilterEvaluateDoesNotThrowTypeMismatchException()
      throws Exception {
    short[] shortCollection = new short[] {1, 2};
    callSingleCollectionFilterEvaluateBehavior(shortCollection);
  }

  @Test
  public void whenPassingByteArrayToSingleCollectionFilterEvaluateDoesNotThrowTypeMismatchException()
      throws Exception {
    byte[] byteCollection = new byte[] {1, 2};
    callSingleCollectionFilterEvaluateBehavior(byteCollection);
  }

  @Test
  public void whenPassingObjectArrayToSingleCollectionFilterEvaluateDoesNotThrowTypeMismatchException()
      throws Exception {
    Object[] objectCollection = new Object[] {1, 2};
    callSingleCollectionFilterEvaluateBehavior(objectCollection);
  }

  public void callSingleCollectionFilterEvaluateBehavior(Object collection) throws Exception {
    CompiledIn compiledIn = new CompiledIn(elm, colln);
    when(colln.evaluate(any())).thenReturn(collection);
    IndexInfo indexInfo =
        new IndexInfo(null, null, mock(IndexProtocol.class), 1, new int[] {1}, 90);
    compiledIn.singleBaseCollectionFilterEvaluate(context, new ResultsSet(), false, null, indexInfo,
        null, false, false, false);
  }

  private PdxInstanceEnumInfo createPdxInstanceEnumInfo(Enum<?> e, int enumId) {
    EnumInfo ei = new EnumInfo(e);
    return (PdxInstanceEnumInfo) ei.getPdxInstance(enumId);
  }

  private enum EnumForTest {
    ONE, TWO, THREE
  };
}
