package com.gemstone.gemfire.cache.query.internal;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CompiledInJUnitTest {

  ExecutionContext context;
  CompiledValue elm;
  CompiledValue colln;

  @Before
  public void setup() {
    context = mock(ExecutionContext.class);
    elm = mock(CompiledValue.class);
    colln = mock(CompiledValue.class);
  }

  @Test
  public void testShouldNotThrowTypeMismatchWithNullElementAndObjectArray() throws Exception {
    Object[] objectValues = new Object[] { true, true };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(null);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(objectValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
  }

  @Test
  public void testTypeMismatchWithNullElementAndPrimitiveArray() throws Exception {
    boolean[] booleanValues = new boolean[] { true, true };
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
    short[] shortValues = new short[] { 1, 1 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("1");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(shortValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueForFloatAgainstShortArray() throws Exception {
    short[] shortValues = new short[] { 1, 1 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(new Float(1.0));
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(shortValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueForShortAgainstShortArray() throws Exception {
    short[] shortValues = new short[] { 1, 2 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(shortValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueForIntegerAgainstShortArray() throws Exception {
    short[] shortValues = new short[] { 1, 2 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(new Integer(1));
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(shortValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesFalseForStringAgainstBooleanArray() throws Exception {
    boolean[] booleanValues = new boolean[] { true, true };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("true");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(booleanValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesFalseWithBooleanArrayNotMatchingBooleanElement() throws Exception {
    boolean[] booleanValues = new boolean[] { true, true };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(false);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(booleanValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithBooleanArrayMatchingBooleanElement() throws Exception {
    boolean[] booleanValues = new boolean[] { true, true };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(true);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(booleanValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithBooleanArrayMatchingBooleanFalseElement() throws Exception {
    boolean[] booleanValues = new boolean[] { false, false };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(false);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(booleanValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithCharArrayMatchingCharElement() throws Exception {
    char[] charValues = new char[] { 'a', 'b', '1' };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn('a');
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(charValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWitCharArrayNotMatchingCharElement() throws Exception {
    char[] charValues = new char[] { 'a', 'b', '1' };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn('c');
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(charValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertFalse((Boolean) result);
  }

  @Test
  public void testStringDoesNotMatchCharElements() throws Exception {
    char[] charValues = new char[] { 'a', 'b', '1' };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("a");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(charValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertFalse((Boolean) result);
  }

  @Test
  public void testIntegerDoesNotMatchCharElements() throws Exception {
    char[] charValues = new char[] { 'a', 'b', '1' };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(97);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(charValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesFalseWithByteArrayNotMatchingIntegerElement() throws Exception {
    byte[] byteValues = new byte[] { 127, 2, 3 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(127);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(byteValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesFalseWithByteArrayNotMatchingLargerIntegerElement() throws Exception {
    byte[] byteValues = new byte[] { 127, 2, 3 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(128);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(byteValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithByteArrayMatchingByteElement() throws Exception {
    byte[] byteValues = new byte[] { 1, 2, 3 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn((Byte.valueOf("1")));
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(byteValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluateNotMatchingLongElement() throws Exception {
    long[] longValues = new long[] { 1, 2, 3 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(4L);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertFalse((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithLongArrayMatchingLongElement() throws Exception {
    long[] longValues = new long[] { 1, 2, 3 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1L);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithSecondElementOfLongArrayMatchingLongElement() throws Exception {
    long[] longValues = new long[] { 1, 2, 3 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(2L);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithLongArrayMatchingAndDoubleElement() throws Exception {
    long[] longValues = new long[] { 1, 2, 3 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1D);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWithLongArrayMatchingAndIntegerElement() throws Exception {
    long[] longValues = new long[] { 1, 2, 3 };
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(new Integer(1));
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(longValues);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
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
    Assert.assertFalse((Boolean) result);
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
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWhenIntegerInSingleElementIntegerCollection() throws Exception {
    Collection collection = new ArrayList();
    collection.add(1);
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn(1);
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(collection);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
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
    Assert.assertFalse((Boolean) result);
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
    Assert.assertFalse((Boolean) result);
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
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testEvaluatesTrueWhenStringInSingleElementStringCollection() throws Exception {
    Collection collection = new ArrayList();
    collection.add("1");
    when(elm.evaluate(isA(ExecutionContext.class))).thenReturn("1");
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(collection);

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertTrue((Boolean) result);
  }

  @Test
  public void testCompiledInCanEvaluate() throws Exception {
    when(colln.evaluate(isA(ExecutionContext.class))).thenReturn(new ArrayList());

    CompiledIn compiledIn = new CompiledIn(elm, colln);
    Object result = compiledIn.evaluate(context);
    Assert.assertNotNull(result);
  }
}
