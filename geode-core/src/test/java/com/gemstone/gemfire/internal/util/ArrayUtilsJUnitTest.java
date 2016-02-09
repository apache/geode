/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The ArrayUtilsJUnitTest class... </p>
 *
 * @author jblum
 * @since 7.x
 */
@Category(UnitTest.class)
public class ArrayUtilsJUnitTest {

  @Test
  @SuppressWarnings("null")
  public void testGetElementAtIndex() {
    final Object[] array = { "test", "testing", "tested" };

    assertEquals("test", ArrayUtils.getElementAtIndex(array, 0, null));
    assertEquals("testing", ArrayUtils.getElementAtIndex(array, 1, null));
    assertEquals("tested", ArrayUtils.getElementAtIndex(array, 2, null));
  }

  @Test
  public void testGetElementAtIndexThrowingArrayIndexOutOfBoundsException() {
    assertEquals("test", ArrayUtils.getElementAtIndex(new Object[0], 0, "test"));
  }

  @Test
  public void testGetElementAtIndexThrowingArrayIndexOutOfBoundsExceptionOnNonEmptyArray() {
    assertEquals("defaultValue", ArrayUtils.getElementAtIndex(new Object[] { "test" }, 1, "defaultValue"));
  }

  @Test
  public void testGetFirst() {
    assertEquals("first", ArrayUtils.getFirst("first", "second", "third"));
    assertEquals("null", ArrayUtils.getFirst("null", "nil", null));
    assertEquals("test", ArrayUtils.getFirst("test"));
    assertNull(ArrayUtils.getFirst((Object[]) null));
    assertNull(ArrayUtils.getFirst(new Object[0]));
    assertNull(ArrayUtils.getFirst(null, null, null));
  }

  @Test
  public void testToString() {
    final Object[] array = { "test", "testing", "tested" };

    assertEquals("[test, testing, tested]", ArrayUtils.toString(array));
  }

  @Test
  public void testToStringWithEmptyArray() {
    assertEquals("[]", ArrayUtils.toString((new Object[0])));
  }

  @Test
  public void testToStringWithNullArray() {
    assertEquals("[]", ArrayUtils.toString((Object[]) null));
  }

  @Test
  public void testGetIntegerArray() {
    final Integer[] array = ArrayUtils.toIntegerArray(new int[] { 0, 1, 2, 4, 8 });

    assertNotNull(array);
    assertEquals(5, array.length);
    assertEquals(0, array[0].intValue());
    assertEquals(1, array[1].intValue());
    assertEquals(2, array[2].intValue());
    assertEquals(4, array[3].intValue());
    assertEquals(8, array[4].intValue());
  }

  @Test
  public void testGetIntegerArrayWithEmptyArray() {
    final Integer[] array = ArrayUtils.toIntegerArray(new int[0]);

    assertNotNull(array);
    assertEquals(0, array.length);
  }

  @Test
  @SuppressWarnings("null")
  public void testGetIntegerArrayWithNullArray() {
    final Integer[] array = ArrayUtils.toIntegerArray(null);

    assertNotNull(array);
    assertEquals(0, array.length);
  }

  @Test
  public void testFromBytesToByteArray() {
    int count = 0;
    final byte[][] array = new byte[10][5];
    for (int i = 0; i < array.length; i++) {
      for (int j = 0; j < array[i].length; j++) {
        array[i][j] = (byte)++count;
      }
    }
    assertEquals(50, count);

    count = 0;
    final Byte[][] byteArray = ArrayUtils.toByteArray(array);
    for (int i = 0; i < byteArray.length; i++) {
      for (int j = 0; j < byteArray[i].length; j++) {
        assertEquals((byte)++count, byteArray[i][j].byteValue());
      }
    }
    assertEquals(50, count);
}

  @Test
  public void testFromByteArrayToBytes() {
    int count = 100;
    final Byte[][] byteArray = new Byte[5][10];
    for (int i = 0; i < byteArray.length; i++) {
      for (int j = 0; j < byteArray[i].length; j++) {
        byteArray[i][j] = (byte)--count;
      }
    }
    assertEquals(50, count);
    
    count = 100;
    final byte[][] array = ArrayUtils.toBytes(byteArray);
    for (int i = 0; i < array.length; i++) {
      for (int j = 0; j < array[i].length; j++) {
        assertEquals((byte)--count, array[i][j]);
      }
    }
    assertEquals(50, count);
  }
  
  @Test
  public void testFromEmptyBytesToByteArray() {
    final byte[][] array = new byte[0][0];
    assertArrayEquals(new Byte[0][0], ArrayUtils.toByteArray(array));
  }
  
  @Test
  public void testFromNullBytesToByteArray() {
    final byte[][] array = null;
    assertNull(ArrayUtils.toByteArray(array));
  }
  
  @Test
  public void testFromEmptyByteArrayToBytes() {
    final Byte[][] byteArray = new Byte[0][0];
    assertArrayEquals(new byte[0][0], ArrayUtils.toBytes(byteArray));
  }

  @Test
  public void testFromNullByteArrayToBytes() {
    final Byte[][] byteArray = null;
    assertNull(ArrayUtils.toBytes(byteArray));
  }

}
