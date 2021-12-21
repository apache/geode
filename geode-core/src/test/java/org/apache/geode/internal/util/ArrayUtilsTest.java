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
package org.apache.geode.internal.util;

import static org.apache.geode.internal.util.ArrayUtils.asList;
import static org.apache.geode.internal.util.ArrayUtils.getElementAtIndex;
import static org.apache.geode.internal.util.ArrayUtils.getFirst;
import static org.apache.geode.internal.util.ArrayUtils.toByteArray;
import static org.apache.geode.internal.util.ArrayUtils.toBytes;
import static org.apache.geode.internal.util.ArrayUtils.toIntegerArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;


/**
 * Unit tests for {@link ArrayUtils}.
 *
 * @since GemFire 7.x
 */
@SuppressWarnings("null")
public class ArrayUtilsTest {

  @Rule
  public TestName testName = new TestName();

  @Test
  public void testGetElementAtIndex() {
    Object[] arrayOfThree = new Object[] {"test", "testing", "tested"};

    assertThat(getElementAtIndex(arrayOfThree, 0, null)).isEqualTo("test");
    assertThat(getElementAtIndex(arrayOfThree, 1, null)).isEqualTo("testing");
    assertThat(getElementAtIndex(arrayOfThree, 2, null)).isEqualTo("tested");
  }

  @Test
  public void getElementAtIndex_emptyArray_returnsDefaultValue() {
    Object[] emptyArray = new Object[] {};
    String defaultValue = testName.getMethodName();

    assertThat(getElementAtIndex(emptyArray, 0, defaultValue)).isEqualTo(defaultValue);
  }

  @Test
  public void getElementAtIndex_emptyArray_returnsNullDefaultValue() {
    Object[] emptyArray = new Object[] {};

    assertThat(getElementAtIndex(emptyArray, 0, null)).isNull();
  }

  @Test
  public void getElementAtIndex_indexOutOfBounds_returnsDefaultValue() {
    Object[] arrayOfOne = new Object[] {"test"};
    String defaultValue = testName.getMethodName();

    assertThat(getElementAtIndex(arrayOfOne, 2, defaultValue)).isEqualTo(defaultValue);
  }

  @Test
  public void getElementAtIndex_empty_indexOutOfBounds_returnsDefaultValue() {
    Object[] emptyArray = new Object[] {};
    String defaultValue = testName.getMethodName();

    assertThat(getElementAtIndex(emptyArray, 2, defaultValue)).isEqualTo(defaultValue);
  }

  @Test
  public void getFirst_array_returnsFirstElement() {
    assertThat(getFirst("first", "second", "third")).isEqualTo("first");
  }

  @Test
  public void getFirst_arrayContainingNull_returnsFirstElement() {
    assertThat(getFirst("null", "nil", null)).isEqualTo("null");
  }

  @Test
  public void getFirst_oneElement_returnsFirstElement() {
    assertThat(getFirst("test")).isEqualTo("test");
  }

  @Test
  public void getFirst_null_returnsNull() {
    Object nullObject = null;
    assertThat(getFirst(nullObject)).isNull();
  }

  @Test
  public void getFirst_empty_returnsNull() {
    Object[] emptyArray = new Object[0];
    assertThat((Object[]) getFirst(emptyArray)).isNull();
  }

  @Test
  public void getFirst_arrayOfNullValues_returnsNull() {
    assertThat((Object) getFirst(null, null, null)).isNull();
  }

  @Test
  public void toString_returnsOrderedStringInBrackets() {
    Object[] arrayOfThree = new Object[] {"test", "testing", "tested"};

    assertThat(ArrayUtils.toString(arrayOfThree)).isEqualTo("[test, testing, tested]");
  }

  @Test
  public void toString_empty_returnsEmptyBrackets() {
    assertThat(ArrayUtils.toString((new Object[0]))).isEqualTo("[]");
  }

  @Test
  public void toString_null_returnsEmptyBrackets() {
    assertThat(ArrayUtils.toString((Object[]) null)).isEqualTo("[]");
  }

  @Test
  public void toIntegerArray_returnsOrderedArray() {
    int[] sequence = new int[] {0, 1, 2, 4, 8};

    assertThat(toIntegerArray(sequence)).isNotNull().hasSize(sequence.length).containsExactly(0, 1,
        2, 4, 8);
  }

  @Test
  public void toIntegerArray_empty_returnsEmptyArray() {
    assertThat(toIntegerArray(new int[] {})).isNotNull().hasSize(0);
  }

  @Test
  public void toIntegerArray_null_returnsEmptyArray() {
    assertThat(toIntegerArray(null)).isNotNull().hasSize(0);
  }

  @Test
  public void toByteArray_returnsBytes() {
    int count = 0;
    byte[][] array = new byte[10][5];
    for (int i = 0; i < array.length; i++) {
      for (int j = 0; j < array[i].length; j++) {
        array[i][j] = (byte) ++count;
      }
    }
    assertThat(count).isEqualTo(50);

    count = 0;
    Byte[][] byteArray = toByteArray(array);
    for (int i = 0; i < byteArray.length; i++) {
      for (int j = 0; j < byteArray[i].length; j++) {
        assertThat(byteArray[i][j].byteValue()).isEqualTo((byte) ++count);
      }
    }
    assertThat(count).isEqualTo(50);
  }

  @Test
  public void toBytes_returnsPrimitiveBytes() {
    int count = 100;
    Byte[][] byteArray = new Byte[5][10];
    for (int i = 0; i < byteArray.length; i++) {
      for (int j = 0; j < byteArray[i].length; j++) {
        byteArray[i][j] = (byte) --count;
      }
    }
    assertThat(count).isEqualTo(50);

    count = 100;
    byte[][] array = toBytes(byteArray);
    for (int i = 0; i < array.length; i++) {
      for (int j = 0; j < array[i].length; j++) {
        assertThat(array[i][j]).isEqualTo((byte) --count);
      }
    }
    assertThat(count).isEqualTo(50);
  }

  @Test
  public void toByteArray_empty_returnsEmptyBytes() {
    byte[][] array = new byte[0][0];
    assertThat(toByteArray(array)).isEqualTo(new Byte[0][0]);
  }

  @Test
  public void toByteArray_null_returnsNull() {
    byte[][] array = null;
    assertThat(toByteArray(array)).isNull();
  }

  @Test
  public void toBytes_empty_returnsEmpty() {
    Byte[][] byteArray = new Byte[0][0];
    assertThat(toBytes(byteArray)).isEqualTo(new byte[0][0]);
  }

  @Test
  public void toBytes_null_returnsNull() {
    Byte[][] byteArray = null;
    assertThat(toBytes(byteArray)).isNull();
  }

  @Test
  public void asList_returnsModifiableList() throws Exception {
    List<String> modifiable = asList("Larry", "Moe", "Curly");
    assertThat(modifiable.remove("Curly")).isTrue();
    assertThat(modifiable).contains("Larry", "Moe").doesNotContain("Curly");
  }
}
