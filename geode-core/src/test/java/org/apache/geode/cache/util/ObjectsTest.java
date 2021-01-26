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

package org.apache.geode.cache.util;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.TimeZone;
import java.util.Vector;

import org.junit.Test;

/**
 * The test serves two purposes:
 * <p>
 * Since Geode hashes rely heavily on the Java implementation hashes. If those were to differ
 * between version it could pose difficulties in routing or finding keys on the correct servers.
 * <p>
 * It also serves as a comparison for hashing in other languages that need to match the Java hash
 * algorithm for specific types.
 */
public class ObjectsTest {
  @Test
  public void hashCodeOfNullIs0() {
    assertThat(Objects.hashCode(null)).isEqualTo(0);
  }

  @Test
  public void hashOfEmptyArgsIs1() {
    assertThat(Objects.hash()).isEqualTo(1);
  }

  @Test
  public void hashOfOneArgumentRelativeToHashCodeOfArgumentPlus31() {
    final String value = "some string";
    assertThat(Objects.hash(value)).isEqualTo(Objects.hashCode(value) + 31);
  }

  @Test
  public void hashCodeString() {
    assertThat(Objects.hashCode("")).isEqualTo(0);
    assertThat(Objects.hashCode("0")).isEqualTo(48);
    assertThat(Objects.hashCode("9")).isEqualTo(57);
    assertThat(Objects.hashCode("a")).isEqualTo(97);
    assertThat(Objects.hashCode("z")).isEqualTo(122);
    assertThat(Objects.hashCode("A")).isEqualTo(65);
    assertThat(Objects.hashCode("Z")).isEqualTo(90);

    assertThat(Objects.hashCode("supercalifragilisticexpialidocious")).isEqualTo(1077910243);

    assertThat(Objects.hashCode("You had me at meat tornad\u00F6!\uDB80\uDC00"))
        .isEqualTo(1544552287);

    assertThat(Objects.hashCode("You had me at\0meat tornad\u00F6!\uDB80\uDC00"))
        .isEqualTo(701776767);

    assertThat(Objects.hashCode(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor "
            + "incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud "
            + "exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute "
            + "irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
            + "pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia "
            + "deserunt mollit anim id est laborum."))
                .isEqualTo(512895612);

    assertThat(Objects.hashCode(
        "\u16bb\u16d6\u0020\u16b3\u16b9\u16ab\u16a6\u0020\u16a6\u16ab\u16cf\u0020\u16bb\u16d6"
            + "\u0020\u16d2\u16a2\u16de\u16d6\u0020\u16a9\u16be\u0020\u16a6\u16ab\u16d7\u0020\u16da"
            + "\u16aa\u16be\u16de\u16d6\u0020\u16be\u16a9\u16b1\u16a6\u16b9\u16d6\u16aa\u16b1\u16de"
            + "\u16a2\u16d7\u0020\u16b9\u16c1\u16a6\u0020\u16a6\u16aa\u0020\u16b9\u16d6\u16e5\u16ab"))
                .isEqualTo(-1425027716);
  }

  @Test
  public void HashOf1String() {
    assertThat(Objects.hash("Z")).isEqualTo(121);
  }

  @Test
  public void hashCodeOfChar() {
    assertThat(Objects.hashCode('0')).isEqualTo(48);
    assertThat(Objects.hashCode('9')).isEqualTo(57);
    assertThat(Objects.hashCode('a')).isEqualTo(97);
    assertThat(Objects.hashCode('z')).isEqualTo(122);
    assertThat(Objects.hashCode('A')).isEqualTo(65);
    assertThat(Objects.hashCode('Z')).isEqualTo(90);
    assertThat(Objects.hashCode('\u16bb')).isEqualTo(5819);
  }

  @Test
  public void hashOf1Char() {
    assertThat(Objects.hash('Z')).isEqualTo(121);
  }

  @Test
  public void hashCodeOfBoolean() {
    assertThat(Objects.hashCode(false)).isEqualTo(1237);
    assertThat(Objects.hashCode(true)).isEqualTo(1231);
  }

  @Test
  public void hashOf1Boolean() {
    assertThat(Objects.hash(true)).isEqualTo(1262);
  }

  @Test
  public void hashCodeOfByte() {
    assertThat(Objects.hashCode((byte) 0)).isEqualTo(0);
    assertThat(Objects.hashCode((byte) 1)).isEqualTo(1);
    assertThat(Objects.hashCode((byte) -1)).isEqualTo(-1);
    assertThat(Objects.hashCode(Byte.MAX_VALUE)).isEqualTo(Byte.MAX_VALUE);
    assertThat(Objects.hashCode(Byte.MIN_VALUE)).isEqualTo(Byte.MIN_VALUE);
  }

  @Test
  public void hashOf1Byte() {
    assertThat(Objects.hash(Byte.MAX_VALUE)).isEqualTo(158);
  }

  @Test
  public void hashCodeOfInt16() {
    assertThat(Objects.hashCode((short) 0)).isEqualTo(0);
    assertThat(Objects.hashCode((short) 1)).isEqualTo(1);
    assertThat(Objects.hashCode((short) -1)).isEqualTo(-1);
    assertThat(Objects.hashCode(Short.MAX_VALUE)).isEqualTo(Short.MAX_VALUE);
    assertThat(Objects.hashCode(Short.MIN_VALUE)).isEqualTo(Short.MIN_VALUE);
  }

  @Test
  public void hashOf1Int16() {
    assertThat(Objects.hash(Short.MAX_VALUE)).isEqualTo(32798);
  }

  @Test
  public void hashCodeOfInt32() {
    assertThat(Objects.hashCode(0)).isEqualTo(0);
    assertThat(Objects.hashCode(1)).isEqualTo(1);
    assertThat(Objects.hashCode(-1)).isEqualTo(-1);
    assertThat(Objects.hashCode(Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    assertThat(Objects.hashCode(Integer.MIN_VALUE)).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  public void hashOf1Int32() {
    assertThat(Objects.hash(0)).isEqualTo(31);
    assertThat(Objects.hash(1)).isEqualTo(32);
    assertThat(Objects.hash(Integer.MAX_VALUE)).isEqualTo(-2147483618);
    assertThat(Objects.hash(Integer.MIN_VALUE)).isEqualTo(-2147483617);
  }

  @Test
  public void HashCodeOf2Int32() {
    assertThat(Objects.hash(0, 1)).isEqualTo(962);
    assertThat(Objects.hash(1, 0)).isEqualTo(992);
    assertThat(Objects.hash(Integer.MAX_VALUE, Integer.MIN_VALUE)).isEqualTo(930);
    assertThat(Objects.hash(Integer.MIN_VALUE, Integer.MAX_VALUE)).isEqualTo(960);
  }

  @Test
  public void HashCodeOf4Int32() {
    assertThat(Objects.hash(0, 1, 2, 3)).isEqualTo(924547);
    assertThat(Objects.hash(1, 0, -1, -2)).isEqualTo(953279);
  }

  @Test
  public void hashCodeOfInt64() {
    assertThat(Objects.hashCode(0L)).isEqualTo(0);
    assertThat(Objects.hashCode(1L)).isEqualTo(1);
    assertThat(Objects.hashCode(-1L)).isEqualTo(0);
    assertThat(Objects.hashCode((long) Integer.MAX_VALUE)).isEqualTo(2147483647);
    assertThat(Objects.hashCode((long) Integer.MIN_VALUE)).isEqualTo(2147483647);
    assertThat(Objects.hashCode(((long) Integer.MAX_VALUE) + 1)).isEqualTo(-2147483648);
    assertThat(Objects.hashCode(((long) Integer.MIN_VALUE) - 1)).isEqualTo(-2147483648);
    assertThat(Objects.hashCode(Long.MAX_VALUE)).isEqualTo(-2147483648);
    assertThat(Objects.hashCode(Long.MIN_VALUE)).isEqualTo(-2147483648);
    assertThat(Objects.hashCode(Long.MAX_VALUE >> 1)).isEqualTo(-1073741824);
    assertThat(Objects.hashCode(Long.MIN_VALUE >> 1)).isEqualTo(-1073741824);
    assertThat(Objects.hashCode(Long.MAX_VALUE >> 2)).isEqualTo(-536870912);
    assertThat(Objects.hashCode(Long.MIN_VALUE >> 2)).isEqualTo(-536870912);
    assertThat(Objects.hashCode(-9223372034707292160L)).isEqualTo(0);

    assertThat(Objects.hashCode(1577836800000L)).isEqualTo(1583802735);

  }

  @Test
  public void hashOf1Int64() {
    assertThat(Objects.hash(Long.MAX_VALUE)).isEqualTo(-2147483617);
  }

  @Test
  public void hashCodeOfFloat() {
    assertThat(Objects.hashCode(0.0f)).isEqualTo(0);
    assertThat(Objects.hashCode(-0.0f)).isEqualTo(-2147483648);
    assertThat(Objects.hashCode(1.0f)).isEqualTo(1065353216);
    assertThat(Objects.hashCode(-1.0f)).isEqualTo(-1082130432);
    assertThat(Objects.hashCode(Float.MAX_VALUE)).isEqualTo(2139095039);
    assertThat(Objects.hashCode(Float.MAX_VALUE * -1)).isEqualTo(-8388609);
    assertThat(Objects.hashCode(Float.MIN_VALUE)).isEqualTo(1);
    assertThat(Objects.hashCode(Float.POSITIVE_INFINITY)).isEqualTo(2139095040);
    assertThat(Objects.hashCode(Float.NEGATIVE_INFINITY)).isEqualTo(-8388608);
    assertThat(Objects.hashCode(Float.MIN_NORMAL)).isEqualTo(8388608);
    assertThat(Objects.hashCode(Float.NaN)).isEqualTo(2143289344);
  }

  @Test
  public void hashOf1Float() {
    assertThat(Objects.hash(Float.MAX_VALUE)).isEqualTo(2139095070);
  }

  @Test
  public void hashCodeOfDouble() {
    assertThat(Objects.hashCode(0.0d)).isEqualTo(0);
    assertThat(Objects.hashCode(-0.0d)).isEqualTo(-2147483648);
    assertThat(Objects.hashCode(1.0d)).isEqualTo(1072693248);
    assertThat(Objects.hashCode(-1.0d)).isEqualTo(-1074790400);
    assertThat(Objects.hashCode(Double.MAX_VALUE)).isEqualTo(-2146435072);
    assertThat(Objects.hashCode(Double.MAX_VALUE * -1)).isEqualTo(1048576);
    assertThat(Objects.hashCode(Double.MIN_VALUE)).isEqualTo(1);
    assertThat(Objects.hashCode(Double.POSITIVE_INFINITY)).isEqualTo(2146435072);
    assertThat(Objects.hashCode(Double.NEGATIVE_INFINITY)).isEqualTo(-1048576);
    assertThat(Objects.hashCode(Double.MIN_NORMAL)).isEqualTo(1048576);
    assertThat(Objects.hashCode(Double.NaN)).isEqualTo(2146959360);
  }

  @Test
  public void hashOf1Double() {
    assertThat(Objects.hash(Double.MAX_VALUE)).isEqualTo(-2146435041);
  }

  @Test
  public void hashCodeOfDate() {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar.setTimeInMillis(0);
    calendar.set(1970, 0, 1, 0, 0, 0);
    assertThat(Objects.hashCode(calendar.getTime())).isEqualTo(0);
    calendar.set(2020, 0, 1, 0, 0, 0);
    assertThat(Objects.hashCode(calendar.getTime())).isEqualTo(1583802735);
    calendar.set(3020, 0, 1, 0, 0, 0);
    assertThat(Objects.hashCode(calendar.getTime())).isEqualTo(-927080926);
    calendar.set(1920, 0, 1, 0, 0, 0);
    assertThat(Objects.hashCode(calendar.getTime())).isEqualTo(1670202000);
    calendar.set(1820, 0, 1, 0, 0, 0);
    assertThat(Objects.hashCode(calendar.getTime())).isEqualTo(542840753);
  }

  @Test
  public void hashOf1Date() {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar.setTimeInMillis(0);
    calendar.set(1970, 0, 1, 0, 0, 0);
    assertThat(Objects.hash(calendar.getTime())).isEqualTo(31);
  }

  @Test
  public void GetHashCodeOfArrays() {
    assertThat(Arrays.hashCode(new byte[] {1, 2, 3, 4})).isEqualTo(955331);
    assertThat(Arrays.hashCode(new short[] {1, 2, 3, 4})).isEqualTo(955331);
    assertThat(Arrays.hashCode(new int[] {1, 2, 3, 4})).isEqualTo(955331);
    assertThat(Arrays.hashCode(new long[] {1L, 2L, 3L, 4L})).isEqualTo(955331);
    assertThat(Arrays.hashCode(new float[] {0.0f, 1.0f, -1.0f, Float.NaN})).isEqualTo(265164673);
    assertThat(Arrays.hashCode(new double[] {0.0d, 1.0d, -1.0d, Double.NaN}))
        .isEqualTo(-1039788159);
    assertThat(Arrays.hashCode(new String[] {"a", "b", "c", "d"})).isEqualTo(3910595);
    assertThat(Arrays.hashCode(new char[] {'a', 'b', 'c', 'd'})).isEqualTo(3910595);
  }

  @Test
  public void hashCodeOfList() {
    final ArrayList<Integer> arrayList = new ArrayList<>(asList(1, 2, 3, 4));
    assertThat(Objects.hashCode(arrayList)).isEqualTo(955331);
    final LinkedList<Integer> linkedList = new LinkedList<>(asList(1, 2, 3, 4));
    assertThat(Objects.hashCode(linkedList)).isEqualTo(955331);
    final Vector<Integer> vector = new Vector<>(asList(1, 2, 3, 4));
    assertThat(Objects.hashCode(vector)).isEqualTo(955331);
    final Stack<Integer> stack = new Stack<>();
    stack.push(1);
    stack.push(2);
    stack.push(3);
    stack.push(4);
    assertThat(Objects.hashCode(stack)).isEqualTo(955331);
  }

  @Test
  public void hashCodeOfMap() {
    Map<Integer, Integer> map = new HashMap<Integer, Integer>() {
      {
        put(1, 2);
        put(3, 4);
      }
    };
    assertThat(Objects.hashCode(map)).isEqualTo(10);
  }

  @Test
  public void HashOfMultipleTypes() {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    calendar.setTimeInMillis(0);
    calendar.set(1970, 0, 1, 0, 0, 0);
    assertThat(Objects
        .hash(calendar.getTime(), true, Byte.MAX_VALUE, Short.MIN_VALUE, Integer.MAX_VALUE,
            Long.MIN_VALUE, Float.POSITIVE_INFINITY, Double.MAX_VALUE, 'C', "a string"))
                .isEqualTo(-1009437857);
  }

  static class CustomKey {
    private int a;
    private double b;
    private String c;

    public CustomKey(int a, double b, String c) {
      this.a = a;
      this.b = b;
      this.c = c;
    }

    @Override
    public boolean equals(final Object o) {
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(a, b, c);
    }
  };

  @Test
  public void hashOfCustomKey() {
    assertThat(Objects.hashCode(new CustomKey(1, 2.0, "key"))).isEqualTo(-1073604993);
  }


}
