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
 *
 */

package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.netty.Coder.longToBytes;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;

import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.ReflectionObjectSizer;

public class RedisStringTest {
  private final ReflectionObjectSizer reflectionObjectSizer = ReflectionObjectSizer.getInstance();

  @BeforeClass
  public static void beforeClass() {
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_STRING_ID,
        RedisString.class);
  }

  @Test
  public void constructorSetsValue() {
    byte[] bytes = {0, 1, 2};
    RedisString string = new RedisString(bytes);
    byte[] returnedBytes = string.get();
    assertThat(returnedBytes).isNotNull();
    assertThat(returnedBytes).isEqualTo(bytes);
  }

  @Test
  public void setSetsValue() {
    RedisString string = new RedisString();
    byte[] bytes = {0, 1, 2};
    string.set(bytes);
    byte[] returnedBytes = string.get();
    assertThat(returnedBytes).isNotNull();
    assertThat(returnedBytes).isEqualTo(bytes);
  }

  @Test
  public void getReturnsSetValue() {
    byte[] bytes = {0, 1};
    RedisString string = new RedisString(bytes);
    byte[] returnedBytes = string.get();
    assertThat(returnedBytes).isNotNull();
    assertThat(returnedBytes).isEqualTo(bytes);
  }

  @Test
  public void getsetSetsValueAndReturnsOldValue() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] oldBytes = {0, 1};
    byte[] newBytes = {0, 1, 2};
    RedisString string = new RedisString(oldBytes);
    byte[] returnedBytes = string.getset(region, null, newBytes);
    assertThat(returnedBytes).isNotNull();
    assertThat(returnedBytes).isEqualTo(oldBytes);
    assertThat(string.get()).isNotNull();
    assertThat(string.get()).isEqualTo(newBytes);
  }

  @Test
  public void appendResizesByteArray() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    RedisString redisString = new RedisString(new byte[] {0, 1});
    int redisStringSize = redisString.strlen();
    byte[] bytesToAppend = {2, 3, 4, 5};
    int appendedSize = bytesToAppend.length;
    int appendedStringSize = redisString.append(region, null, bytesToAppend);
    assertThat(appendedStringSize).isEqualTo(redisStringSize + appendedSize);
  }

  @Test
  public void appendStoresStableDelta() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final byte[] baseBytes = {'0', '1'};
    final byte[] bytesToAppend = {'2', '3'};

    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(baseBytes, invocation));
    RedisString stringOne = new RedisString(baseBytes);

    stringOne.append(region, null, bytesToAppend);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    RedisString stringOne = new RedisString(new byte[] {0, 1, 2, 3});
    int expirationTimestamp = 1000;
    stringOne.setExpirationTimestampNoDelta(expirationTimestamp);
    HeapDataOutputStream outputStream = new HeapDataOutputStream(100);
    DataSerializer.writeObject(stringOne, outputStream);
    ByteArrayDataInput dataInput = new ByteArrayDataInput(outputStream.toByteArray());
    RedisString stringTwo = DataSerializer.readObject(dataInput);
    assertThat(stringTwo).isEqualTo(stringOne);
    assertThat(stringTwo.getExpirationTimestamp())
        .isEqualTo(stringOne.getExpirationTimestamp())
        .isEqualTo(expirationTimestamp);
  }

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier
        .isSynchronized(RedisString.class
            .getMethod("toData", DataOutput.class, SerializationContext.class).getModifiers()))
                .isTrue();
  }

  @Test
  public void incrThrowsArithmeticErrorWhenNotALong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = stringToBytes("10 1");
    RedisString string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incr(region, null)).isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void incrErrorsWhenValueOverflows() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = longToBytes(Long.MAX_VALUE);
    RedisString string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incr(region, null)).isInstanceOf(ArithmeticException.class);
  }

  @Test
  public void incrIncrementsValueAtGivenKey() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = stringToBytes("10");
    RedisString string = new RedisString(bytes);
    string.incr(region, null);
    assertThat(string.get()).isEqualTo(stringToBytes("11"));
  }

  @Test
  public void incrbyThrowsNumberFormatExceptionWhenNotALong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = stringToBytes("10 1");
    RedisString string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incrby(region, null, 2L))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void incrbyErrorsWhenValueOverflows() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = longToBytes(Long.MAX_VALUE);
    RedisString string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incrby(region, null, 2L))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  public void incrbyIncrementsValueByGivenLong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = stringToBytes("10");
    RedisString string = new RedisString(bytes);
    string.incrby(region, null, 2L);
    assertThat(string.get()).isEqualTo(stringToBytes("12"));
  }

  @Test
  public void incrbyfloatThrowsArithmeticErrorWhenNotADouble() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = stringToBytes("10 1");
    RedisString string = new RedisString(bytes);
    assertThatThrownBy(() -> string.incrbyfloat(region, null, new BigDecimal("1.1")))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void incrbyfloatIncrementsValueByGivenFloat() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = stringToBytes("10");
    RedisString string = new RedisString(bytes);
    string.incrbyfloat(region, null, new BigDecimal("2.20"));
    assertThat(string.get()).isEqualTo(stringToBytes("12.20"));
  }

  @Test
  public void decrThrowsNumberFormatExceptionWhenNotALong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = {0};
    RedisString string = new RedisString(bytes);
    assertThatThrownBy(() -> string.decr(region, null)).isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void decrThrowsArithmeticExceptionWhenDecrementingMin() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = longToBytes(Long.MIN_VALUE);
    RedisString string = new RedisString(bytes);
    assertThatThrownBy(() -> string.decr(region, null)).isInstanceOf(ArithmeticException.class);
  }

  @Test
  public void decrDecrementsValue() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = stringToBytes("10");
    RedisString string = new RedisString(bytes);
    string.decr(region, null);
    assertThat(string.get()).isEqualTo(stringToBytes("9"));
  }

  @Test
  public void decrbyThrowsNumberFormatExceptionWhenNotALong() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = {1};
    RedisString string = new RedisString(bytes);
    assertThatThrownBy(() -> string.decrby(region, null, 2))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  public void decrbyThrowsArithmeticExceptionWhenDecrementingMin() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = longToBytes(Long.MIN_VALUE);
    RedisString string = new RedisString(bytes);
    assertThatThrownBy(() -> string.decrby(region, null, 2))
        .isInstanceOf(ArithmeticException.class);
  }

  @Test
  public void decrbyDecrementsValue() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    byte[] bytes = stringToBytes("10");
    RedisString string = new RedisString(bytes);
    string.decrby(region, null, 2);
    assertThat(string.get()).isEqualTo(stringToBytes("8"));
  }

  @Test
  public void strlenReturnsStringLength() {
    byte[] bytes = {1, 2, 3, 4};
    RedisString string = new RedisString(bytes);
    assertThat(string.strlen()).isEqualTo(bytes.length);
  }

  @Test
  public void strlenReturnsLengthOfEmptyString() {
    RedisString string = new RedisString(new byte[] {});
    assertThat(string.strlen()).isEqualTo(0);
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    byte[] bytes = {0, 1, 2, 3};
    RedisString stringOne = new RedisString(bytes);
    stringOne.setExpirationTimestampNoDelta(1000);
    RedisString stringTwo = new RedisString(bytes);
    stringTwo.setExpirationTimestampNoDelta(999);
    assertThat(stringOne).isNotEqualTo(stringTwo);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    int expirationTimestamp = 1000;
    RedisString stringOne = new RedisString(new byte[] {0, 1, 2, 3});
    stringOne.setExpirationTimestampNoDelta(expirationTimestamp);
    RedisString stringTwo = new RedisString(new byte[] {0, 1, 2, 2});
    stringTwo.setExpirationTimestampNoDelta(expirationTimestamp);
    assertThat(stringOne).isNotEqualTo(stringTwo);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    byte[] bytes = {0, 1, 2, 3};
    int expirationTimestamp = 1000;
    RedisString stringOne = new RedisString(bytes);
    stringOne.setExpirationTimestampNoDelta(expirationTimestamp);
    RedisString stringTwo = new RedisString(bytes);
    stringTwo.setExpirationTimestampNoDelta(expirationTimestamp);
    assertThat(stringOne).isEqualTo(stringTwo);
  }

  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final byte[] bytes = {0, 1};
    when(region.put(any(), any()))
        .thenAnswer(invocation -> validateDeltaSerialization(bytes, invocation));
    RedisString stringOne = new RedisString(bytes);

    stringOne.setExpirationTimestamp(region, null, 999);

    verify(region).put(any(), any());
    assertThat(stringOne.hasDelta()).isFalse();
  }

  private Object validateDeltaSerialization(byte[] bytes, InvocationOnMock invocation)
      throws IOException {
    RedisString value = invocation.getArgument(1, RedisString.class);
    assertThat(value.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    value.toDelta(out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisString stringTwo = new RedisString(bytes);
    assertThat(stringTwo).isNotEqualTo(value);
    stringTwo.fromDelta(in);
    assertThat(stringTwo).isEqualTo(value);
    return null;
  }

  @Test
  public void bitposReturnsNegativeOneWhenBitIsNotZeroOrOne() {
    RedisString string = new RedisString(new byte[] {0, 1});
    assertThat(string.bitpos(2, 0, 1)).isEqualTo(-1);
  }

  /************* Size in Bytes Tests *************/
  /******* constructors *******/
  @Test
  public void should_calculateSize_equalToROSSize_ofLargeStrings() {
    String javaString = makeStringOfSpecifiedSize(10_000);
    RedisString string = new RedisString(stringToBytes(javaString));

    int actual = string.getSizeInBytes();
    int expected = reflectionObjectSizer.sizeof(string);

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_equalToROSSize_ofStringOfVariousSizes() {
    String javaString;
    for (int i = 0; i < 512; i += 8) {
      javaString = makeStringOfSpecifiedSize(i);
      RedisString string = new RedisString(stringToBytes(javaString));

      int expected = reflectionObjectSizer.sizeof(string);
      int actual = string.getSizeInBytes();

      assertThat(actual).isEqualTo(expected);
    }
  }

  /******* changing values *******/
  @Test
  public void changingStringValue_toShorterString_shouldDecreaseSizeInBytes() {
    String baseString = "baseString";
    String stringToRemove = "asdf1234567890";
    RedisString string = new RedisString(stringToBytes((baseString + stringToRemove)));

    int initialSize = string.getSizeInBytes();
    assertThat(initialSize).isEqualTo(reflectionObjectSizer.sizeof(string));

    string.set(stringToBytes(baseString));

    int finalSize = string.getSizeInBytes();
    assertThat(finalSize).isEqualTo(reflectionObjectSizer.sizeof(string));

    assertThat(finalSize).isLessThan(initialSize);
  }

  @Test
  public void changingStringValue_toLongerString_shouldIncreaseSizeInBytes() {
    String baseString = "baseString";
    RedisString string = new RedisString(stringToBytes(baseString));

    int initialSize = string.getSizeInBytes();
    assertThat(initialSize).isEqualTo(reflectionObjectSizer.sizeof(string));

    String addedString = "asdf1234567890";
    string.set(stringToBytes((baseString + addedString)));

    int finalSize = string.getSizeInBytes();
    assertThat(finalSize).isEqualTo(reflectionObjectSizer.sizeof(string));

    assertThat(finalSize).isGreaterThan(initialSize);
  }

  @Test
  public void changingStringValue_toEmptyString_shouldDecreaseSizeInBytes() {
    String baseString = "baseString1234567890";
    final int emptySize = reflectionObjectSizer.sizeof(new RedisString(stringToBytes("")));
    RedisString string = new RedisString(stringToBytes((baseString)));
    int baseSize = string.getSizeInBytes();

    string.set(stringToBytes(""));

    int finalSize = string.getSizeInBytes();

    assertThat(finalSize).isEqualTo(emptySize);
    assertThat(finalSize).isLessThan(baseSize);
  }

  /******* helper methods *******/

  private String makeStringOfSpecifiedSize(final int stringSize) {
    return StringUtils.repeat("a", stringSize);
  }
}
