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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Objects;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.delta.AppendDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.executor.string.SetOptions;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisString extends AbstractRedisData {

  private int appendSequence;

  private ByteArrayWrapper value;

  public RedisString(ByteArrayWrapper value) {
    this.value = value;
  }

  // for serialization
  public RedisString() {}

  public ByteArrayWrapper get() {
    return new ByteArrayWrapper(value.toBytes());
  }

  public void set(ByteArrayWrapper value) {
    valueSet(value);
  }

  public int append(ByteArrayWrapper appendValue, Region<RedisKey, RedisData> region,
      RedisKey key) {
    valueAppend(appendValue.toBytes());
    appendSequence++;
    storeChanges(region, key, new AppendDeltaInfo(appendValue.toBytes(), appendSequence));
    return value.length();
  }

  public long incr(Region<RedisKey, RedisData> region, RedisKey key)
      throws NumberFormatException, ArithmeticException {
    long longValue = parseValueAsLong();
    if (longValue == Long.MAX_VALUE) {
      throw new ArithmeticException(RedisConstants.ERROR_OVERFLOW);
    }
    longValue++;
    valueSetBytes(Coder.longToBytes(longValue));
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return longValue;
  }

  public long incrby(Region<RedisKey, RedisData> region, RedisKey key, long increment)
      throws NumberFormatException, ArithmeticException {
    long longValue = parseValueAsLong();
    if (longValue >= 0 && increment > (Long.MAX_VALUE - longValue)) {
      throw new ArithmeticException(RedisConstants.ERROR_OVERFLOW);
    }
    longValue += increment;
    valueSetBytes(Coder.longToBytes(longValue));
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return longValue;
  }

  public BigDecimal incrbyfloat(Region<RedisKey, RedisData> region, RedisKey key,
      BigDecimal increment)
      throws NumberFormatException, ArithmeticException {
    BigDecimal bigDecimalValue = parseValueAsBigDecimal();
    bigDecimalValue = bigDecimalValue.add(increment);
    valueSetBytes(Coder.bigDecimalToBytes(bigDecimalValue));

    // numeric strings are short so no need to use delta
    region.put(key, this);
    return bigDecimalValue;
  }

  public long decrby(Region<RedisKey, RedisData> region, RedisKey key, long decrement) {
    long longValue = parseValueAsLong();
    if (longValue <= 0 && -decrement < (Long.MIN_VALUE - longValue)) {
      throw new ArithmeticException(RedisConstants.ERROR_OVERFLOW);
    }
    longValue -= decrement;
    valueSetBytes(Coder.longToBytes(longValue));
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return longValue;
  }

  public long decr(Region<RedisKey, RedisData> region, RedisKey key)
      throws NumberFormatException, ArithmeticException {
    long longValue = parseValueAsLong();
    if (longValue == Long.MIN_VALUE) {
      throw new ArithmeticException(RedisConstants.ERROR_OVERFLOW);
    }
    longValue--;
    valueSetBytes(Coder.longToBytes(longValue));
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return longValue;
  }

  private long parseValueAsLong() {
    try {
      return Long.parseLong(value.toString());
    } catch (NumberFormatException ex) {
      throw new NumberFormatException(RedisConstants.ERROR_NOT_INTEGER);
    }
  }

  private BigDecimal parseValueAsBigDecimal() {
    String valueString = value.toString();
    if (valueString.contains(" ")) {
      throw new NumberFormatException(RedisConstants.ERROR_NOT_A_VALID_FLOAT);
    }
    try {
      return new BigDecimal(valueString);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(RedisConstants.ERROR_NOT_A_VALID_FLOAT);
    }
  }

  public ByteArrayWrapper getrange(long start, long end) {
    int length = value.length();
    int boundedStart = getBoundedStartIndex(start, length);
    int boundedEnd = getBoundedEndIndex(end, length);

    /*
     * Can't 'start' at end of value
     */
    if (boundedStart > boundedEnd || boundedStart == length) {
      return new ByteArrayWrapper(new byte[0]);
    }
    /*
     * 1 is added to end because the end in copyOfRange is exclusive but in Redis it is inclusive
     */
    if (boundedEnd != length) {
      boundedEnd++;
    }
    byte[] returnRange = Arrays.copyOfRange(value.toBytes(), boundedStart, boundedEnd);
    return new ByteArrayWrapper(returnRange);
  }

  public int setrange(Region<RedisKey, RedisData> region, RedisKey key, int offset,
      byte[] valueToAdd) {
    if (valueToAdd.length == 0) {
      return value.length();
    }
    int totalLength = offset + valueToAdd.length;
    byte[] bytes = value.toBytes();
    if (totalLength < bytes.length) {
      System.arraycopy(valueToAdd, 0, bytes, offset, valueToAdd.length);
    } else {
      byte[] newBytes = Arrays.copyOf(bytes, totalLength);
      System.arraycopy(valueToAdd, 0, newBytes, offset, valueToAdd.length);
      valueSetBytes(newBytes);
    }
    // TODO add delta support
    region.put(key, this);
    return value.length();
  }

  private int getBoundedStartIndex(long index, int size) {
    if (index >= 0L) {
      return (int) Math.min(index, size);
    } else {
      return (int) Math.max(index + size, 0);
    }
  }

  private int getBoundedEndIndex(long index, int size) {
    if (index >= 0L) {
      return (int) Math.min(index, size);
    } else {
      return (int) Math.max(index + size, -1);
    }
  }

  public int bitpos(Region<RedisKey, RedisData> region, RedisKey key, int bit,
      int start, Integer end) {
    int length = value.length();
    if (length == 0) {
      return -1;
    }
    boolean endSet = end != null;
    if (!endSet) {
      end = length - 1;
    }
    if (start < 0) {
      start += length;
    }
    if (end < 0) {
      end += length;
    }

    if (start < 0) {
      start = 0;
    }
    if (end < 0) {
      end = 0;
    }

    if (start > length) {
      start = length - 1;
    }
    if (end > length) {
      end = length - 1;
    }

    if (end < start) {
      return -1;
    }

    byte[] bytes = value.toBytes();
    for (int i = start; i <= end; i++) {
      int cBit;
      byte cByte = bytes[i];
      for (int j = 0; j < 8; j++) {
        cBit = (cByte & (0x80 >> j)) >> (7 - j);
        if (cBit == bit) {
          return 8 * i + j;
        }
      }
    }

    if (bit == 0 && !endSet) {
      return length * 8;
    }

    return -1;
  }


  public long bitcount(int start, int end) {
    if (start < 0) {
      start += value.length();
    }
    if (end < 0) {
      end += value.length();
    }

    if (start < 0) {
      start = 0;
    }
    if (end < 0) {
      end = 0;
    }

    if (end > value.length() - 1) {
      end = value.length() - 1;
    }

    if (end < start || start >= value.length()) {
      return 0;
    }

    long setBits = 0;
    for (int j = start; j <= end; j++) {
      setBits += bitcountTable[0xFF & value.toBytes()[j]];
    }
    return setBits;
  }

  public long bitcount() {
    return bitcount(0, value.length() - 1);
  }

  private static final byte[] bitcountTable = {
      0, // 0x0
      1, // 0x1
      1, // 0x2
      2, // 0x3
      1, // 0x4
      2, // 0x5
      2, // 0x6
      3, // 0x7
      1, // 0x8
      2, // 0x9
      2, // 0xa
      3, // 0xb
      2, // 0xc
      3, // 0xd
      3, // 0xe
      4, // 0xf
      1, // 0x10
      2, // 0x11
      2, // 0x12
      3, // 0x13
      2, // 0x14
      3, // 0x15
      3, // 0x16
      4, // 0x17
      2, // 0x18
      3, // 0x19
      3, // 0x1a
      4, // 0x1b
      3, // 0x1c
      4, // 0x1d
      4, // 0x1e
      5, // 0x1f
      1, // 0x20
      2, // 0x21
      2, // 0x22
      3, // 0x23
      2, // 0x24
      3, // 0x25
      3, // 0x26
      4, // 0x27
      2, // 0x28
      3, // 0x29
      3, // 0x2a
      4, // 0x2b
      3, // 0x2c
      4, // 0x2d
      4, // 0x2e
      5, // 0x2f
      2, // 0x30
      3, // 0x31
      3, // 0x32
      4, // 0x33
      3, // 0x34
      4, // 0x35
      4, // 0x36
      5, // 0x37
      3, // 0x38
      4, // 0x39
      4, // 0x3a
      5, // 0x3b
      4, // 0x3c
      5, // 0x3d
      5, // 0x3e
      6, // 0x3f
      1, // 0x40
      2, // 0x41
      2, // 0x42
      3, // 0x43
      2, // 0x44
      3, // 0x45
      3, // 0x46
      4, // 0x47
      2, // 0x48
      3, // 0x49
      3, // 0x4a
      4, // 0x4b
      3, // 0x4c
      4, // 0x4d
      4, // 0x4e
      5, // 0x4f
      2, // 0x50
      3, // 0x51
      3, // 0x52
      4, // 0x53
      3, // 0x54
      4, // 0x55
      4, // 0x56
      5, // 0x57
      3, // 0x58
      4, // 0x59
      4, // 0x5a
      5, // 0x5b
      4, // 0x5c
      5, // 0x5d
      5, // 0x5e
      6, // 0x5f
      2, // 0x60
      3, // 0x61
      3, // 0x62
      4, // 0x63
      3, // 0x64
      4, // 0x65
      4, // 0x66
      5, // 0x67
      3, // 0x68
      4, // 0x69
      4, // 0x6a
      5, // 0x6b
      4, // 0x6c
      5, // 0x6d
      5, // 0x6e
      6, // 0x6f
      3, // 0x70
      4, // 0x71
      4, // 0x72
      5, // 0x73
      4, // 0x74
      5, // 0x75
      5, // 0x76
      6, // 0x77
      4, // 0x78
      5, // 0x79
      5, // 0x7a
      6, // 0x7b
      5, // 0x7c
      6, // 0x7d
      6, // 0x7e
      7, // 0x7f
      1, // 0x80
      2, // 0x81
      2, // 0x82
      3, // 0x83
      2, // 0x84
      3, // 0x85
      3, // 0x86
      4, // 0x87
      2, // 0x88
      3, // 0x89
      3, // 0x8a
      4, // 0x8b
      3, // 0x8c
      4, // 0x8d
      4, // 0x8e
      5, // 0x8f
      2, // 0x90
      3, // 0x91
      3, // 0x92
      4, // 0x93
      3, // 0x94
      4, // 0x95
      4, // 0x96
      5, // 0x97
      3, // 0x98
      4, // 0x99
      4, // 0x9a
      5, // 0x9b
      4, // 0x9c
      5, // 0x9d
      5, // 0x9e
      6, // 0x9f
      2, // 0xa0
      3, // 0xa1
      3, // 0xa2
      4, // 0xa3
      3, // 0xa4
      4, // 0xa5
      4, // 0xa6
      5, // 0xa7
      3, // 0xa8
      4, // 0xa9
      4, // 0xaa
      5, // 0xab
      4, // 0xac
      5, // 0xad
      5, // 0xae
      6, // 0xaf
      3, // 0xb0
      4, // 0xb1
      4, // 0xb2
      5, // 0xb3
      4, // 0xb4
      5, // 0xb5
      5, // 0xb6
      6, // 0xb7
      4, // 0xb8
      5, // 0xb9
      5, // 0xba
      6, // 0xbb
      5, // 0xbc
      6, // 0xbd
      6, // 0xbe
      7, // 0xbf
      2, // 0xc0
      3, // 0xc1
      3, // 0xc2
      4, // 0xc3
      3, // 0xc4
      4, // 0xc5
      4, // 0xc6
      5, // 0xc7
      3, // 0xc8
      4, // 0xc9
      4, // 0xca
      5, // 0xcb
      4, // 0xcc
      5, // 0xcd
      5, // 0xce
      6, // 0xcf
      3, // 0xd0
      4, // 0xd1
      4, // 0xd2
      5, // 0xd3
      4, // 0xd4
      5, // 0xd5
      5, // 0xd6
      6, // 0xd7
      4, // 0xd8
      5, // 0xd9
      5, // 0xda
      6, // 0xdb
      5, // 0xdc
      6, // 0xdd
      6, // 0xde
      7, // 0xdf
      3, // 0xe0
      4, // 0xe1
      4, // 0xe2
      5, // 0xe3
      4, // 0xe4
      5, // 0xe5
      5, // 0xe6
      6, // 0xe7
      4, // 0xe8
      5, // 0xe9
      5, // 0xea
      6, // 0xeb
      5, // 0xec
      6, // 0xed
      6, // 0xee
      7, // 0xef
      4, // 0xf0
      5, // 0xf1
      5, // 0xf2
      6, // 0xf3
      5, // 0xf4
      6, // 0xf5
      6, // 0xf6
      7, // 0xf7
      5, // 0xf8
      6, // 0xf9
      6, // 0xfa
      7, // 0xfb
      6, // 0xfc
      7, // 0xfd
      7, // 0xfe
      8 // 0xff
  };


  public int strlen() {
    return value.length();
  }

  public int getbit(int offset) {
    if (offset < 0) {
      offset += value.length() * 8;
    }

    if (offset < 0 || offset > value.length() * 8) {
      return 0;
    }

    int byteIndex = offset / 8;
    offset %= 8;

    if (byteIndex >= value.length()) {
      return 0;
    }

    return (value.toBytes()[byteIndex] & (0x80 >> offset)) >> (7 - offset);
  }

  public int setbit(Region<RedisKey, RedisData> region, RedisKey key,
      int bitValue, int byteIndex, byte bitIndex) {
    int returnBit;
    byte[] bytes = value.toBytes();
    if (byteIndex < bytes.length) {
      returnBit = (bytes[byteIndex] & (0x80 >> bitIndex)) >> (7 - bitIndex);
    } else {
      returnBit = 0;
    }

    if (byteIndex < bytes.length) {
      bytes[byteIndex] = bitValue == 1 ? (byte) (bytes[byteIndex] | (0x80 >> bitIndex))
          : (byte) (bytes[byteIndex] & ~(0x80 >> bitIndex));
    } else {
      byte[] newBytes = new byte[byteIndex + 1];
      System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
      newBytes[byteIndex] = bitValue == 1 ? (byte) (newBytes[byteIndex] | (0x80 >> bitIndex))
          : (byte) (newBytes[byteIndex] & ~(0x80 >> bitIndex));
      valueSetBytes(newBytes);
    }
    // TODO: add delta support
    region.put(key, this);
    return returnBit;
  }

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads
   * are modifying this object, the striped executor will not protect toData.
   * So any methods that modify "value", "appendSequence" need to be thread safe with toData.
   */

  @Override
  public synchronized void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writePrimitiveInt(appendSequence, out);
    DataSerializer.writeByteArray(value.toBytes(), out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    appendSequence = DataSerializer.readPrimitiveInt(in);
    value = new ByteArrayWrapper(DataSerializer.readByteArray(in));

  }

  @Override
  public int getDSFID() {
    return REDIS_STRING_ID;
  }

  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
    AppendDeltaInfo appendDeltaInfo = (AppendDeltaInfo) deltaInfo;
    byte[] appendBytes = appendDeltaInfo.getBytes();

    if (value == null) {
      value = new ByteArrayWrapper(appendBytes);
      appendSequence = appendDeltaInfo.getSequence();
    } else {
      if (appendDeltaInfo.getSequence() == appendSequence + 1) {
        valueAppend(appendBytes);
        appendSequence = appendDeltaInfo.getSequence();
      } else if (appendDeltaInfo.getSequence() != appendSequence) {
        // Exceptional case should never happen
        throw new RuntimeException("Redis APPEND sequence mismatch - delta sequence number: "
            + appendDeltaInfo.getSequence() + " current sequence number: " + appendSequence);
      }
    }
  }

  @Override
  public RedisDataType getType() {
    return RedisDataType.REDIS_STRING;
  }

  public ByteArrayWrapper getset(Region<RedisKey, RedisData> region, RedisKey key,
      ByteArrayWrapper newValue) {
    // No need to copy "value" since we are locked and will be calling set which replaces
    // "value" with a new instance.
    ByteArrayWrapper result = value;
    set(newValue);
    persistNoDelta();
    region.put(key, this);
    return result;
  }

  @Override
  protected boolean removeFromRegion() {
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisString)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RedisString that = (RedisString) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), value);
  }

  ByteArrayWrapper getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "RedisString{" +
        super.toString() + ", " +
        "value=" + value +
        '}';
  }

  protected void handleSetExpiration(SetOptions options) {
    long setExpiration = options == null ? 0L : options.getExpiration();
    if (setExpiration != 0) {
      long now = System.currentTimeMillis();
      long timestamp = now + setExpiration;
      setExpirationTimestampNoDelta(timestamp);
    } else if (options == null || !options.isKeepTTL()) {
      persistNoDelta();
    }
  }

  ////// methods that modify the "value" field ////////////

  protected void valueAppend(byte[] bytes) {
    value.append(bytes);
  }

  protected void valueSet(ByteArrayWrapper newValue) {
    value = newValue;
  }

  protected void valueSetBytes(byte[] bytes) {
    value.setBytes(bytes);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
