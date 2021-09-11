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

import static org.apache.geode.internal.JvmSizeUtils.memoryOverhead;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

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

  private static final int REDIS_STRING_OVERHEAD = memoryOverhead(RedisString.class);
  // An array containing the number of set bits for each value from 0x00 to 0xff
  private static final byte[] bitCountTable = getBitCountTable();

  private int appendSequence;
  private byte[] value;

  public RedisString(byte[] value) {
    this.value = value;
  }

  // for serialization
  public RedisString() {}

  public byte[] get() {
    return value;
  }

  public void set(byte[] value) {
    this.value = value;
  }

  public int append(Region<RedisKey, RedisData> region, RedisKey key, byte[] appendValue) {
    valueAppend(appendValue);
    appendSequence++;
    storeChanges(region, key, new AppendDeltaInfo(appendValue, appendSequence));
    return value.length;
  }

  public byte[] incr(Region<RedisKey, RedisData> region, RedisKey key)
      throws NumberFormatException, ArithmeticException {
    long longValue = parseValueAsLong();
    if (longValue == Long.MAX_VALUE) {
      throw new ArithmeticException(RedisConstants.ERROR_OVERFLOW);
    }
    longValue++;
    value = Coder.longToBytes(longValue);
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return value;
  }

  public byte[] incrby(Region<RedisKey, RedisData> region, RedisKey key, long increment)
      throws NumberFormatException, ArithmeticException {
    long longValue = parseValueAsLong();
    if (longValue >= 0 && increment > (Long.MAX_VALUE - longValue)) {
      throw new ArithmeticException(RedisConstants.ERROR_OVERFLOW);
    }
    longValue += increment;
    value = Coder.longToBytes(longValue);
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return value;
  }

  public BigDecimal incrbyfloat(Region<RedisKey, RedisData> region, RedisKey key,
      BigDecimal increment)
      throws NumberFormatException, ArithmeticException {
    BigDecimal bigDecimalValue = parseValueAsBigDecimal();
    bigDecimalValue = bigDecimalValue.add(increment);
    value = Coder.bigDecimalToBytes(bigDecimalValue);

    // numeric strings are short so no need to use delta
    region.put(key, this);
    return bigDecimalValue;
  }

  public byte[] decrby(Region<RedisKey, RedisData> region, RedisKey key, long decrement) {
    long longValue = parseValueAsLong();
    if (longValue <= 0 && -decrement < (Long.MIN_VALUE - longValue)) {
      throw new ArithmeticException(RedisConstants.ERROR_OVERFLOW);
    }
    longValue -= decrement;
    value = Coder.longToBytes(longValue);
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return value;
  }

  public byte[] decr(Region<RedisKey, RedisData> region, RedisKey key)
      throws NumberFormatException, ArithmeticException {
    long longValue = parseValueAsLong();
    if (longValue == Long.MIN_VALUE) {
      throw new ArithmeticException(RedisConstants.ERROR_OVERFLOW);
    }
    longValue--;
    value = Coder.longToBytes(longValue);
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return value;
  }

  private long parseValueAsLong() {
    try {
      return Coder.bytesToLong(value);
    } catch (NumberFormatException ex) {
      throw new NumberFormatException(RedisConstants.ERROR_NOT_INTEGER);
    }
  }

  private BigDecimal parseValueAsBigDecimal() {
    String valueString = bytesToString(value);
    if (valueString.contains(" ")) {
      throw new NumberFormatException(RedisConstants.ERROR_NOT_A_VALID_FLOAT);
    }
    try {
      return new BigDecimal(valueString);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(RedisConstants.ERROR_NOT_A_VALID_FLOAT);
    }
  }

  public byte[] getrange(long start, long end) {
    int length = value.length;
    int boundedStart = getBoundedStartIndex(start, length);
    int boundedEnd = getBoundedEndIndex(end, length);

    // Can't 'start' at end of value
    if (boundedStart > boundedEnd || boundedStart == length) {
      return new byte[0];
    }
    // 1 is added to end because the end in copyOfRange is exclusive but in Redis it is inclusive
    if (boundedEnd != length) {
      boundedEnd++;
    }
    return Arrays.copyOfRange(value, boundedStart, boundedEnd);
  }

  public int setrange(Region<RedisKey, RedisData> region, RedisKey key, int offset,
      byte[] valueToAdd) {
    if (valueToAdd.length == 0) {
      return value.length;
    }
    int totalLength = offset + valueToAdd.length;
    byte[] bytes = value;
    if (totalLength < bytes.length) {
      System.arraycopy(valueToAdd, 0, bytes, offset, valueToAdd.length);
    } else {
      byte[] newBytes = Arrays.copyOf(bytes, totalLength);
      System.arraycopy(valueToAdd, 0, newBytes, offset, valueToAdd.length);
      value = newBytes;
    }
    // TODO add delta support
    region.put(key, this);
    return value.length;
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

  public int bitpos(int bit, int start, Integer end) {
    int length = value.length;
    if (length == 0) {
      return -1;
    }
    boolean endSet = end != null;
    if (!endSet) {
      end = length - 1;
    }

    // BITPOS allows indexing from the end of the string using negative values for start and end
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

    if (start >= length) {
      start = length - 1;
    }
    if (end >= length) {
      end = length - 1;
    }

    if (end < start) {
      return -1;
    }

    for (int i = start; i <= end; i++) {
      int cBit;
      byte cByte = value[i];
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
      start += value.length;
    }
    if (end < 0) {
      end += value.length;
    }

    if (start < 0) {
      start = 0;
    }
    if (end < 0) {
      end = 0;
    }

    if (end > value.length - 1) {
      end = value.length - 1;
    }

    if (end < start) {
      return 0;
    }

    long setBits = 0;
    for (int j = start; j <= end; j++) {
      setBits += bitCountTable[0xFF & value[j]];
    }
    return setBits;
  }

  public long bitcount() {
    return bitcount(0, value.length - 1);
  }

  public int strlen() {
    return value.length;
  }

  public int getbit(int offset) {
    if (offset < 0) {
      offset += value.length * 8;
    }

    if (offset < 0 || offset > value.length * 8) {
      return 0;
    }

    int byteIndex = offset / 8;
    offset %= 8;

    if (byteIndex >= value.length) {
      return 0;
    }

    return (value[byteIndex] & (0x80 >> offset)) >> (7 - offset);
  }

  public int setbit(Region<RedisKey, RedisData> region, RedisKey key,
      int bitValue, int byteIndex, byte bitIndex) {
    int returnBit;
    byte[] bytes = value;
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
      value = newBytes;
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
    DataSerializer.writeByteArray(value, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    appendSequence = DataSerializer.readPrimitiveInt(in);
    value = DataSerializer.readByteArray(in);

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
      value = appendBytes;
      appendSequence = appendDeltaInfo.getSequence();
    } else {
      if (appendDeltaInfo.getSequence() == appendSequence + 1) {
        valueAppend(appendBytes);
        appendSequence = appendDeltaInfo.getSequence();
      } else if (appendDeltaInfo.getSequence() != appendSequence) {
        // Exceptional case should never happen
        throw new RuntimeException(
            "APPEND sequence mismatch - delta sequence number: "
                + appendDeltaInfo.getSequence() + " current sequence number: " + appendSequence);
      }
    }
  }

  @Override
  public RedisDataType getType() {
    return RedisDataType.REDIS_STRING;
  }

  public byte[] getset(Region<RedisKey, RedisData> region, RedisKey key, byte[] newValue) {
    // No need to copy "value" since we are locked and will be calling set which replaces
    // "value" with a new instance.
    byte[] result = value;
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
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), value);
  }

  byte[] getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "RedisString{" +
        super.toString() + ", " +
        "value=" + bytesToString(value) +
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
    int initialLength = value.length;
    int additionalLength = bytes.length;
    byte[] combined = new byte[initialLength + additionalLength];
    System.arraycopy(value, 0, combined, 0, initialLength);
    System.arraycopy(bytes, 0, combined, initialLength, additionalLength);
    value = combined;
  }

  @SuppressWarnings("unused")
  protected void valueSet(byte[] bytes) {
    value = bytes;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getSizeInBytes() {
    return REDIS_STRING_OVERHEAD + memoryOverhead(value);
  }

  private static byte[] getBitCountTable() {
    byte[] table = new byte[256];
    for (int i = 0; i < table.length; ++i) {
      table[i] = (byte) Integer.bitCount(i);
    }
    return table;
  }
}
