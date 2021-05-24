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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_OVERFLOW;
import static org.apache.geode.redis.internal.netty.Coder.bytesToLong;
import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.collections.SizeableObject2ObjectOpenCustomHashMapWithCursor;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisHash extends AbstractRedisData {
  // The following constant was calculated using reflection. you can find the test for this value in
  // RedisHashTest, which shows the way this number was calculated. If our internal implementation
  // changes, these values may be incorrect. An increase in overhead should be carefully considered.
  protected static final int BASE_REDIS_HASH_OVERHEAD = 32;

  private SizeableObject2ObjectOpenCustomHashMapWithCursor<byte[], byte[]> hash;

  @VisibleForTesting
  public RedisHash(List<byte[]> fieldsToSet) {
    final int numKeysAndValues = fieldsToSet.size();
    if (numKeysAndValues % 2 != 0) {
      throw new IllegalStateException(
          "fieldsToSet should have an even number of elements but was size " + numKeysAndValues);
    }

    hash = new SizeableObject2ObjectOpenCustomHashMapWithCursor<>(numKeysAndValues / 2,
        ByteArrays.HASH_STRATEGY);
    Iterator<byte[]> iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      hashPut(iterator.next(), iterator.next());
    }
  }

  /**
   * For deserialization only.
   */
  public RedisHash() {}

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads are modifying this
   * object, the striped executor will not protect toData. So any methods that modify "hash" needs
   * to be thread safe with toData.
   */
  @Override
  public synchronized void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writePrimitiveInt(hash.size(), out);
    for (Map.Entry<byte[], byte[]> entry : hash.entrySet()) {
      byte[] key = entry.getKey();
      byte[] value = entry.getValue();
      DataSerializer.writeByteArray(key, out);
      DataSerializer.writeByteArray(value, out);
    }
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int size = DataSerializer.readInteger(in);
    hash = new SizeableObject2ObjectOpenCustomHashMapWithCursor<>(size, ByteArrays.HASH_STRATEGY);
    for (int i = 0; i < size; i++) {
      hash.put(DataSerializer.readByteArray(in), DataSerializer.readByteArray(in));
    }
  }

  @Override
  public int getDSFID() {
    return REDIS_HASH_ID;
  }

  synchronized byte[] hashPut(byte[] field, byte[] value) {
    return hash.put(field, value);
  }

  private synchronized byte[] hashPutIfAbsent(byte[] field, byte[] value) {
    return hash.putIfAbsent(field, value);
  }

  private synchronized byte[] hashRemove(byte[] field) {
    return hash.remove(field);
  }

  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
    if (deltaInfo instanceof AddsDeltaInfo) {
      AddsDeltaInfo addsDeltaInfo = (AddsDeltaInfo) deltaInfo;
      Iterator<byte[]> iterator = addsDeltaInfo.getAdds().iterator();
      while (iterator.hasNext()) {
        byte[] field = iterator.next();
        byte[] value = iterator.next();
        hashPut(field, value);
      }
    } else {
      RemsDeltaInfo remsDeltaInfo = (RemsDeltaInfo) deltaInfo;
      for (byte[] field : remsDeltaInfo.getRemoves()) {
        hashRemove(field);
      }
    }
  }

  public int hset(Region<RedisKey, RedisData> region, RedisKey key,
      List<byte[]> fieldsToSet, boolean nx) {
    int fieldsAdded = 0;
    AddsDeltaInfo deltaInfo = null;
    Iterator<byte[]> iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      byte[] field = iterator.next();
      byte[] value = iterator.next();
      boolean added = true;
      boolean newField;
      if (nx) {
        newField = hashPutIfAbsent(field, value) == null;
        added = newField;
      } else {
        newField = hashPut(field, value) == null;
      }

      if (added) {
        if (deltaInfo == null) {
          deltaInfo = new AddsDeltaInfo(fieldsToSet.size());
        }
        deltaInfo.add(field);
        deltaInfo.add(value);
      }

      if (newField) {
        fieldsAdded++;
      }
    }
    storeChanges(region, key, deltaInfo);

    return fieldsAdded;
  }

  public int hdel(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> fieldsToRemove) {
    int fieldsRemoved = 0;
    RemsDeltaInfo deltaInfo = null;
    for (byte[] fieldToRemove : fieldsToRemove) {
      if (hashRemove(fieldToRemove) != null) {
        if (deltaInfo == null) {
          deltaInfo = new RemsDeltaInfo();
        }
        deltaInfo.add(fieldToRemove);
        fieldsRemoved++;
      }
    }
    storeChanges(region, key, deltaInfo);
    return fieldsRemoved;
  }

  public Collection<byte[]> hgetall() {
    ArrayList<byte[]> result = new ArrayList<>(hash.size());
    for (Map.Entry<byte[], byte[]> entry : hash.entrySet()) {
      result.add(entry.getKey());
      result.add(entry.getValue());
    }
    return result;
  }

  public int hexists(byte[] field) {
    if (hash.containsKey(field)) {
      return 1;
    } else {
      return 0;
    }
  }

  public byte[] hget(byte[] field) {
    return hash.get(field);
  }

  public int hlen() {
    return hash.size();
  }

  public int hstrlen(byte[] field) {
    byte[] entry = hget(field);
    return entry != null ? entry.length : 0;
  }

  public List<byte[]> hmget(List<byte[]> fields) {
    ArrayList<byte[]> results = new ArrayList<>(fields.size());
    for (byte[] field : fields) {
      results.add(hash.get(field));
    }
    return results;
  }

  public Collection<byte[]> hvals() {
    return new ArrayList<>(hash.values());
  }

  public Collection<byte[]> hkeys() {
    return new ArrayList<>(hash.keySet());
  }

  public ImmutablePair<Integer, List<byte[]>> hscan(Pattern matchPattern,
      int count,
      int cursor) {

    ArrayList<byte[]> resultList = new ArrayList<>(count + 2);
    do {
      cursor = hash.scan(cursor, 1,
          (list, key, value) -> addIfMatching(matchPattern, list, key, value), resultList);
    } while (cursor != 0 && resultList.size() < (count * 2));

    return new ImmutablePair<>(cursor, resultList);
  }

  private void addIfMatching(Pattern matchPattern, List<byte[]> resultList, byte[] key,
      byte[] value) {
    if (matchPattern != null) {
      if (matchPattern.matcher(bytesToString(key)).matches()) {
        resultList.add(key);
        resultList.add(value);
      }
    } else {
      resultList.add(key);
      resultList.add(value);
    }
  }

  public long hincrby(Region<RedisKey, RedisData> region, RedisKey key,
      byte[] field, long increment)
      throws NumberFormatException, ArithmeticException {
    byte[] oldValue = hash.get(field);
    if (oldValue == null) {
      byte[] newValue = Coder.longToBytes(increment);
      hashPut(field, newValue);
      AddsDeltaInfo deltaInfo = new AddsDeltaInfo(2);
      deltaInfo.add(field);
      deltaInfo.add(newValue);
      storeChanges(region, key, deltaInfo);
      return increment;
    }

    long value;
    try {
      value = bytesToLong(oldValue);
    } catch (NumberFormatException ex) {
      throw new NumberFormatException(ERROR_NOT_INTEGER);
    }
    if ((value >= 0 && increment > (Long.MAX_VALUE - value))
        || (value <= 0 && increment < (Long.MIN_VALUE - value))) {
      throw new ArithmeticException(ERROR_OVERFLOW);
    }

    value += increment;

    byte[] modifiedValue = Coder.longToBytes(value);
    hashPut(field, modifiedValue);
    AddsDeltaInfo deltaInfo = new AddsDeltaInfo(2);
    deltaInfo.add(field);
    deltaInfo.add(modifiedValue);
    storeChanges(region, key, deltaInfo);
    return value;
  }

  public BigDecimal hincrbyfloat(Region<RedisKey, RedisData> region, RedisKey key,
      byte[] field, BigDecimal increment) throws NumberFormatException {
    byte[] oldValue = hash.get(field);
    if (oldValue == null) {
      byte[] newValue = Coder.bigDecimalToBytes(increment);
      hashPut(field, newValue);
      AddsDeltaInfo deltaInfo = new AddsDeltaInfo(2);
      deltaInfo.add(field);
      deltaInfo.add(newValue);
      storeChanges(region, key, deltaInfo);
      return increment.stripTrailingZeros();
    }

    String valueS = bytesToString(oldValue);
    if (valueS.contains(" ")) {
      throw new NumberFormatException("hash value is not a float");
    }

    BigDecimal value;
    try {
      value = new BigDecimal(valueS);
    } catch (NumberFormatException ex) {
      throw new NumberFormatException("hash value is not a float");
    }

    value = value.add(increment);

    byte[] modifiedValue = Coder.bigDecimalToBytes(value);
    hashPut(field, modifiedValue);
    AddsDeltaInfo deltaInfo = new AddsDeltaInfo(2);
    deltaInfo.add(field);
    deltaInfo.add(modifiedValue);
    storeChanges(region, key, deltaInfo);
    return value.stripTrailingZeros();
  }

  @Override
  public RedisDataType getType() {
    return RedisDataType.REDIS_HASH;
  }

  @Override
  protected boolean removeFromRegion() {
    return hash.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisHash)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RedisHash redisHash = (RedisHash) o;
    if (hash.size() != redisHash.hash.size()) {
      return false;
    }

    for (Map.Entry<byte[], byte[]> entry : hash.entrySet()) {
      if (!Arrays.equals(redisHash.hash.get(entry.getKey()), (entry.getValue()))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), hash);
  }

  @Override
  public String toString() {
    return "RedisHash{" + super.toString() + ", " + "size=" + hash.size() + "}";
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getSizeInBytes() {
    return BASE_REDIS_HASH_OVERHEAD + hash.getSizeInBytes();
  }
}
