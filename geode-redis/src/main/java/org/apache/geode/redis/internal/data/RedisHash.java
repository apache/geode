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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisHash extends AbstractRedisData {
  public static final RedisHash NULL_REDIS_HASH = new NullRedisHash();
  private HashMap<ByteArrayWrapper, ByteArrayWrapper> hash;

  public RedisHash(List<ByteArrayWrapper> fieldsToSet) {
    hash = new HashMap<>();
    Iterator<ByteArrayWrapper> iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      hashPut(iterator.next(), iterator.next());
    }
  }

  public RedisHash() {
    // for serialization
  }

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads
   * are modifying this object, the striped executor will not protect toData.
   * So any methods that modify "hash" needs to be thread safe with toData.
   */
  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeHashMap(hash, out);
  }

  private synchronized ByteArrayWrapper hashPut(ByteArrayWrapper field, ByteArrayWrapper value) {
    return hash.put(field, value);
  }

  private synchronized ByteArrayWrapper hashPutIfAbsent(ByteArrayWrapper field,
      ByteArrayWrapper value) {
    return hash.putIfAbsent(field, value);
  }

  private synchronized ByteArrayWrapper hashRemove(ByteArrayWrapper field) {
    return hash.remove(field);
  }


  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    hash = DataSerializer.readHashMap(in);
  }


  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
    if (deltaInfo instanceof AddsDeltaInfo) {
      AddsDeltaInfo addsDeltaInfo = (AddsDeltaInfo) deltaInfo;
      Iterator<ByteArrayWrapper> iterator = addsDeltaInfo.getAdds().iterator();
      while (iterator.hasNext()) {
        ByteArrayWrapper field = iterator.next();
        ByteArrayWrapper value = iterator.next();
        hashPut(field, value);
      }
    } else {
      RemsDeltaInfo remsDeltaInfo = (RemsDeltaInfo) deltaInfo;
      for (ByteArrayWrapper field : remsDeltaInfo.getRemoves()) {
        hashRemove(field);
      }
    }
  }

  public int hset(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToSet, boolean nx) {
    int fieldsAdded = 0;
    AddsDeltaInfo deltaInfo = null;
    Iterator<ByteArrayWrapper> iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      ByteArrayWrapper field = iterator.next();
      ByteArrayWrapper value = iterator.next();
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
          deltaInfo = new AddsDeltaInfo();
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

  public int hdel(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      List<ByteArrayWrapper> fieldsToRemove) {
    int fieldsRemoved = 0;
    RemsDeltaInfo deltaInfo = null;
    for (ByteArrayWrapper fieldToRemove : fieldsToRemove) {
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

  public Collection<ByteArrayWrapper> hgetall() {
    ArrayList<ByteArrayWrapper> result = new ArrayList<>();
    for (Map.Entry<ByteArrayWrapper, ByteArrayWrapper> entry : hash.entrySet()) {
      result.add(entry.getKey());
      result.add(entry.getValue());
    }
    return result;
  }

  public int hexists(ByteArrayWrapper field) {
    if (hash.containsKey(field)) {
      return 1;
    } else {
      return 0;
    }
  }

  public ByteArrayWrapper hget(ByteArrayWrapper field) {
    return hash.get(field);
  }

  public int hlen() {
    return hash.size();
  }

  public int hstrlen(ByteArrayWrapper field) {
    ByteArrayWrapper entry = hget(field);
    return entry != null ? entry.length() : 0;
  }

  public List<ByteArrayWrapper> hmget(List<ByteArrayWrapper> fields) {
    ArrayList<ByteArrayWrapper> results = new ArrayList<>(fields.size());
    for (ByteArrayWrapper field : fields) {
      results.add(hash.get(field));
    }
    return results;
  }

  public Collection<ByteArrayWrapper> hvals() {
    return new ArrayList<>(hash.values());
  }

  public Collection<ByteArrayWrapper> hkeys() {
    return new ArrayList<>(hash.keySet());
  }

  public List<Object> hscan(Pattern matchPattern, int count, int cursor) {
    List<Object> returnList = new ArrayList<Object>();
    int size = hash.size();
    int beforeCursor = 0;
    int numElements = 0;
    int i = -1;
    for (Map.Entry<ByteArrayWrapper, ByteArrayWrapper> entry : hash.entrySet()) {
      ByteArrayWrapper key = entry.getKey();
      ByteArrayWrapper value = entry.getValue();
      i++;
      if (beforeCursor < cursor) {
        beforeCursor++;
        continue;
      } else if (numElements < count) {
        if (matchPattern != null) {
          if (matchPattern.matcher(key.toString()).matches()) {
            returnList.add(key);
            returnList.add(value);
            numElements++;
          }
        } else {
          returnList.add(key);
          returnList.add(value);
          numElements++;
        }
      } else {
        break;
      }
    }

    if (i == size - 1) {
      returnList.add(0, String.valueOf(0));
    } else {
      returnList.add(0, String.valueOf(i));
    }
    return returnList;
  }

  public long hincrby(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      ByteArrayWrapper field, long increment)
      throws NumberFormatException, ArithmeticException {
    ByteArrayWrapper oldValue = hash.get(field);
    if (oldValue == null) {
      ByteArrayWrapper newValue = new ByteArrayWrapper(Coder.longToBytes(increment));
      hashPut(field, newValue);
      AddsDeltaInfo deltaInfo = new AddsDeltaInfo();
      deltaInfo.add(field);
      deltaInfo.add(newValue);
      storeChanges(region, key, deltaInfo);
      return increment;
    }

    long value;
    try {
      value = Long.parseLong(oldValue.toString());
    } catch (NumberFormatException ex) {
      throw new NumberFormatException(ERROR_NOT_INTEGER);
    }
    if ((value >= 0 && increment > (Long.MAX_VALUE - value))
        || (value <= 0 && increment < (Long.MIN_VALUE - value))) {
      throw new ArithmeticException(ERROR_OVERFLOW);
    }

    value += increment;

    ByteArrayWrapper modifiedValue = new ByteArrayWrapper(Coder.longToBytes(value));
    hashPut(field, modifiedValue);
    AddsDeltaInfo deltaInfo = new AddsDeltaInfo();
    deltaInfo.add(field);
    deltaInfo.add(modifiedValue);
    storeChanges(region, key, deltaInfo);
    return value;
  }

  public double hincrbyfloat(Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key,
      ByteArrayWrapper field, double increment)
      throws NumberFormatException {
    ByteArrayWrapper oldValue = hash.get(field);
    if (oldValue == null) {
      ByteArrayWrapper newValue = new ByteArrayWrapper(Coder.doubleToBytes(increment));
      hashPut(field, newValue);
      AddsDeltaInfo deltaInfo = new AddsDeltaInfo();
      deltaInfo.add(field);
      deltaInfo.add(newValue);
      storeChanges(region, key, deltaInfo);
      return increment;
    }

    String valueS = oldValue.toString();
    if (valueS.contains(" ")) {
      throw new NumberFormatException("hash value is not a float");
    }
    double value;
    try {
      value = Coder.stringToDouble(valueS);
    } catch (NumberFormatException ex) {
      throw new NumberFormatException("hash value is not a float");
    }

    value += increment;

    ByteArrayWrapper modifiedValue = new ByteArrayWrapper(Coder.doubleToBytes(value));
    hashPut(field, modifiedValue);
    AddsDeltaInfo deltaInfo = new AddsDeltaInfo();
    deltaInfo.add(field);
    deltaInfo.add(modifiedValue);
    storeChanges(region, key, deltaInfo);
    return value;
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
    return Objects.equals(hash, redisHash.hash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), hash);
  }

  @Override
  public String toString() {
    return "RedisHash{" +
        super.toString() + ", " +
        "hash=" + hash +
        '}';
  }
}
