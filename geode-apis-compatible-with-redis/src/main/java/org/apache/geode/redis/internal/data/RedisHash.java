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

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.logging.internal.executors.LoggingExecutors.newSingleThreadScheduledExecutor;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_INTEGER;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_OVERFLOW;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisHash extends AbstractRedisData {
  private HashMap<ByteArrayWrapper, ByteArrayWrapper> hash;
  private ConcurrentHashMap<UUID, List<ByteArrayWrapper>> hScanSnapShots;
  private ConcurrentHashMap<UUID, Long> hScanSnapShotCreationTimes;
  private ScheduledExecutorService HSCANSnapshotExpirationExecutor = null;

  // these values are empirically derived using ReflectionObjectSizer, which provides an exact size
  // of the object. It can't be used directly because of its performance impact. These values cause
  // the size we keep track of to converge to the actual size as the number of entries/instances
  // increases.
  private static final int PER_BYTE_ARRAY_WRAPPER_OVERHEAD = PER_OBJECT_OVERHEAD + 46;
//  private static final int PER_HASH_OVERHEAD = PER_OBJECT_OVERHEAD + 324;

  private int myCalculatedSize;

  private static int defaultHscanSnapshotsExpireCheckFrequency =
      Integer.getInteger("redis.hscan-snapshot-cleanup-interval", 30000);

  private static int defaultHscanSnapshotsMillisecondsToLive =
      Integer.getInteger("redis.hscan-snapshot-expiry", 30000);

  private int HSCAN_SNAPSHOTS_EXPIRE_CHECK_FREQUENCY_MILLISECONDS;
  private int MINIMUM_MILLISECONDS_FOR_HSCAN_SNAPSHOTS_TO_LIVE;


  @VisibleForTesting
  public RedisHash(List<ByteArrayWrapper> fieldsToSet, int hscanSnapShotExpirationCheckFrequency,
                   int minimumLifeForHscanSnaphot) {
    this();
    this.HSCAN_SNAPSHOTS_EXPIRE_CHECK_FREQUENCY_MILLISECONDS =
        hscanSnapShotExpirationCheckFrequency;
    this.MINIMUM_MILLISECONDS_FOR_HSCAN_SNAPSHOTS_TO_LIVE = minimumLifeForHscanSnaphot;

    Iterator<ByteArrayWrapper> iterator = fieldsToSet.iterator();
    while (iterator.hasNext()) {
      hashPut(iterator.next(), iterator.next());
    }
  }

  public RedisHash(List<ByteArrayWrapper> fieldsToSet) {
    this(fieldsToSet,
        defaultHscanSnapshotsExpireCheckFrequency,
        defaultHscanSnapshotsMillisecondsToLive);
  }

  // for serialization
  public RedisHash() {
    this.hash = new HashMap<>();
    this.hScanSnapShots = new ConcurrentHashMap<>();
    this.hScanSnapShotCreationTimes = new ConcurrentHashMap<>();

    this.HSCAN_SNAPSHOTS_EXPIRE_CHECK_FREQUENCY_MILLISECONDS =
        this.defaultHscanSnapshotsExpireCheckFrequency;

    this.MINIMUM_MILLISECONDS_FOR_HSCAN_SNAPSHOTS_TO_LIVE =
        this.defaultHscanSnapshotsMillisecondsToLive;

    calibrate_memory_values();
  }

  private static int baseRedisHashOverhead;

  private static int hashMapInternalValuePairOverhead;
  private static int sizeOfOverheadOfFirstPair;
  private static int internalHashMapStorageOverhead;


  private void calibrate_memory_values() {
    ReflectionObjectSizer reflectionObjectSizer = ReflectionObjectSizer.getInstance();

    myCalculatedSize = reflectionObjectSizer.sizeof(this);


    HashMap<ByteArrayWrapper, ByteArrayWrapper> temp_hashmap = new HashMap<>();

    int base_hashmap_size = reflectionObjectSizer.sizeof(temp_hashmap);

    ByteArrayWrapper field1 = new ByteArrayWrapper("a".getBytes());
    ByteArrayWrapper value1 = new ByteArrayWrapper("b".getBytes());
    ByteArrayWrapper field2 = new ByteArrayWrapper("c".getBytes());
    ByteArrayWrapper value2 = new ByteArrayWrapper("d".getBytes());

    temp_hashmap.put(field1, value1);

    int hashmapWithSinglePairSize = reflectionObjectSizer.sizeof(temp_hashmap);

    int one_entry_hashmap_size = reflectionObjectSizer.sizeof(temp_hashmap);

    temp_hashmap.put(field2, value2);

    int two_entries_hashmap_size = reflectionObjectSizer.sizeof(temp_hashmap);

    int sizeOfDataForOneFieldValuePair = 2;

    hashMapInternalValuePairOverhead =
        two_entries_hashmap_size - one_entry_hashmap_size - sizeOfDataForOneFieldValuePair;

    sizeOfOverheadOfFirstPair = hashmapWithSinglePairSize - base_hashmap_size
        - hashMapInternalValuePairOverhead - sizeOfDataForOneFieldValuePair;

    System.out.println("3 letters in byte array wrapper: "
        + "" +reflectionObjectSizer.sizeof(new ByteArrayWrapper("abc".getBytes())));

    System.out.println("byte array wrapper: "
        + "" +reflectionObjectSizer.sizeof(new ByteArrayWrapper()));

    //wrong!!! correct before use!!!
//    internalHashMapStorageOverhead =
//        two_entries_hashmap_size - (2 * hashMapInternalValuePairOverhead) - base_hashmap_size;

  }



  private void expireHScanSnapshots() {
    this.hScanSnapShotCreationTimes.entrySet().forEach(entry -> {
      Long creationTime = entry.getValue();
      long millisecondsSinceCreation = currentTimeMillis() - creationTime;

      if (millisecondsSinceCreation >= MINIMUM_MILLISECONDS_FOR_HSCAN_SNAPSHOTS_TO_LIVE) {
        UUID client = entry.getKey();
        removeHSCANSnapshot(client);
      }
    });
  }

  @VisibleForTesting
  public ConcurrentHashMap<UUID, List<ByteArrayWrapper>> getHscanSnapShots() {
    return this.hScanSnapShots;
  }

  private void startHscanSnapshotScheduledRemoval() {
    final int DELAY = this.HSCAN_SNAPSHOTS_EXPIRE_CHECK_FREQUENCY_MILLISECONDS;

    this.HSCANSnapshotExpirationExecutor =
        newSingleThreadScheduledExecutor("GemFireRedis-HSCANSnapshotRemoval-");

    this.HSCANSnapshotExpirationExecutor.scheduleWithFixedDelay(
        this::expireHScanSnapshots, DELAY, DELAY, MILLISECONDS);
  }

  private void shutDownHscanSnapshotScheduledRemoval() {
    this.HSCANSnapshotExpirationExecutor.shutdown();
    this.HSCANSnapshotExpirationExecutor = null;
  }

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads are modifying this
   * object, the striped executor will not protect toData. So any methods that modify "hash" needs
   * to be thread safe with toData.
   */
  @Override
  public synchronized void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeHashMap(hash, out);
    DataSerializer.writeInteger(myCalculatedSize, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    hash = DataSerializer.readHashMap(in);
    myCalculatedSize = DataSerializer.readInteger(in);
  }

  @Override
  public int getDSFID() {
    return REDIS_HASH_ID;
  }


  private synchronized ByteArrayWrapper hashPut(ByteArrayWrapper field, ByteArrayWrapper value) {
    if (this.hash.isEmpty()){
      this.myCalculatedSize +=sizeOfOverheadOfFirstPair;
    }

    ByteArrayWrapper oldvalue = hash.put(field, value);

    if (oldvalue == null) {
      int fieldValuePairSize =
          (int) (hashMapInternalValuePairOverhead + Math.ceil(field.length()/8) + Math.ceil(value.length()/8));

      System.out.println("hashMapInternalValuePairOverhead: " + hashMapInternalValuePairOverhead);
      System.out.println("field size length: " + field.length());
      System.out.println("value size length : " + value.length());


      System.out.println("adding to non-existing value using calculated weights: " + fieldValuePairSize);

      this.myCalculatedSize += fieldValuePairSize;

    } else {

      int newValueSize = value.length() - oldvalue.length();

      System.out.println("adding to existing value" + newValueSize);

      this.myCalculatedSize += newValueSize;
    }

    return oldvalue;
  }

  private synchronized ByteArrayWrapper hashPutIfAbsent(ByteArrayWrapper field,
                                                        ByteArrayWrapper value) {
    ByteArrayWrapper oldvalue = hash.putIfAbsent(field, value);

    if (oldvalue == null) {
      myCalculatedSize += 2 * hashMapInternalValuePairOverhead + field.length() + value.length();
    }
    return oldvalue;
  }

  private synchronized ByteArrayWrapper hashRemove(ByteArrayWrapper field) {
    ByteArrayWrapper oldValue = hash.remove(field);
    if (oldValue != null) {
      myCalculatedSize -= hashMapInternalValuePairOverhead + field.length() + oldValue.length();
    }
    return oldValue;
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

  public int hset(Region<RedisKey, RedisData> region, RedisKey key,
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

  public int hdel(Region<RedisKey, RedisData> region, RedisKey key,
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

  public ImmutablePair<Integer, List<Object>> hscan(UUID clientID, Pattern matchPattern,
                                                    int count, int startCursor) {

    List<ByteArrayWrapper> keysToScan = getSnapShotOfKeySet(clientID);

    Pair<Integer, List<Object>> resultsPair =
        getResultsPair(keysToScan, startCursor, count, matchPattern);

    List<Object> resultList = resultsPair.getRight();

    Integer numberOfIterationsCompleted = resultsPair.getLeft();

    int returnCursorValueAsInt =
        getCursorValueToReturn(startCursor, numberOfIterationsCompleted, keysToScan);

    if (returnCursorValueAsInt == 0) {
      removeHSCANSnapshot(clientID);
    }

    return new ImmutablePair<>(returnCursorValueAsInt, resultList);
  }

  private void removeHSCANSnapshot(UUID clientID) {
    this.hScanSnapShots.remove(clientID);
    this.hScanSnapShotCreationTimes.remove(clientID);

    if (this.hScanSnapShots.isEmpty()) {
      shutDownHscanSnapshotScheduledRemoval();
    }
  }

  @SuppressWarnings("unchecked")
  private Pair<Integer, List<Object>> getResultsPair(List<ByteArrayWrapper> keysSnapShot,
                                                     int startCursor,
                                                     int count,
                                                     Pattern matchPattern) {

    int indexOfKeys = startCursor;

    List<ByteArrayWrapper> resultList = new ArrayList<>();

    for (int index = startCursor; index < keysSnapShot.size(); index++) {
      if ((index - startCursor) == count) {
        break;
      }

      ByteArrayWrapper key = keysSnapShot.get(index);
      indexOfKeys++;
      ByteArrayWrapper value = hash.get(key);
      if (value == null) {
        continue;
      }

      if (matchPattern != null) {
        if (matchPattern.matcher(key.toString()).matches()) {
          resultList.add(key);
          resultList.add(value);
        }
      } else {
        resultList.add(key);
        resultList.add(value);
      }
    }

    Integer numberOfIterationsCompleted = indexOfKeys - startCursor;

    return new ImmutablePair(numberOfIterationsCompleted, resultList);
  }

  private int getCursorValueToReturn(int startCursor,
                                     int numberOfIterationsCompleted,
                                     List<ByteArrayWrapper> keySnapshot) {

    if (startCursor + numberOfIterationsCompleted >= keySnapshot.size()) {
      return 0;
    }

    return (startCursor + numberOfIterationsCompleted);
  }

  @SuppressWarnings("unchecked")
  private List<ByteArrayWrapper> getSnapShotOfKeySet(UUID clientID) {
    List<ByteArrayWrapper> keySnapShot = this.hScanSnapShots.get(clientID);

    if (keySnapShot == null) {
      if (this.hScanSnapShots.isEmpty()) {
        startHscanSnapshotScheduledRemoval();
      }
      keySnapShot = createKeySnapShot(clientID);
    }
    return keySnapShot;
  }

  @SuppressWarnings("unchecked")
  private List<ByteArrayWrapper> createKeySnapShot(UUID clientID) {

    List<ByteArrayWrapper> keySnapShot = new ArrayList<>(hash.keySet());

    this.hScanSnapShots.put(clientID, keySnapShot);
    this.hScanSnapShotCreationTimes.put(clientID, currentTimeMillis());

    return keySnapShot;
  }

  public long hincrby(Region<RedisKey, RedisData> region, RedisKey key,
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

  public BigDecimal hincrbyfloat(Region<RedisKey, RedisData> region, RedisKey key,
                                 ByteArrayWrapper field, BigDecimal increment)
      throws NumberFormatException {
    ByteArrayWrapper oldValue = hash.get(field);
    if (oldValue == null) {
      ByteArrayWrapper newValue = new ByteArrayWrapper(Coder.bigDecimalToBytes(increment));
      hashPut(field, newValue);
      AddsDeltaInfo deltaInfo = new AddsDeltaInfo();
      deltaInfo.add(field);
      deltaInfo.add(newValue);
      storeChanges(region, key, deltaInfo);
      return increment.stripTrailingZeros();
    }

    String valueS = oldValue.toString();
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

    ByteArrayWrapper modifiedValue = new ByteArrayWrapper(Coder.bigDecimalToBytes(value));
    hashPut(field, modifiedValue);
    AddsDeltaInfo deltaInfo = new AddsDeltaInfo();
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
    return Objects.equals(hash, redisHash.hash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), hash);
  }

  @Override
  public String toString() {
    return "RedisHash{" + super.toString() + ", " + "hash=" + hash + '}';
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getSizeInBytes() {
    return myCalculatedSize;
  }

  @VisibleForTesting
  protected static int getPerByteArrayWrapperOverhead() {
    return PER_BYTE_ARRAY_WRAPPER_OVERHEAD;
  }

//  @VisibleForTesting
//  protected static int getPerHashOverhead() {
//    return PER_HASH_OVERHEAD;
//  }

  @VisibleForTesting
  protected HashMap<ByteArrayWrapper, ByteArrayWrapper> getHashMap() {
    return hash;
  }
}
