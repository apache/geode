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
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_LIST;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.data.collections.SizeableByteArrayList;
import org.apache.geode.redis.internal.data.delta.AddByteArrays;
import org.apache.geode.redis.internal.data.delta.RemoveElementsByIndex;

public class RedisList extends AbstractRedisData {
  protected static final int REDIS_LIST_OVERHEAD = memoryOverhead(RedisList.class);
  private final SizeableByteArrayList elementList;

  public RedisList() {
    this.elementList = new SizeableByteArrayList();
  }

  /**
   * @param elementsToAdd elements to add to this set; NOTE this list may by modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @return the number of elements actually added
   */
  public long lpush(List<byte[]> elementsToAdd, Region<RedisKey, RedisData> region, RedisKey key) {
    elementsPush(elementsToAdd);
    storeChanges(region, key, new AddByteArrays(elementsToAdd));
    return elementList.size();
  }

  /**
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @return the element actually popped
   */
  public byte[] lpop(Region<RedisKey, RedisData> region, RedisKey key) {
    byte[] popped = elementRemove(0);
    RemoveElementsByIndex removed = new RemoveElementsByIndex();
    removed.add(0);
    storeChanges(region, key, removed);
    return popped;
  }

  /**
   * @return the number of elements in the list
   */
  public int llen() {
    return elementList.size();
  }

  @Override
  public void applyAddByteArrayDelta(byte[] bytes) {
    elementPush(bytes);
  }

  @Override
  public void applyRemoveElementsByIndex(List<Integer> indexes) {
    for (int index : indexes) {
      elementRemove(index);
    }
  }

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads are modifying this
   * object, the striped executor will not protect toData. So any methods that modify "elements"
   * needs to be thread safe with toData.
   */

  @Override
  public synchronized void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writePrimitiveInt(elementList.size(), out);
    for (byte[] element : elementList) {
      DataSerializer.writeByteArray(element, out);
    }
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int size = DataSerializer.readPrimitiveInt(in);
    for (int i = 0; i < size; ++i) {
      byte[] element = DataSerializer.readByteArray(in);
      elementList.addLast(element);
    }
  }

  @Override
  public int getDSFID() {
    return REDIS_LIST_ID;
  }

  public synchronized byte[] elementRemove(int index) {
    return elementList.remove(index);
  }

  public synchronized boolean elementRemove(byte[] element) {
    return elementList.remove(element);
  }

  public synchronized void elementPush(byte[] element) {
    elementList.addFirst(element);
  }

  public synchronized void elementsPush(List<byte[]> elementsToAdd) {
    for (byte[] element : elementsToAdd) {
      elementPush(element);
    }
  }

  @Override
  public RedisDataType getType() {
    return REDIS_LIST;
  }

  @Override
  protected boolean removeFromRegion() {
    return elementList.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisList)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RedisList redisList = (RedisList) o;
    return elementList.equals(redisList.elementList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), elementList.hashCode());
  }

  @Override
  public String toString() {
    return "RedisList{" + super.toString() + ", " + "size=" + elementList.size() + '}';
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getSizeInBytes() {
    return REDIS_LIST_OVERHEAD + elementList.getSizeInBytes();
  }
}
