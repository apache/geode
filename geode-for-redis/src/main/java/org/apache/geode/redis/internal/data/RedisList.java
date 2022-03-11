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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.data.collections.SizeableByteArrayList;
import org.apache.geode.redis.internal.data.delta.AddByteArrays;
import org.apache.geode.redis.internal.data.delta.AddByteArraysTail;
import org.apache.geode.redis.internal.data.delta.RemoveElementsByIndex;

public class RedisList extends AbstractRedisData {
  protected static final int REDIS_LIST_OVERHEAD = memoryOverhead(RedisList.class);
  private final SizeableByteArrayList elementList;

  private static final int INVALID_INDEX = -1;

  public RedisList() {
    this.elementList = new SizeableByteArrayList();
  }

  /**
   * @param start start index of desired elements
   * @param stop stop index of desired elements
   * @return list of elements in the range (inclusive).
   */
  public List<byte[]> lrange(int start, int stop) {
    start = normalizeStartIndex(start);
    stop = normalizeStopIndex(stop);

    int elementSize = elementList.size();
    if (start > stop || elementSize <= start) {
      return Collections.emptyList();
    }

    int resultLength = stop - start + 1;

    // Finds the shortest distance to access nodes in range
    if (start <= elementSize - stop - 1) {
      // Starts at head to access nodes at start index then iterates forwards
      List<byte[]> result = new ArrayList<>(resultLength);
      ListIterator<byte[]> iterator = elementList.listIterator(start);

      for (int i = start; i <= stop; i++) {
        byte[] element = iterator.next();
        result.add(element);
      }
      return result;

    } else {
      // Starts at tail to access nodes at stop index then iterates backwards
      byte[][] result = new byte[resultLength][];
      ListIterator<byte[]> iterator = elementList.listIterator(stop + 1);

      for (int i = resultLength - 1; i >= 0; i--) {
        byte[] element = iterator.previous();
        result[i] = element;
      }
      return Arrays.asList(result);
    }
  }

  /**
   * @param index index of desired element. Positive index starts at the head. Negative index starts
   *        at the tail.
   * @return element at index. Null if index is out of range.
   */
  public byte[] lindex(int index) {
    index = getArrayIndex(index);

    if (index == INVALID_INDEX || elementList.size() <= index) {
      return null;
    } else {
      return elementList.get(index);
    }
  }

  private int normalizeStartIndex(int startIndex) {
    return Math.max(0, getArrayIndex(startIndex));
  }

  private int normalizeStopIndex(int stopIndex) {
    return Math.min(elementList.size() - 1, getArrayIndex(stopIndex));
  }

  /**
   * Changes negative index to corresponding positive index.
   * If there is no corresponding positive index, returns INVALID_INDEX.
   */
  private int getArrayIndex(int listIndex) {
    if (listIndex < 0) {
      listIndex = elementList.size() + listIndex;
      if (listIndex < 0) {
        return INVALID_INDEX;
      }
    }
    return listIndex;
  }

  /**
   * @param elementsToAdd elements to add to this list; NOTE this list may be modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the list to add to
   * @param onlyIfExists if true then the elements should only be added if the key already exists
   *        and holds a list, otherwise no operation is performed.
   * @return the length of the list after the operation
   */
  public long lpush(List<byte[]> elementsToAdd, Region<RedisKey, RedisData> region, RedisKey key,
      final boolean onlyIfExists) {
    elementsPushHead(elementsToAdd);
    storeChanges(region, key, new AddByteArrays(elementsToAdd));
    return elementList.size();
  }

  /**
   * @param elementsToAdd elements to add to this list; NOTE this list may be modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the list to add to
   * @param onlyIfExists if true then the elements should only be added if the key already exists
   *        and holds a list, otherwise no operation is performed.
   * @return the length of the list after the operation
   */
  public long rpush(List<byte[]> elementsToAdd, Region<RedisKey, RedisData> region, RedisKey key,
      final boolean onlyIfExists) {
    elementsPushTail(elementsToAdd);
    storeChanges(region, key, new AddByteArraysTail(elementsToAdd));
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
    elementPushHead(bytes);
  }

  @Override
  public void applyAddByteArrayTailDelta(byte[] bytes) {
    elementPushTail(bytes);
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

  public synchronized void elementPushHead(byte[] element) {
    elementList.addFirst(element);
  }

  public synchronized void elementsPushHead(List<byte[]> elementsToAdd) {
    for (byte[] element : elementsToAdd) {
      elementPushHead(element);
    }
  }

  public synchronized void elementPushTail(byte[] element) {
    elementList.addLast(element);
  }

  public synchronized void elementsPushTail(List<byte[]> elementsToAdd) {
    for (byte[] element : elementsToAdd) {
      elementPushTail(element);
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
