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
import static org.apache.geode.redis.internal.RedisConstants.ERROR_INDEX_OUT_OF_RANGE;
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
import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.data.collections.SizeableByteArrayList;
import org.apache.geode.redis.internal.data.delta.AddByteArrays;
import org.apache.geode.redis.internal.data.delta.AddByteArraysTail;
import org.apache.geode.redis.internal.data.delta.InsertByteArray;
import org.apache.geode.redis.internal.data.delta.RemoveElementsByIndex;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrayAtOffset;
import org.apache.geode.redis.internal.eventing.BlockingCommandListener;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.services.RegionProvider;
import org.apache.geode.redis.internal.data.delta.RemoveElementsByIndexReverseOrder;

public class RedisList extends AbstractRedisData {
  protected static final int REDIS_LIST_OVERHEAD = memoryOverhead(RedisList.class);
  private final SizeableByteArrayList elementList;

  private static final int INVALID_INDEX = -1;

  public RedisList() {
    this.elementList = new SizeableByteArrayList();
  }

  /**
   * @param count number of elements to remove.
   *        A count that is 0 removes all matching elements in the list.
   *        Positive count starts from the head and moves to the tail.
   *        Negative count starts from the tail and moves to the head.
   * @param element element to remove
   * @param region the region this instance is stored in
   * @param key the name of the set to add
   * @return amount of elements that were actually removed
   */
  public int lrem(int count, byte[] element, Region<RedisKey, RedisData> region, RedisKey key) {
    List<Integer> removedIndexes;
    byte version;
    synchronized (this) {
      removedIndexes = elementList.remove(element, count);
      version = incrementAndGetVersion();
    }

    if (!removedIndexes.isEmpty()) {
      storeChanges(region, key,
          new RemoveElementsByIndex(version, removedIndexes));
    }

    return removedIndexes.size();
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
   * @param index index of desired element.
   *        Positive index starts at the head.
   *        Negative index starts at the tail.
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
   * @param elementToInsert element to insert into the list
   * @param referenceElement element to insert next to
   * @param before true if inserting before reference element, false if it is after
   * @param region the region this instance is store in
   * @param key the name of the list to add to
   * @return the number of elements in the list after the element is inserted,
   *         or -1 if the pivot is not found.
   */
  public int linsert(byte[] elementToInsert, byte[] referenceElement, boolean before,
      Region<RedisKey, RedisData> region, RedisKey key) {
    byte newVersion;
    int index;

    synchronized (this) {
      index = elementInsert(elementToInsert, referenceElement, before);
      if (index == -1) {
        return index;
      }
      newVersion = incrementAndGetVersion();
    }
    storeChanges(region, key, new InsertByteArray(newVersion, elementToInsert, index));

    return elementList.size();
  }

  /**
   * @param context the context of the executing command
   * @param elementsToAdd elements to add to this list
   * @param key the name of the set to add to
   * @param onlyIfExists if true then the elements should only be added if the key already exists
   *        and holds a list, otherwise no operation is performed.
   * @return the length of the list after the operation
   */
  public long lpush(ExecutionHandlerContext context, List<byte[]> elementsToAdd,
      RedisKey key, boolean onlyIfExists) {
    byte newVersion;
    synchronized (this) {
      newVersion = incrementAndGetVersion();
      elementsPushHead(elementsToAdd);
    }
    storeChanges(context.getRegion(), key, new AddByteArrays(elementsToAdd, newVersion));
    context.fireEvent(RedisCommandType.LPUSH, key);

    return elementList.size();
  }

  /**
   * @param context the context of the executing command
   * @param elementsToAdd elements to add to this list;
   * @param key the name of the list to add to
   * @param onlyIfExists if true then the elements should only be added if the key already exists
   *        and holds a list, otherwise no operation is performed.
   * @return the length of the list after the operation
   */
  public long rpush(ExecutionHandlerContext context, List<byte[]> elementsToAdd, RedisKey key,
      boolean onlyIfExists) {
    byte newVersion;
    synchronized (this) {
      newVersion = incrementAndGetVersion();
      elementsToAdd.forEach(this::elementPushTail);
    }
    storeChanges(context.getRegion(), key, new AddByteArraysTail(newVersion, elementsToAdd));
    context.fireEvent(RedisCommandType.RPUSH, key);

    return elementList.size();
  }

  /**
   * @param region the region this instance is stored in
   * @param key the name of the list to add to
   * @return the element actually popped
   */
  public byte[] lpop(Region<RedisKey, RedisData> region, RedisKey key) {
    byte newVersion;
    byte[] popped;
    RemoveElementsByIndex removed;
    synchronized (this) {
      newVersion = incrementAndGetVersion();
      popped = removeElement(0);
    }
    removed = new RemoveElementsByIndex(newVersion);
    removed.add(0);
    storeChanges(region, key, removed);
    return popped;
  }

  public static List<byte[]> blpop(ExecutionHandlerContext context, Command command,
      List<RedisKey> keys, double timeoutSeconds) {
    RegionProvider regionProvider = context.getRegionProvider();
    for (RedisKey key : keys) {
      RedisList list = regionProvider.getTypedRedisData(REDIS_LIST, key, false);
      if (!list.isNull()) {
        byte[] poppedValue = list.lpop(context.getRegion(), key);

        // return the key and value
        List<byte[]> result = new ArrayList<>(2);
        result.add(key.toBytes());
        result.add(poppedValue);
        return result;
      }
    }

    context.registerListener(new BlockingCommandListener(context, command, keys, timeoutSeconds));

    return null;
  }

  /**
   * @return the number of elements in the list
   */
  public int llen() {
    return elementList.size();
  }

  /**
   * @param region the region to set on
   * @param key the key to set on
   * @param index the index specified by the user
   * @param value the value to set
   */
  public void lset(Region<RedisKey, RedisData> region, RedisKey key, int index, byte[] value) {
    int listSize = elementList.size();
    int adjustedIndex = index >= 0 ? index : listSize + index;
    if (adjustedIndex > listSize - 1 || adjustedIndex < 0) {
      throw new RedisException(ERROR_INDEX_OUT_OF_RANGE);
    }

    elementReplace(adjustedIndex, value);
    storeChanges(region, key, new ReplaceByteArrayAtOffset(index, value));
  }

  /**
   * @param region the region this instance is stored in
   * @param key the name of the list to add to
   * @return the element actually popped
   */
  public byte[] rpop(Region<RedisKey, RedisData> region, RedisKey key) {
    byte newVersion;
    int index = elementList.size() - 1;
    byte[] popped;
    RemoveElementsByIndex removed;
    synchronized (this) {
      newVersion = incrementAndGetVersion();
      popped = removeElement(index);
    }
    removed = new RemoveElementsByIndex(newVersion);
    removed.add(index);
    storeChanges(region, key, removed);
    return popped;
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
  public void applyInsertByteArrayDelta(byte[] toInsert, int index) {
    elementInsert(toInsert, index);
  }

  @Override
  public void applyRemoveElementsByIndex(List<Integer> indexes) {
    if (indexes.size() == 1) {
      elementRemove(indexes.get(0));
    } else {
      elementList.removeIndexes(indexes);
    }
  }

  @Override
  public void applyReplaceByteArrayAtOffsetDelta(int offset, byte[] newValue) {
    elementReplace(offset, newValue);
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

  public synchronized int elementInsert(byte[] elementToInsert, byte[] referenceElement,
      boolean before) {
    int i = 0;
    ListIterator<byte[]> iterator = elementList.listIterator();

    while (iterator.hasNext()) {
      if (Arrays.equals(iterator.next(), referenceElement)) {
        if (before) {
          iterator.previous();
          iterator.add(elementToInsert);
          return i;
        } else {
          iterator.add(elementToInsert);
          return i + 1;
        }
      }
      i++;
    }

    return -1;
  }

  public synchronized void elementInsert(byte[] toInsert, int index) {
    elementList.add(index, toInsert);
  }

  public synchronized byte[] removeElement(int index) {
    return elementList.remove(index);
  }

  protected synchronized void elementPushHead(byte[] element) {
    elementList.addFirst(element);
  }

  protected synchronized void elementsPushHead(List<byte[]> elementsToAdd) {
    for (byte[] element : elementsToAdd) {
      elementPushHead(element);
    }
  }

  public synchronized void elementReplace(int index, byte[] newValue) {
    elementList.set(index, newValue);
  }

  protected synchronized void elementPushTail(byte[] element) {
    elementList.addLast(element);
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
