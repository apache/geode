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
import static org.apache.geode.redis.internal.RedisConstants.REDIS_LIST_DATA_SERIALIZABLE_ID;
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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RedisException;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.data.collections.SizeableByteArrayList;
import org.apache.geode.redis.internal.data.delta.AddByteArrays;
import org.apache.geode.redis.internal.data.delta.AddByteArraysTail;
import org.apache.geode.redis.internal.data.delta.InsertByteArray;
import org.apache.geode.redis.internal.data.delta.RemoveElementsByIndex;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrayAtOffset;
import org.apache.geode.redis.internal.data.delta.RetainElementsByIndexRange;
import org.apache.geode.redis.internal.eventing.BlockingCommandListener;
import org.apache.geode.redis.internal.eventing.NotificationEvent;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;
import org.apache.geode.redis.internal.services.RegionProvider;

public class RedisList extends AbstractRedisData {

  static {
    Instantiator.register(new Instantiator(RedisList.class, REDIS_LIST_DATA_SERIALIZABLE_ID) {
      public DataSerializable newInstance() {
        return new RedisList();
      }
    });
  }

  protected static final int REDIS_LIST_OVERHEAD = memoryOverhead(RedisList.class);
  private final SizeableByteArrayList elementList;

  private static final int INVALID_INDEX = -1;

  public RedisList() {
    this.elementList = new SizeableByteArrayList();
  }

  public RedisList(RedisList redisList) {
    setExpirationTimestampNoDelta(redisList.getExpirationTimestamp());
    setVersion(redisList.getVersion());
    elementList = new SizeableByteArrayList(redisList.elementList);
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
      index = elementList.insert(elementToInsert, referenceElement, before);
      if (index == -1) {
        return index;
      }
      newVersion = incrementAndGetVersion();
    }
    storeChanges(region, key, new InsertByteArray(newVersion, elementToInsert, index));

    return elementList.size();
  }

  /**
   * @return the number of elements in the list
   */
  public int llen() {
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
      popped = elementList.remove(0);
    }
    removed = new RemoveElementsByIndex(newVersion);
    removed.add(0);
    storeChanges(region, key, removed);
    return popped;
  }

  /**
   * @param context the context of the executing command
   * @param elementsToAdd elements to add to this list
   * @param key the name of the list to add to
   * @param onlyIfExists if true then the elements should only be added if the key already exists
   *        and holds a list, otherwise no operation is performed.
   * @return the length of the list after the operation
   */
  public long lpush(ExecutionHandlerContext context, List<byte[]> elementsToAdd,
      RedisKey key, boolean onlyIfExists) {
    byte newVersion;
    synchronized (this) {
      newVersion = incrementAndGetVersion();
      for (byte[] element : elementsToAdd) {
        elementPushHead(element);
      }
    }
    storeChanges(context.getRegion(), key, new AddByteArrays(elementsToAdd, newVersion));
    context.fireEvent(NotificationEvent.LPUSH, key);

    return elementList.size();
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
   * @param count number of elements to remove.
   *        A count that is 0 removes all matching elements in the list.
   *        Positive count starts from the head and moves to the tail.
   *        Negative count starts from the tail and moves to the head.
   * @param element element to remove
   * @param region the region this instance is stored in
   * @param key the name of the list to add
   * @return amount of elements that were actually removed
   */
  public int lrem(int count, byte[] element, Region<RedisKey, RedisData> region, RedisKey key) {
    List<Integer> removedIndexes;
    byte newVersion;
    synchronized (this) {
      removedIndexes = elementList.remove(element, count);
      if (removedIndexes.isEmpty()) {
        return 0;
      }

      newVersion = incrementAndGetVersion();
    }

    storeChanges(region, key,
        new RemoveElementsByIndex(newVersion, removedIndexes));
    return removedIndexes.size();
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

    elementList.set(adjustedIndex, value);
    storeChanges(region, key, new ReplaceByteArrayAtOffset(index, value));
  }

  /**
   * @param start the index of the first element to retain
   * @param end the index of the last element to retain
   * @param region the region this instance is stored in
   * @param key the name of the list to pop from
   */
  public Void ltrim(int start, int end, Region<RedisKey, RedisData> region,
      RedisKey key) {
    int length = elementList.size();
    int boundedStart = getBoundedStartIndex(start, length);
    int boundedEnd = getBoundedEndIndex(end, length);

    if (boundedStart > boundedEnd || boundedStart == length) {
      // Remove everything
      region.remove(key);
      return null;
    }

    if (boundedStart == 0 && boundedEnd == length - 1) {
      // No-op, return without modifying the list
      return null;
    }

    RetainElementsByIndexRange retainElementsByRange;
    synchronized (this) {
      elementsRetainByIndexRange(boundedStart, boundedEnd);

      retainElementsByRange =
          new RetainElementsByIndexRange(incrementAndGetVersion(), boundedStart, boundedEnd);
    }
    storeChanges(region, key, retainElementsByRange);
    return null;
  }

  private int getBoundedStartIndex(int index, int size) {
    if (index >= 0L) {
      return Math.min(index, size);
    } else {
      return Math.max(index + size, 0);
    }
  }

  private int getBoundedEndIndex(int index, int size) {
    if (index >= 0L) {
      return Math.min(index, size - 1);
    } else {
      return Math.max(index + size, -1);
    }
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
    context.fireEvent(NotificationEvent.RPUSH, key);

    return elementList.size();
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
      popped = elementList.remove(index);
    }
    removed = new RemoveElementsByIndex(newVersion);
    removed.add(index);
    storeChanges(region, key, removed);
    return popped;
  }

  /**
   * @param context The {@link ExecutionHandlerContext} for this operation, passed to allow events
   *        to be triggered
   * @param source The {@link RedisKey} associated with the source RedisList
   * @param destination The {@link RedisKey} associated with the destination RedisList
   * @return The list element moved from source to destination, as a byte array
   */
  public static byte[] rpoplpush(ExecutionHandlerContext context, RedisKey source,
      RedisKey destination) {
    RegionProvider regionProvider = context.getRegionProvider();
    RedisList sourceList = regionProvider.getTypedRedisData(REDIS_LIST, source, false);

    if (sourceList.isNull()) {
      return null;
    }

    RedisList newSourceList = new RedisList(sourceList);
    Region<RedisKey, RedisData> region = regionProvider.getDataRegion();
    byte[] moved = newSourceList.rpop(region, source);

    if (source.equals(destination)) {
      newSourceList.lpush(context, Collections.singletonList(moved), destination, false);
    } else {
      RedisList destinationList = regionProvider.getTypedRedisData(REDIS_LIST, destination, false);
      new RedisList(destinationList).lpush(context, Collections.singletonList(moved), destination,
          false);
    }
    return moved;
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
    elementList.add(index, toInsert);
  }

  @Override
  public void applyRemoveElementsByIndex(List<Integer> indexes) {
    if (indexes.size() == 1) {
      elementList.remove((int) indexes.get(0));
    } else {
      elementList.removeIndexes(indexes);
    }
  }

  @Override
  public void applyReplaceByteArrayAtOffsetDelta(int offset, byte[] newValue) {
    elementList.set(offset, newValue);
  }

  @Override
  public void applyRetainElementsByIndexRange(int start, int end) {
    elementsRetainByIndexRange(start, end);
  }

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads are modifying this
   * object, the striped executor will not protect toData. So any methods that modify "elements"
   * needs to be thread safe with toData.
   */
  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveInt(elementList.size(), out);
    for (byte[] element : elementList) {
      DataSerializer.writeByteArray(element, out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    int size = DataSerializer.readPrimitiveInt(in);
    for (int i = 0; i < size; ++i) {
      byte[] element = DataSerializer.readByteArray(in);
      elementList.addLast(element);
    }
  }

  protected void elementPushHead(byte[] element) {
    elementList.addFirst(element);
  }

  private void elementsRetainByIndexRange(int start, int end) {
    if (end < elementList.size()) {
      elementList.clearSublist(end + 1, elementList.size());
    }

    if (start > 0) {
      elementList.clearSublist(0, start);
    }
  }

  protected void elementPushTail(byte[] element) {
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
  public int getSizeInBytes() {
    return REDIS_LIST_OVERHEAD + elementList.getSizeInBytes();
  }
}
