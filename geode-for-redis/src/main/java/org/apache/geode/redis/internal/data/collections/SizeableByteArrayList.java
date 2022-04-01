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
 */
package org.apache.geode.redis.internal.data.collections;

import static org.apache.geode.internal.JvmSizeUtils.getObjectHeaderSize;
import static org.apache.geode.internal.JvmSizeUtils.getReferenceSize;
import static org.apache.geode.internal.JvmSizeUtils.memoryOverhead;
import static org.apache.geode.internal.JvmSizeUtils.roundUpSize;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.geode.internal.size.Sizeable;

public class SizeableByteArrayList extends LinkedList<byte[]> implements Sizeable {
  private static final int BYTE_ARRAY_LIST_OVERHEAD = memoryOverhead(SizeableByteArrayList.class);
  private static final int NODE_OVERHEAD =
      roundUpSize(getObjectHeaderSize() + 3 * getReferenceSize());
  private int memberOverhead;

  /**
   * @param elementToRemove element to remove from the list
   * @param count number of elements that match object o to remove from the list.
   *        Count that is equal to 0 removes all matching elements from the list.
   * @return list of indexes that were removed in order.
   */
  public List<Integer> remove(byte[] elementToRemove, int count) {
    if (0 <= count) {
      count = count == 0 ? this.size() : count;
      return removeObjectsStartingAtHead(elementToRemove, count);
    } else {
      return removeObjectsStartingAtTail(elementToRemove, -count);
    }
  }

  private List<Integer> removeObjectsStartingAtHead(byte[] elementToRemove, int count) {
    int index = 0;
    ListIterator<byte[]> iterator = listIterator(index);
    List<Integer> indexesRemoved = new LinkedList<>();

    while (iterator.hasNext() && count != indexesRemoved.size()) {
      byte[] element = iterator.next();
      if (Arrays.equals(element, elementToRemove)) {
        iterator.remove();
        memberOverhead -= calculateByteArrayOverhead(element);
        indexesRemoved.add(index);
      }

      index++;
    }
    return indexesRemoved;
  }

  private List<Integer> removeObjectsStartingAtTail(byte[] elementToRemove, int count) {
    int index = size() - 1;
    ListIterator<byte[]> descendingIterator = listIterator(size());
    List<Integer> indexesRemoved = new LinkedList<>();

    while (descendingIterator.hasPrevious() && indexesRemoved.size() != count) {
      byte[] element = descendingIterator.previous();
      if (Arrays.equals(element, elementToRemove)) {
        descendingIterator.remove();
        memberOverhead -= calculateByteArrayOverhead(element);
        indexesRemoved.add(0, index);
      }

      index--;
    }
    return indexesRemoved;
  }

  public void clearSublist(int fromIndex, int toIndex) {
    if (fromIndex < size() - toIndex) {
      clearFromBeginning(fromIndex, toIndex);
    } else {
      clearFromEnd(fromIndex, toIndex);
    }
  }

  private void clearFromBeginning(int fromIndex, int toIndex) {
    ListIterator<byte[]> iterator = listIterator(fromIndex);
    int removeCount = toIndex - fromIndex;
    int count = 0;

    while (iterator.hasNext() && count < removeCount) {
      byte[] element = iterator.next();
      iterator.remove();
      count++;
      memberOverhead -= calculateByteArrayOverhead(element);
    }
  }

  private void clearFromEnd(int fromIndex, int toIndex) {
    ListIterator<byte[]> descendingIterator = listIterator(toIndex);
    int removeCount = toIndex - fromIndex;
    int count = 0;

    while (descendingIterator.hasPrevious() && count < removeCount) {
      byte[] element = descendingIterator.previous();
      descendingIterator.remove();
      count++;
      memberOverhead -= calculateByteArrayOverhead(element);
    }
  }

  @Override
  public int indexOf(Object o) {
    ListIterator<byte[]> iterator = this.listIterator();
    while (iterator.hasNext()) {
      int index = iterator.nextIndex();
      byte[] element = iterator.next();
      if (Arrays.equals(element, (byte[]) o)) {
        return index;
      }
    }
    return -1;
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new UnsupportedOperationException();
  }

  /**
   * @param remove in order (smallest to largest) list of indexes to remove
   */
  public void removeIndexes(List<Integer> remove) {
    int removeIndex = 0;
    int firstIndexToRemove = remove.get(0);
    ListIterator<byte[]> iterator = listIterator(firstIndexToRemove);

    // Iterates only through the indexes to remove
    for (int i = firstIndexToRemove; i <= remove.get(remove.size() - 1); i++) {
      byte[] element = iterator.next();
      if (i == remove.get(removeIndex)) {
        iterator.remove();
        memberOverhead -= calculateByteArrayOverhead(element);
        removeIndex++;
      }
    }
  }

  @Override
  public boolean remove(Object o) {
    ListIterator<byte[]> iterator = this.listIterator();
    while (iterator.hasNext()) {
      byte[] element = iterator.next();
      if (Arrays.equals(element, (byte[]) o)) {
        iterator.remove();
        memberOverhead -= calculateByteArrayOverhead(element);
        return true;
      }
    }
    return false;
  }

  @Override
  public byte[] remove(int index) {
    byte[] element = super.remove(index);
    memberOverhead -= calculateByteArrayOverhead(element);
    return element;
  }

  @Override
  public void addFirst(byte[] element) {
    memberOverhead += calculateByteArrayOverhead(element);
    super.addFirst(element);
  }

  @Override
  public void addLast(byte[] element) {
    memberOverhead += calculateByteArrayOverhead(element);
    super.addLast(element);
  }

  public boolean removeLastOccurrence(Object o) {
    throw new UnsupportedOperationException();
  }

  private int calculateByteArrayOverhead(byte[] element) {
    return NODE_OVERHEAD + memoryOverhead(element);
  }

  @Override
  public int getSizeInBytes() {
    return BYTE_ARRAY_LIST_OVERHEAD + memberOverhead;
  }

  @Override
  public int hashCode() {
    final int primeNumber = 31;
    int hashCode = 1;
    for (byte[] element : this) {
      hashCode = hashCode * primeNumber + Arrays.hashCode(element);
    }
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SizeableByteArrayList)) {
      return false;
    }
    SizeableByteArrayList sizeableByteArrayList = (SizeableByteArrayList) o;
    if (sizeableByteArrayList.size() != this.size()) {
      return false;
    }
    ListIterator<byte[]> otherListIterator = ((SizeableByteArrayList) o).listIterator();
    ListIterator<byte[]> thisListIterator = (this.listIterator());
    while (thisListIterator.hasNext()) {
      if (!Arrays.equals(thisListIterator.next(), otherListIterator.next())) {
        return false;
      }
    }
    return true;
  }
}
