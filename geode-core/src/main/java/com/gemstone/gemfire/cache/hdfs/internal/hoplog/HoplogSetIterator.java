/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HFileSortedOplog.HFileReader.HFileSortedIterator;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.ByteComparator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.TrackedReference;

/**
 * Provides a merged iterator on set of {@link HFileSortedOplog}
 */
public class HoplogSetIterator implements HoplogIterator<ByteBuffer, ByteBuffer> {
  private final List<HFileSortedIterator> iters;

  // Number of entries remaining to be iterated by this scanner
  private int entriesRemaining;

  // points at the current iterator holding the next entry
  private ByteBuffer currentKey;
  private ByteBuffer currentValue;

  public HoplogSetIterator(List<TrackedReference<Hoplog>> targets) throws IOException {
    iters = new ArrayList<HFileSortedIterator>();
    for (TrackedReference<Hoplog> oplog : targets) {
      HFileSortedIterator iter = (HFileSortedIterator) oplog.get().getReader().scan();
      if (!iter.hasNext()) {
        // the oplog is empty, exclude from iterator
        continue;
      }

      // initialize the iterator
      iter.nextBB();
      iters.add(iter);
      entriesRemaining += oplog.get().getReader().getEntryCount();
    }
  }

  public boolean hasNext() {
    return entriesRemaining > 0;
  }

  @Override
  public ByteBuffer next() throws IOException {
    return nextBB();
  }
  public ByteBuffer nextBB() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    seekToMinKeyIter();

    return currentKey;
  }

  private void seekToMinKeyIter() throws IOException {
    HFileSortedIterator currentIter = null;
    ByteBuffer minKey = null;

    // scan through all hoplog iterators to reach to the iterator with smallest
    // key on the head and remove duplicate keys
    for (Iterator<HFileSortedIterator> iterator = iters.iterator(); iterator.hasNext();) {
      HFileSortedIterator iter = iterator.next();
      
      ByteBuffer tmpK = iter.getKeyBB();
      ByteBuffer tmpV = iter.getValueBB();
      if (minKey == null || ByteComparator.compareBytes(tmpK.array(), tmpK.arrayOffset(), tmpK.remaining(), minKey.array(), minKey.arrayOffset(), minKey.remaining()) < 0) {
        minKey = tmpK;
        currentKey = tmpK;
        currentValue = tmpV;
        currentIter = iter;
      } else {
        // remove possible duplicate key entries from iterator
        if (seekHigherKeyInIter(minKey, iter) == null) {
          // no more keys left in this iterator
          iter.close();
          iterator.remove();
        }
      }
    }
    
    //seek next key in current iter
    if (currentIter != null && seekHigherKeyInIter(minKey, currentIter) == null) {
      // no more keys left in this iterator
      currentIter.close();
      iters.remove(currentIter);
    }
  }

  private ByteBuffer seekHigherKeyInIter(ByteBuffer key, HFileSortedIterator iter) throws IOException {
    ByteBuffer newK = iter.getKeyBB();

    // remove all duplicates by incrementing iterator when a key is less than
    // equal to current key
    while (ByteComparator.compareBytes(newK.array(), newK.arrayOffset(), newK.remaining(), key.array(), key.arrayOffset(), key.remaining()) <= 0) {
      entriesRemaining--;
      if (iter.hasNext()) {
        newK = iter.nextBB();
      } else {
        newK = null;
        break;
      }
    }
    return newK;
  }

  @Override
  public ByteBuffer getKey() {
    return getKeyBB();
  }
  public ByteBuffer getKeyBB() {
    if (currentKey == null) {
      throw new IllegalStateException();
    }
    return currentKey;
  }

  @Override
  public ByteBuffer getValue() {
    return getValueBB();
  }
  public ByteBuffer getValueBB() {
    if (currentValue == null) {
      throw new IllegalStateException();
    }
    return currentValue;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    for (HoplogIterator<byte[], byte[]> iter : iters) {
      iter.close();
    }
  }

  public int getRemainingEntryCount() {
    return entriesRemaining;
  }
}
