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
package org.apache.geode.internal.cache.versions;

import java.util.BitSet;
import java.util.Iterator;

/**
 * An iterator on a bit set that produces {@link RVVException} for gaps
 * in the set bits in the bitset.
 */
public class BitSetExceptionIterator implements Iterator<RVVException> {
  private final BitSet bitSet;
  private final long bitSetVersion;
  private final long maximumVersion;
  private long nextClearBit;

  /**
   * Create a new bitset iterator
   *
   * @param bitSet The bitset to iterate on
   * @param bitSetVersion An offset of the bitset. If this iterator generates a RVVException,
   *        the previous and next values will include this base value.
   * @param maximumVersion The value to stop creating exceptions at. This should be greater than
   *        bitSetVersion. This may be within the bitSetVersion+bitSet.size, or it could be much
   *        large.
   *        If this value falls within the bitset, gaps past this value will not be returned as
   *        RVVExceptions
   *        by this iterator.
   */
  public BitSetExceptionIterator(BitSet bitSet, long bitSetVersion, long maximumVersion) {
    this.bitSet = bitSet;
    this.bitSetVersion = bitSetVersion;
    this.maximumVersion = maximumVersion;
    nextClearBit = findNextClearBit(bitSet, 0);
  }

  /**
   * Find the next clear bit from a given index in the bitset, that is less than or
   * equal to our maximum version for this iterator.
   *
   * @return the next clear bit, or -1 if there is no next clear bit within the range.
   */
  private int findNextClearBit(BitSet bitSet, int fromIndex) {
    int nextClearBit = bitSet.nextClearBit(fromIndex);

    long maxmimumClearBit = maximumVersion - bitSetVersion;
    if (nextClearBit >= maxmimumClearBit) {
      // We found empty bits, but past the offset we are interested in
      // Ignore these
      return -1;
    }

    return nextClearBit;
  }

  @Override
  public boolean hasNext() {
    return nextClearBit != -1;
  }

  @Override
  public RVVException next() {
    if (!hasNext()) {
      return null;
    }

    int nextSetBit = bitSet.nextSetBit((int) Math.min(Integer.MAX_VALUE, nextClearBit));

    long nextSetVersion = nextSetBit == -1 ? maximumVersion : nextSetBit + bitSetVersion;

    RVVException exception =
        RVVException.createException(nextClearBit + bitSetVersion - 1, nextSetVersion);

    nextClearBit = nextSetBit == -1 ? -1 : findNextClearBit(bitSet, nextSetBit);

    return exception;
  }
}
