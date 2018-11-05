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
package org.apache.geode.internal;


/**
 * UniqueIdGenerator is factory that will produce unique ids that fall in a range between 0 and
 * numIds-1 inclusive. Obtained ids will not be reissued until they are released and until every
 * other non-obtained id has been obtained first.
 * <p>
 * Memory Use: this implementation allocates numIds/64 8-byte longs at construction time and keeps
 * them allocated.
 * <p>
 * Instances of this class are thread safe.
 *
 * @since GemFire 5.0.2
 *
 */
public class UniqueIdGenerator {
  /*
   * BitSets are packed into arrays of "units." Currently a unit is a long, which consists of 64
   * bits, requiring 6 address bits. The choice of unit is determined purely by performance
   * concerns.
   */
  private static final int ADDRESS_BITS_PER_UNIT = 6;
  private static final int BITS_PER_UNIT = 1 << ADDRESS_BITS_PER_UNIT;
  private static final int BIT_INDEX_MASK = BITS_PER_UNIT - 1;

  /**
   * The units in this BitSet. The ith bit is stored in units[i/64] at bit position i % 64 (where
   * bit position 0 refers to the least significant bit and 63 refers to the most significant bit).
   */
  private final long units[];
  /**
   * The maximum id supported by this generator
   */
  private final int MAX_ID;
  /**
   * The next id to return from obtain if it is available.
   */
  private int ctr = 0;

  /**
   * Given a bit index return unit index containing it.
   */
  private static int unitIndex(int bitIndex) {
    return bitIndex >> ADDRESS_BITS_PER_UNIT;
  }

  /**
   * Given a bit index, return a unit that masks that bit in its unit.
   */
  private static long bit(int bitIndex) {
    return 1L << (bitIndex & BIT_INDEX_MASK);
  }

  /**
   * Sets the bit at the specified index to <code>true</code> if it is not already in the set.
   * Otherwise does nothing.
   *
   * @param bitIndex a bit index.
   * @exception IndexOutOfBoundsException if the specified index is negative.
   */
  private void setBit(int bitIndex) {
    int unitIndex = unitIndex(bitIndex);
    long bit = bit(bitIndex);
    this.units[unitIndex] |= bit;
  }

  /**
   * Sets the bit specified by the index to <code>false</code>.
   *
   * @param bitIndex the index of the bit to be cleared.
   */
  private void clearBit(int bitIndex) {
    int unitIndex = unitIndex(bitIndex);
    long bit = bit(bitIndex);
    this.units[unitIndex] &= ~bit;
  }

  /*
   * trailingZeroTable[i] is the number of trailing zero bits in the binary representation of i.
   */
  private static final byte trailingZeroTable[] = {-25, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
      4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1,
      0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0,
      1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2,
      0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0,
      2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1,
      0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0, 3, 0,
      1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3,
      0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0};

  private static int trailingZeroCnt(long val) {
    // Loop unrolled for performance
    int byteVal = (int) val & 0xff;
    if (byteVal != 0) {
      return trailingZeroTable[byteVal];
    }

    byteVal = (int) (val >>> 8) & 0xff;
    if (byteVal != 0) {
      return trailingZeroTable[byteVal] + 8;
    }

    byteVal = (int) (val >>> 16) & 0xff;
    if (byteVal != 0) {
      return trailingZeroTable[byteVal] + 16;
    }

    byteVal = (int) (val >>> 24) & 0xff;
    if (byteVal != 0) {
      return trailingZeroTable[byteVal] + 24;
    }

    byteVal = (int) (val >>> 32) & 0xff;
    if (byteVal != 0) {
      return trailingZeroTable[byteVal] + 32;
    }

    byteVal = (int) (val >>> 40) & 0xff;
    if (byteVal != 0) {
      return trailingZeroTable[byteVal] + 40;
    }

    byteVal = (int) (val >>> 48) & 0xff;
    if (byteVal != 0) {
      return trailingZeroTable[byteVal] + 48;
    }

    byteVal = (int) (val >>> 56) & 0xff;
    return trailingZeroTable[byteVal] + 56;
  }

  private static final long WORD_MASK = 0xffffffffffffffffL;

  /**
   * Returns the index of the first bit that is set to <code>false</code> that occurs on or after
   * the specified starting index.
   *
   * @param fromIndex the index to start checking from (inclusive).
   * @return the index of the next clear bit.
   */
  private int nextClearBit(int fromIndex) {
    int u = unitIndex(fromIndex);
    int testIndex = (fromIndex & BIT_INDEX_MASK);
    long unit = this.units[u] >> testIndex;

    if (unit == (WORD_MASK >> testIndex)) {
      testIndex = 0;
    }

    while ((unit == WORD_MASK) && (u < this.units.length - 1)) {
      unit = this.units[++u];
    }

    if (unit == WORD_MASK) {
      return -1; // could not find a clear bit
    }

    int result;
    if (unit == 0) {
      result = u * BITS_PER_UNIT + testIndex;
    } else {
      testIndex += trailingZeroCnt(~unit);
      result = ((u * BITS_PER_UNIT) + testIndex);
    }
    if (result > MAX_ID || result < 0) {
      return -1;
    } else {
      return result;
    }
  }

  /**
   * Creates unique id pool that has numIds in it.
   *
   * @param numIds the maximum number of ids
   */
  public UniqueIdGenerator(int numIds) {
    if (numIds <= 0) {
      throw new IllegalArgumentException(
          "numIds < 0");
    }
    this.units = new long[(unitIndex(numIds - 1) + 1)];
    this.MAX_ID = numIds - 1;
    this.ctr = 0;
  }

  /**
   * Obtain an id form the pool of available ids and return it.
   *
   * @throws IllegalStateException if all ids have been obtained
   */
  public int obtain() {
    synchronized (this) {
      int candidate = this.ctr;
      int result = nextClearBit(candidate);
      if (result == -1 && candidate != 0) {
        // The following check will do some additional work by scanning
        // the range [candidate..MAX_ID] again if it does not find a clear bit
        // in the range [0..candidate-1] but it will only do this when
        // we are going to throw an exception.
        result = nextClearBit(0);
      }
      if (result == -1) {
        throw new IllegalStateException(
            "Ran out of message ids");
      } else {
        setBit(result);
        if (result == MAX_ID) {
          this.ctr = 0;
        } else {
          this.ctr = result + 1;
        }
        return result;
      }
    }
  }

  /**
   * Release a previously obtained id allowing it to be obtained in the future.
   */
  public void release(int id) {
    if (id < 0) {
      throw new IllegalArgumentException(
          String.format("negative id: %s", Integer.valueOf(id)));
    } else if (id > this.MAX_ID) {
      throw new IllegalArgumentException(
          String.format("id > MAX_ID: %s", Integer.valueOf(id)));
    }
    synchronized (this) {
      clearBit(id);
    }
  }
}
