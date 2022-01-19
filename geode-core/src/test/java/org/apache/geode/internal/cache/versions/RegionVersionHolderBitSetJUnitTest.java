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

import static org.apache.geode.internal.cache.versions.RegionVersionHolder.BIT_SET_WIDTH;
import static org.apache.geode.internal.cache.versions.RegionVersionHolder2JUnitTest.createHolder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.junit.Test;

/**
 * Tests of how the RegionVersionHolder maintains it's bitset under updates
 */
public class RegionVersionHolderBitSetJUnitTest {

  @Test
  public void recordVersionLargerThanIntMaxValueShouldSucceed() {
    RegionVersionHolder h = createHolder(true);
    long version = ((long) Integer.MAX_VALUE) + 10L;
    h.recordVersion(version);
    assertEquals(version, h.getBitSetVersionForTesting());
    assertEquals(bitSet(0), h.getBitSetForTesting());
    assertContains(h, version);
    assertHasExceptions(h, RVVException.createException(0, version));
  }

  @Test
  public void initializeFromUpdatesBitSetVersionCorrectly() {
    RegionVersionHolder holder = createHolder(true);
    RegionVersionHolder other = createHolder(true);

    int moreThanBitSetWidth = BIT_SET_WIDTH + 10;
    holder.recordVersion(moreThanBitSetWidth);
    other.recordVersion(2);

    holder.initializeFrom(other);

    assertThat(holder.getBitSetVersionForTesting()).isEqualTo(moreThanBitSetWidth);
    assertEquals(bitSet(0), holder.getBitSetForTesting());
    assertHasExceptions(holder, RVVException.createException(2, moreThanBitSetWidth + 1),
        RVVException.createException(0, 2));

    holder.recordVersion(moreThanBitSetWidth);

    assertThat(holder.getBitSetVersionForTesting()).isEqualTo(moreThanBitSetWidth);
    assertEquals(bitSet(0), holder.getBitSetForTesting());
    assertContains(holder, 2, moreThanBitSetWidth);
    assertHasExceptions(holder, RVVException.createException(2, moreThanBitSetWidth),
        RVVException.createException(0, 2));
  }

  @Test
  public void recordVersionLessThanBitSetWidthShouldNotMoveBitSet() {
    RegionVersionHolder h = createHolder(true);
    int version = BIT_SET_WIDTH - 10;
    h.recordVersion(version);
    assertEquals(1, h.getBitSetVersionForTesting());
    assertEquals(bitSet(version - 1), h.getBitSetForTesting());
    assertContains(h, version);
    assertHasExceptions(h, RVVException.createException(0, version));
  }

  @Test
  public void recordVersionGreaterThanBitSetWidthShouldMoveBitSet() {
    RegionVersionHolder h = createHolder(true);
    long version = ((long) Integer.MAX_VALUE) - 10L;
    h.recordVersion(version);
    assertEquals(version, h.getBitSetVersionForTesting());
    assertEquals(bitSet(0), h.getBitSetForTesting());
    assertContains(h, version);
    assertHasExceptions(h, RVVException.createException(0, version));
  }

  @Test
  public void recordFirstVersionShouldSetFirstBit() {
    RegionVersionHolder h = createHolder(true);
    h.recordVersion(1);
    assertEquals(1, h.getBitSetVersionForTesting());
    assertEquals(bitSet(0), h.getBitSetForTesting());
    assertContains(h, 1);
    assertHasExceptions(h);
  }

  @Test
  public void recordLargeVersionWithSomeBitsSetShouldMoveBitSet() {
    RegionVersionHolder h = createHolder(true);

    LongStream.range(1, 10).forEach(h::recordVersion);
    long version = ((long) Integer.MAX_VALUE) - 10L;
    h.recordVersion(version);
    assertEquals(version, h.getBitSetVersionForTesting());
    assertEquals(bitSet(0), h.getBitSetForTesting());
    assertContains(h, 1, 2, 3, 4, 5, 6, 7, 8, 9, version);
    assertHasExceptions(h, RVVException.createException(9, version));
  }

  @Test
  public void recordOneVersionPastBitSetWidthShouldMoveBitSet() {
    RegionVersionHolder h = createHolder(true);

    LongStream.range(1, BIT_SET_WIDTH + 1).forEach(h::recordVersion);
    assertEquals(1, h.getBitSetVersionForTesting());
    BitSet expectedBitSet = new BitSet(BIT_SET_WIDTH);
    expectedBitSet.set(0, BIT_SET_WIDTH);
    assertEquals(expectedBitSet, h.getBitSetForTesting());

    h.recordVersion(BIT_SET_WIDTH + 1);
    assertEquals(BIT_SET_WIDTH * 3 / 4 + 1, h.getBitSetVersionForTesting());
    assertEquals(bitSet(IntStream.range(0, (BIT_SET_WIDTH / 4) + 1).toArray()),
        h.getBitSetForTesting());
    assertContains(h, LongStream.range(0, BIT_SET_WIDTH + 2).toArray());
    assertHasExceptions(h);
  }

  @Test
  public void recordVersionGreaterThanTwiceBitSetWidthShouldMoveBitSetAndCreateExceptions() {
    RegionVersionHolder h = createHolder(true);
    long version = ((long) Integer.MAX_VALUE) - 10L;
    LongStream.range(1, 10).forEach(h::recordVersion);
    h.recordVersion(15);
    h.recordVersion(version);
    assertEquals(version, h.getBitSetVersionForTesting());
    assertEquals(bitSet(0), h.getBitSetForTesting());
    assertContains(h, 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, version);
    assertHasExceptions(h,
        RVVException.createException(15, version),
        RVVException.createException(9, 15));
  }

  @Test
  public void recordVersionLessThanTwiceBitSetWidthShouldSlideBitSet() {
    RegionVersionHolder h = createHolder(true);
    LongStream.range(1, 10).forEach(h::recordVersion);
    h.recordVersion(15);
    int lessThanBitSetVersion = BIT_SET_WIDTH - 10;
    h.recordVersion(lessThanBitSetVersion);
    int moreThanBitSetVersion = BIT_SET_WIDTH + 10;
    h.recordVersion(moreThanBitSetVersion);

    // The bit set will only slide to start from 15, because that is the last set bit
    // less than 3/4 the size of the bit set
    assertEquals(15, h.getBitSetVersionForTesting());
    assertEquals(bitSet(0, lessThanBitSetVersion - 15, moreThanBitSetVersion - 15),
        h.getBitSetForTesting());

    assertContains(h, 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, lessThanBitSetVersion, moreThanBitSetVersion);
    assertHasExceptions(h,
        RVVException.createException(lessThanBitSetVersion, moreThanBitSetVersion),
        RVVException.createException(15, lessThanBitSetVersion),
        RVVException.createException(9, 15));
  }


  private void assertContains(RegionVersionHolder h, long... versions) {
    LongStream.of(versions).forEach(version -> {
      assertThat(h.contains(version))
          .withFailMessage("Did not contain %s", version)
          .isTrue();
    });
  }

  private void assertHasExceptions(RegionVersionHolder h, RVVException... exceptions) {
    List<RVVException> actualExceptions = h.getExceptionForTest();
    RegionVersionHolderUtilities.assertSameExceptions(actualExceptions, exceptions);
  }

  private BitSet bitSet(int... setBits) {
    BitSet bitSet = new BitSet(BIT_SET_WIDTH);
    IntStream.of(setBits).forEach(bitSet::set);
    return bitSet;
  }
}
