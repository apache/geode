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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.junit.Test;

public class BitSetExceptionIteratorTest {

  @Test
  public void findsLeadingException() {
    BitSet bitSet = new BitSet(10);
    bitSet.set(1, 10);

    BitSetExceptionIterator exceptionIterator = new BitSetExceptionIterator(bitSet, 50, 55);
    assertContainsExceptions(exceptionIterator, RVVException.createException(49, 51));
  }


  @Test
  public void findsTrailingException() {
    BitSet bitSet = new BitSet(10);
    bitSet.set(0, 6);

    BitSetExceptionIterator exceptionIterator = new BitSetExceptionIterator(bitSet, 50, 57);
    assertContainsExceptions(exceptionIterator, RVVException.createException(55, 57));

  }

  @Test
  public void findsTrailingExceptionDueToLargeVersion() {
    BitSet bitSet = new BitSet(10);
    bitSet.set(0, 10);

    BitSetExceptionIterator exceptionIterator = new BitSetExceptionIterator(bitSet, 50, 61);
    assertContainsExceptions(exceptionIterator, RVVException.createException(59, 61));
  }

  @Test
  public void ignoresExceptionsPastEndVersion() {
    BitSet bitSet = new BitSet(10);
    bitSet.set(0, 8);

    BitSetExceptionIterator exceptionIterator = new BitSetExceptionIterator(bitSet, 50, 57);
    assertContainsExceptions(exceptionIterator);
  }

  @Test
  public void ignoresExceptionsIfNextVersionIsOnePastTheEndOfFullBitset() {
    BitSet bitSet = new BitSet(10);
    bitSet.set(0, 10);

    BitSetExceptionIterator exceptionIterator = new BitSetExceptionIterator(bitSet, 50, 60);
    assertContainsExceptions(exceptionIterator);
  }

  @Test
  public void findsMiddleException() {
    BitSet bitSet = new BitSet(10);
    bitSet.set(0, 4);
    bitSet.set(6, 10);

    BitSetExceptionIterator exceptionIterator = new BitSetExceptionIterator(bitSet, 50, 59);
    assertContainsExceptions(exceptionIterator, RVVException.createException(53, 56));

  }

  private void assertContainsExceptions(BitSetExceptionIterator exceptionIterator,
      RVVException... expectedExceptions) {
    List<RVVException> foundExceptions = new ArrayList<>();
    exceptionIterator.forEachRemaining(foundExceptions::add);
    RegionVersionHolderUtilities.assertSameExceptions(foundExceptions, expectedExceptions);
  }
}
