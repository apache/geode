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
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.DiskStoreImpl.OplogEntryIdSet;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Tests DiskStoreImpl.OplogEntryIdSet
 */
@Category(UnitTest.class)
public class OplogEntryIdSetJUnitTest {

  @Test
  public void testBasics() {
    OplogEntryIdSet s = new OplogEntryIdSet();
    for (long i=1; i < 777777; i++) {
      assertEquals(false, s.contains(i));
    }
    for (long i=1; i < 777777; i++) {
      s.add(i);
    }
    for (long i=1; i < 777777; i++) {
      assertEquals(true, s.contains(i));
    }

    try {
      s.add(DiskStoreImpl.INVALID_ID);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    assertEquals(false, s.contains(0));

    assertEquals(false, s.contains(0x00000000FFFFFFFFL));
    s.add(0x00000000FFFFFFFFL);
    assertEquals(true, s.contains(0x00000000FFFFFFFFL));

    for (long i=0x00000000FFFFFFFFL+1; i < 0x00000000FFFFFFFFL+777777; i++) {
      assertEquals(false, s.contains(i));
    }
    for (long i=0x00000000FFFFFFFFL+1; i < 0x00000000FFFFFFFFL+777777; i++) {
      s.add(i);
    }
    for (long i=0x00000000FFFFFFFFL+1; i < 0x00000000FFFFFFFFL+777777; i++) {
      assertEquals(true, s.contains(i));
    }

    for (long i=1; i < 777777; i++) {
      assertEquals(true, s.contains(i));
    }

    assertEquals(false, s.contains(Long.MAX_VALUE));
    s.add(Long.MAX_VALUE);
    assertEquals(true, s.contains(Long.MAX_VALUE));
    assertEquals(false, s.contains(Long.MIN_VALUE));
    s.add(Long.MIN_VALUE);
    assertEquals(true, s.contains(Long.MIN_VALUE));
  }
}
