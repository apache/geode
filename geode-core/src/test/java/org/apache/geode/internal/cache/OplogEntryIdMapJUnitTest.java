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
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.Oplog.OplogEntryIdMap;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Tests DiskStoreImpl.OplogEntryIdMap
 */
@Category(UnitTest.class)
public class OplogEntryIdMapJUnitTest {

  @Test
  public void testBasics() {
    OplogEntryIdMap m = new OplogEntryIdMap();
    for (long i = 1; i <= 777777; i++) {
      assertEquals(null, m.get(i));
    }
    for (long i = 1; i <= 777777; i++) {
      m.put(i, new Long(i));
    }
    for (long i = 1; i <= 777777; i++) {
      assertEquals(new Long(i), m.get(i));
    }

    assertEquals(777777, m.size());

    try {
      m.put(DiskStoreImpl.INVALID_ID, new Object());
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
    assertEquals(null, m.get(0));
    assertEquals(777777, m.size());

    assertEquals(null, m.get(0x00000000FFFFFFFFL));
    m.put(0x00000000FFFFFFFFL, new Long(0x00000000FFFFFFFFL));
    assertEquals(new Long(0x00000000FFFFFFFFL), m.get(0x00000000FFFFFFFFL));
    assertEquals(777777 + 1, m.size());

    for (long i = 0x00000000FFFFFFFFL + 1; i <= 0x00000000FFFFFFFFL + 777777; i++) {
      assertEquals(null, m.get(i));
    }
    for (long i = 0x00000000FFFFFFFFL + 1; i <= 0x00000000FFFFFFFFL + 777777; i++) {
      m.put(i, new Long(i));
    }
    for (long i = 0x00000000FFFFFFFFL + 1; i <= 0x00000000FFFFFFFFL + 777777; i++) {
      assertEquals(new Long(i), m.get(i));
    }
    assertEquals(777777 + 1 + 777777, m.size());

    for (long i = 1; i < 777777; i++) {
      assertEquals(new Long(i), m.get(i));
    }

    assertEquals(null, m.get(Long.MAX_VALUE));
    m.put(Long.MAX_VALUE, new Long(Long.MAX_VALUE));
    assertEquals(new Long(Long.MAX_VALUE), m.get(Long.MAX_VALUE));
    assertEquals(777777 + 1 + 777777 + 1, m.size());
    assertEquals(null, m.get(Long.MIN_VALUE));
    m.put(Long.MIN_VALUE, new Long(Long.MIN_VALUE));
    assertEquals(new Long(Long.MIN_VALUE), m.get(Long.MIN_VALUE));
    assertEquals(777777 + 1 + 777777 + 1 + 1, m.size());

    int count = 0;
    for (OplogEntryIdMap.Iterator it = m.iterator(); it.hasNext();) {
      count++;
      it.advance();
      it.key();
      it.value();
    }
    assertEquals(777777 + 1 + 777777 + 1 + 1, count);
  }
}
