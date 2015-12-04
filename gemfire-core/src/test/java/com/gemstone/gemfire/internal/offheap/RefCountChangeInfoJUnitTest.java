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
package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class RefCountChangeInfoJUnitTest {

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testGetOwner() {

    String owner1 = new String("Info1");
    String notOwner1 = new String("notInfo1");

    RefCountChangeInfo refInfo1 = new RefCountChangeInfo(true, 1, owner1);

    assertEquals(owner1, refInfo1.getOwner());

    try {
      assertEquals(owner1, notOwner1);
      fail("Expected owner1 != notOwner1");
    } catch (AssertionError e) {
      // Ignore expected error
    }

  }

  @Test
  public void testGetDupCount() {

    String owner1 = new String("Info1");
    String owner2 = new String("Info2");

    RefCountChangeInfo refInfo1 = new RefCountChangeInfo(true, 1, owner1);
    assertEquals(0, refInfo1.getDupCount());

    RefCountChangeInfo refInfo2 = new RefCountChangeInfo(true, 1, owner1);
    assertTrue(refInfo1.isDuplicate(refInfo2));
    assertEquals(1, refInfo1.getDupCount());

    // owner not used in isDup
    RefCountChangeInfo refInfo3 = new RefCountChangeInfo(true, 1, owner2);
    assertTrue(refInfo1.isDuplicate(refInfo3));
    assertEquals(2, refInfo1.getDupCount());

    RefCountChangeInfo refInfo4 = new RefCountChangeInfo(false, 1, owner2);
    assertFalse(refInfo1.isDuplicate(refInfo4));
    assertEquals(2, refInfo1.getDupCount());

  }

  @Test
  public void testDecDupCount() {

    String owner1 = new String("Info1");
    String owner2 = new String("Info2");

    RefCountChangeInfo refInfo1 = new RefCountChangeInfo(true, 1, owner1);
    assertEquals(0, refInfo1.getDupCount());

    RefCountChangeInfo refInfo2 = new RefCountChangeInfo(true, 1, owner1);
    assertTrue(refInfo1.isDuplicate(refInfo2));
    assertEquals(1, refInfo1.getDupCount());

    // owner not used in isDuplicate check
    RefCountChangeInfo refInfo3 = new RefCountChangeInfo(true, 1, owner2);
    assertTrue(refInfo1.isDuplicate(refInfo3));
    assertEquals(2, refInfo1.getDupCount());

    refInfo1.decDupCount();
    assertEquals(1, refInfo1.getDupCount());

    refInfo1.decDupCount();
    assertEquals(0, refInfo1.getDupCount());

  }

  @Test
  public void testToString() {

    String owner1 = new String("Info1");

    RefCountChangeInfo refInfo1 = new RefCountChangeInfo(true, 1, owner1);

    RefCountChangeInfo refInfo2 = new RefCountChangeInfo(true, 1, owner1);
    assertEquals(refInfo1.toString(), refInfo2.toString());

    RefCountChangeInfo refInfo3 = new RefCountChangeInfo(false, 1, owner1);
    try {
      assertEquals(refInfo1.toString(), refInfo3.toString());
      fail("expected refInfo1.toString() != refInfo3.toString()");
    } catch (AssertionError e) {
      // ignore expected IllegalArgumentException
    }

    RefCountChangeInfo refInfo4 = new RefCountChangeInfo(true, 2, owner1);
    try {
      assertEquals(refInfo1.toString(), refInfo4.toString());
      fail("expected refInfo1.toString() != refInfo4.toString()");
    } catch (AssertionError e) {
      // ignore expected IllegalArgumentException
    }

  }

  @Test
  public void testIsDuplicate() {

    String owner1 = new String("Info1");
    String owner2 = new String("Info2");

    RefCountChangeInfo refInfo1 = new RefCountChangeInfo(true, 1, owner1);
    assertEquals(0, refInfo1.getDupCount());

    RefCountChangeInfo refInfo2 = new RefCountChangeInfo(true, 1, owner1);
    assertTrue(refInfo1.isDuplicate(refInfo2));
    assertEquals(1, refInfo1.getDupCount());

    RefCountChangeInfo refInfo3 = new RefCountChangeInfo(false, 1, owner1);
    assertFalse(refInfo1.isDuplicate(refInfo3));
    assertEquals(1, refInfo1.getDupCount());

    RefCountChangeInfo refInfo4 = new RefCountChangeInfo(true, 1, owner2);
    assertTrue(refInfo1.isDuplicate(refInfo4));
    assertEquals(2, refInfo1.getDupCount());

  }

}
