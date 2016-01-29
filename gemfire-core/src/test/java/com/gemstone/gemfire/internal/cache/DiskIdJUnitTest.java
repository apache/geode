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
package com.gemstone.gemfire.internal.cache;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;

/**
 * 
 * Test verifies the setting and getting of disk id values are correctly 
 * 
 * @author Mitul Bid
 *
 */
@Category(UnitTest.class)
public class DiskIdJUnitTest extends TestCase
{

  /**
   * Test the getOplogId returns what has been set
   * 
   * @throws Exception
   */
  public void testGetSetOplogId() throws Exception
  {
    DiskId did = getDiskId();
    did.setOplogId(-1);
    assertEquals(-1, did.getOplogId());
    did.setOplogId(0);
    assertEquals(0, did.getOplogId());
    did.setOplogId(1024);
    assertEquals(1024, did.getOplogId());
    did.setOplogId(-1024);
    assertEquals(-1024, did.getOplogId());
  }

  /**
   * Test the getUserbits returns what has been set
   * 
   * @throws Exception
   */
 
  public void testGetSetUserBits() throws Exception
  {
    DiskId did = getDiskId();
    byte userBits = 0;
    userBits = EntryBits.setSerialized(userBits, true);
    did.setUserBits(userBits);
    assertEquals(userBits, did.getUserBits());
    userBits = EntryBits.setInvalid(userBits, true);
    did.setUserBits(userBits);
    assertEquals(userBits, did.getUserBits());
    userBits = EntryBits.setLocalInvalid(userBits, true);
    did.setUserBits(userBits);
    assertEquals(userBits, did.getUserBits());
    assertTrue(EntryBits.isSerialized(userBits));
    assertTrue(EntryBits.isInvalid(userBits));
    assertTrue(EntryBits.isLocalInvalid(userBits));
    userBits = EntryBits.setSerialized(userBits, false);
    did.setUserBits(userBits);
    assertEquals(userBits, did.getUserBits());
    userBits = EntryBits.setInvalid(userBits, false);
    did.setUserBits(userBits);
    assertEquals(userBits, did.getUserBits());
    userBits = EntryBits.setLocalInvalid(userBits, false);
    did.setUserBits(userBits);
    assertFalse(EntryBits.isSerialized(userBits));
    assertFalse(EntryBits.isInvalid(userBits));
    assertFalse(EntryBits.isLocalInvalid(userBits));    
    
    userBits = 0x0;
    userBits = EntryBits.setSerialized(userBits, true);
    did.setUserBits(userBits);
    assertTrue(EntryBits.isSerialized(userBits));
    assertFalse(EntryBits.isInvalid(userBits));
    assertFalse(EntryBits.isLocalInvalid(userBits));
    
    userBits = 0x0;
    userBits = EntryBits.setInvalid(userBits, true);
    did.setUserBits(userBits);
    assertFalse(EntryBits.isSerialized(userBits));
    assertTrue(EntryBits.isInvalid(userBits));
    assertFalse(EntryBits.isLocalInvalid(userBits));
    
    userBits = 0x0;
    userBits = EntryBits.setLocalInvalid(userBits, true);
    did.setUserBits(userBits);
    assertFalse(EntryBits.isSerialized(userBits));
    assertFalse(EntryBits.isInvalid(userBits));
    assertTrue(EntryBits.isLocalInvalid(userBits));
    
    userBits = 0x0;
    userBits = EntryBits.setTombstone(userBits, true);
    userBits = EntryBits.setWithVersions(userBits, true);
    did.setUserBits(userBits);
    assertFalse(EntryBits.isLocalInvalid(userBits));
    assertFalse(EntryBits.isSerialized(userBits));
    assertFalse(EntryBits.isInvalid(userBits));
    assertTrue(EntryBits.isTombstone(userBits));
    assertTrue(EntryBits.isWithVersions(userBits));
  }

  /**
   * Test the whether setting of one set of values does not affect another set of values
   */
 
  public void testAllOperationsValidatingResult1()
  {
    DiskId did = getDiskId();
    for (int i = -16777215; i < 16777215; i++) {
      boolean boolValuePerIteration = false;
      did.setOplogId(i);
      // set true for even, set false for odd
      switch ((i % 3 )) {
      case 0:
        boolValuePerIteration = true;
        break;
      case 1:
      case 2:  
        boolValuePerIteration = false;
        break;         
      }
      byte userbits = 0;
      switch (i % 4) {
      case 0:
        break;
      case 1:
        did.setUserBits(EntryBits
            .setSerialized(userbits, boolValuePerIteration));
        break;
      case 2:
        did.setUserBits(EntryBits.setInvalid(userbits, boolValuePerIteration));
        break;
      case 3:
        did.setUserBits(EntryBits.setLocalInvalid(userbits,
            boolValuePerIteration));
        break;
      }
      assertEquals(did.getOplogId(), i);
      byte userBits2 = did.getUserBits();
      switch (i % 4) {
      case 0:
        break;
      case 1:
        assertEquals(EntryBits.isSerialized(userBits2), boolValuePerIteration);
        break;
      case 2:
        assertEquals(EntryBits.isInvalid(userBits2), boolValuePerIteration);
        break;
      case 3:
        assertEquals(EntryBits.isLocalInvalid(userBits2), boolValuePerIteration);
        break;
      }
    }

  }
  
  /**
   * Tests that an instance of 'PersistenceIntOplogOffsetDiskId' is created when
   * max-oplog-size (in bytes) passed is smaller than Integer.MAX_VALUE
   */
  public void testPersistIntDiskIdInstance()
  {
    int maxOplogSizeinMB = 2;

    DiskId diskId = DiskId.createDiskId(maxOplogSizeinMB, true /*is persistence type*/, true);
    assertTrue(
        "Instance of 'PersistIntOplogOffsetDiskId' was not created though max oplog size (in bytes) was smaller than Integer.MAX_VALUE",
        DiskId.isInstanceofPersistIntOplogOffsetDiskId(diskId));
  }

  /**
   * Tests that an instance of 'LongOplogOffsetDiskId' is created when
   * max-oplog-size (in bytes) passed is greater than Integer.MAX_VALUE
   */
  public void testPersistLongDiskIdInstance()
  {
    long maxOplogSizeInBytes = (long)Integer.MAX_VALUE + 1;
    int maxOplogSizeinMB = (int)(maxOplogSizeInBytes / (1024 * 1024));

    DiskId diskId = DiskId.createDiskId(maxOplogSizeinMB, true/* is persistence type */, true);
    assertTrue(
        "Instance of 'PersistLongOplogOffsetDiskId' was not created though max oplog size (in bytes) was greater than Integer.MAX_VALUE",
        DiskId.isInstanceofPersistLongOplogOffsetDiskId(diskId));
  }
  
  /**
   * Tests that an instance of 'PersistenceIntOplogOffsetDiskId' is created when
   * max-oplog-size (in bytes) passed is smaller than Integer.MAX_VALUE
   */
  public void testOverflowIntDiskIdInstance()
  {
    int maxOplogSizeinMB = 2;

    DiskId diskId = DiskId.createDiskId(maxOplogSizeinMB, false /*is overflow type*/, true);
    assertTrue(
        "Instance of 'OverflowIntOplogOffsetDiskId' was not created though max oplog size (in bytes) was smaller than Integer.MAX_VALUE",
        DiskId.isInstanceofOverflowIntOplogOffsetDiskId(diskId));   
  }

  /**
   * Tests that an instance of 'LongOplogOffsetDiskId' is created when
   * max-oplog-size (in bytes) passed is greater than Integer.MAX_VALUE
   */
  public void testOverflowLongDiskIdInstance()
  {
    long maxOplogSizeInBytes = (long)Integer.MAX_VALUE + 1;
    int maxOplogSizeinMB = (int)(maxOplogSizeInBytes / (1024 * 1024));

    DiskId diskId = DiskId.createDiskId(maxOplogSizeinMB, false/* is overflow type */, true);
    assertTrue(
        "Instance of 'OverflowLongOplogOffsetDiskId' was not created though max oplog size (in bytes) was greater than Integer.MAX_VALUE",
        DiskId.isInstanceofOverflowOnlyWithLongOffset(diskId));
  }

  private DiskId getDiskId()
  {
    return DiskId.createDiskId(1024, true /* is persistence type*/, true);
  }

}
