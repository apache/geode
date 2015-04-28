/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

@Category(UnitTest.class)
public class OrderedTombstoneMapJUnitTest extends TestCase {
  
  public void test() {
    OrderedTombstoneMap<String> map = new OrderedTombstoneMap<String>();
    
    DiskStoreID id1 = DiskStoreID.random();
    DiskStoreID id2 = DiskStoreID.random();
    map.put(createVersionTag(id1, 1, 7), "one");
    map.put(createVersionTag(id1, 3, 2), "two");
    map.put(createVersionTag(id2, 3, 5), "three");
    map.put(createVersionTag(id1, 2, 3), "four");
    map.put(createVersionTag(id1, 0, 2), "five");
    map.put(createVersionTag(id2, 4, 4), "six");
    
    //Now make sure we get the entries in the order we expect (ordered by version tag with a member
    //and by timestampe otherwise.
    assertEquals("five", map.take().getValue());
    assertEquals("three", map.take().getValue());
    assertEquals("six", map.take().getValue());
    assertEquals("one", map.take().getValue());
    assertEquals("four", map.take().getValue());
    assertEquals("two", map.take().getValue());
  }

  private VersionTag createVersionTag(DiskStoreID id, long regionVersion, long timeStamp) {
    VersionTag tag = VersionTag.create(id);
    tag.setRegionVersion(regionVersion);
    tag.setVersionTimeStamp(timeStamp);
    return tag;
  }

}
