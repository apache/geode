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
/*
 * Created on Feb 20, 2006
 * 
 * TODO To change the template for this generated file go to Window -
 * Preferences - Java - Code Style - Code Templates
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * This test does a check that conflation in the buffer happen correctly
 * 
 * Conflation cases tested include:
 * <ul>
 * <li> create, modify
 * <li> create, destroy
 * <li> create, destroy, create
 * <li> create, invalidate
 * <li> create, invalidate
 * <li> create, invalidate, modify
 * </ul>
 * The test is done for persist only, overflow only and persist + overflow only (async modes).
 * 
 * @author Mitul Bid
 *
 */
@Category(IntegrationTest.class)
public class ConflationJUnitTest extends DiskRegionTestingBase
{
  private DiskRegionProperties diskProps = new DiskRegionProperties();

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    diskProps.setDiskDirs(dirs);
    diskProps.setBytesThreshold(100000000);
    diskProps.setTimeInterval(100000000);
    diskProps.setSynchronous(false);
  }



  protected void createOverflowOnly()
  {
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache,
        diskProps);
  }

  protected void createPersistOnly()
  {
    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);
  }

  protected void createOverflowAndPersist()
  {
    region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        diskProps);
  }

  /**
   * do a put followed by a put
   *  
   */
  void putAndPut()
  {
    region.put(new Integer(1), new Integer(1));
    region.put(new Integer(1), new Integer(2));
  }

  /**
   * do a put followed by a destroy on the same entry
   *  
   */
  void putAndDestroy()
  {
    region.put(new Integer(1), new Integer(1));
    try {
      region.destroy(new Integer(1));
    }
    catch (Exception e) {
      logWriter.error("Exception occured",e);
      fail(" failed to destory Integer");
    }
  }

  /**
   * do a put destroy the same entry and put it again
   *  
   */
  void putDestroyPut()
  {
    putAndDestroy();
    region.put(new Integer(1), new Integer(2));
  }

  /**
   * put a key and then invalidate it
   *  
   */
  void putAndInvalidate()
  {
    region.put(new Integer(1), new Integer(1));
    try {
      region.invalidate(new Integer(1));
    }
    catch (Exception e) {
      logWriter.error("Exception occured",e);
      fail(" failed to invalidate Integer");
    }
  }

  /**
   * put a key, invalidate it and the perform a put on it
   *  
   */
  void putInvalidatePut()
  {
    putAndInvalidate();
    region.put(new Integer(1), new Integer(2));
  }

  /**
   * do a create and then a put on the same key
   *  
   */
  void createAndPut()
  {
    try {
      region.create(new Integer(1), new Integer(1));
    }
    catch (Exception e) {
      logWriter.error("Exception occured",e);
       fail(" failed in trying to create");
    }
    region.put(new Integer(1), new Integer(2));
  }

  /**
   * do a create and then a destroy
   *  
   */
  void createAndDestroy()
  {
    try {
      region.create(new Integer(1), new Integer(1));
    }
    catch (Exception e) {
      logWriter.error("Exception occured",e);
      fail(" failed in trying to create");
    }
    try {
      region.destroy(new Integer(1));
    }
    catch (Exception e) {
      logWriter.error("Exception occured",e);
      fail(" failed to destory Integer");
    }
  }

  /**
   * do a create then destroy the entry and create it again
   *  
   */
  void createDestroyCreate()
  {
    createAndDestroy();
    try {
      region.create(new Integer(1), new Integer(2));
    }
    catch (Exception e) {
      logWriter.error("Exception occured",e);
      fail(" failed in trying to create");
    }
  }

  /**
   * create an entry and then invalidate it
   *  
   */
  void createAndInvalidate()
  {
    try {
      region.create(new Integer(1), new Integer(1));
    }
    catch (Exception e) {
      logWriter.error("Exception occured",e);
      fail(" failed in trying to create");
    }
    try {
      region.invalidate(new Integer(1));
    }
    catch (Exception e) {
      logWriter.error("Exception occured",e);
      fail(" failed to invalidate Integer");
    }
  }

  /**
   * create an entry, invalidate it and then perform a put on the same key
   *  
   */
  void createInvalidatePut()
  {
    createAndInvalidate();
    region.put(new Integer(1), new Integer(2));
  }

  /**
   * validate whether a modification of an entry was correctly done
   *  
   */
  void validateModification()
  {
    Collection entries = ((LocalRegion)region).entries.regionEntries();
    if (entries.size() != 1) {
      fail("expected size to be 1 but is not so");
    }
    RegionEntry entry = (RegionEntry)entries.iterator().next();
    DiskId id = ((DiskEntry)entry).getDiskId();
    Object obj = ((LocalRegion)region).getDiskRegion().get(id);
    if (!(obj.equals(new Integer(2)))) {
      fail(" incorrect modification");
    }
  }

  /**
   * validate whether nothing was written
   */  
 
  void validateNothingWritten()
  {
    Collection entries = ((LocalRegion)region).entries.regionEntries();
    //We actually will have a tombstone in the region, hence
    //the 1 entry
    if (entries.size() != 1) {
      fail("expected size to be 1 but is " + entries.size());
    }
    assertEquals(this.flushCount, getCurrentFlushCount());
//     Oplog oplog = ((LocalRegion)region).getDiskRegion().getChild();
//     if (oplog.getOplogSize() != 0) {
//       fail(" expected zero bytes to have been written but is "
//           + oplog.getOplogSize());
//     }
  }
  
  /**
   * validate whether invalidate was done
   *  
   */
  void validateTombstone()
  {
    Collection entries = ((LocalRegion)region).entries.regionEntries();
    if (entries.size() != 1) {
      fail("expected size to be 1 but is " + entries.size());
    }
    RegionEntry entry = (RegionEntry)entries.iterator().next();
    DiskId id = ((DiskEntry)entry).getDiskId();
    Object obj = ((LocalRegion)region).getDiskRegion().get(id);
    assertEquals(Token.TOMBSTONE, obj);
  }

  /**
   * validate whether invalidate was done
   *  
   */
  void validateInvalidate()
  {
    Collection entries = ((LocalRegion)region).entries.regionEntries();
    if (entries.size() != 1) {
      fail("expected size to be 1 but is " + entries.size());
    }
    RegionEntry entry = (RegionEntry)entries.iterator().next();
    DiskId id = ((DiskEntry)entry).getDiskId();
    Object obj = ((LocalRegion)region).getDiskRegion().get(id);
    if (!(obj.equals(Token.INVALID))) {
      fail(" incorrect invalidation");
    }
  }

  private long flushCount;

  private long getCurrentFlushCount() {
    return ((LocalRegion)region).getDiskStore().getStats().getFlushes();
  }
  void pauseFlush() {
    ((LocalRegion)region).getDiskRegion().pauseFlusherForTesting();
    this.flushCount = getCurrentFlushCount();
  }
  
  /**
   * force a flush on the region
   *  
   */
  void forceFlush()
  {
    ((LocalRegion)region).getDiskRegion().flushForTesting();
  }

  /**
   * all the operations done here
   *  
   */
  void allTest()
  {
    pauseFlush();
    createAndPut();
    forceFlush();
    validateModification();
    region.clear();

    pauseFlush();
    createAndDestroy();
    forceFlush();
    validateTombstone();
    region.clear();

    pauseFlush();
    createAndInvalidate();
    forceFlush();
    validateInvalidate();
    region.clear();

    pauseFlush();
    createDestroyCreate();
    forceFlush();
    validateModification();

    pauseFlush();
    putAndPut();
    forceFlush();
    validateModification();
    region.clear();

    pauseFlush();
    putAndDestroy();
    forceFlush();
    validateTombstone();
    region.clear();

    pauseFlush();
    putAndInvalidate();
    forceFlush();
    validateInvalidate();
    region.clear();

  }

  /**
   * test conflation for perist only
   *  
   */
  @Test
  public void testPersistOnlyConflation()
  {
    createPersistOnly();
    allTest();
    closeDown();
  }

  /**
   * test conflation for overflow and persist
   *  
   */
  @Test
  public void testOverFlowAndPersistOnlyConflation()
  {
    try {
      createOverflowAndPersist();
      allTest();
      closeDown();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }

 

}
