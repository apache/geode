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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.Scope;

/**
 * Unit testing for ComplexDiskRegion API's
 */
public class ComplexDiskRegionJUnitTest extends DiskRegionTestingBase {

  private DiskRegionProperties diskProps = new DiskRegionProperties();

  @Override
  protected final void postSetUp() throws Exception {
    diskProps.setDiskDirs(dirs);
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @Override
  protected final void postTearDown() throws Exception {
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
  }

  /**
   * Test method for 'org.apache.geode.internal.cache.ComplexDiskRegion.addToBeCompacted(Oplog)'
   *
   * The test will test that an oplog is correctly being added to be rolled
   */
  @Test
  public void testAddToBeCompacted() {
    deleteFiles();
    diskProps.setRolling(false);
    diskProps.setAllowForceCompaction(true);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    StatisticsFactory factory = region.getCache().getDistributedSystem();
    Oplog oplog1 = new Oplog(11, dr.getOplogSet(), new DirectoryHolder(factory, dirs[1], 1000, 0));
    Oplog oplog2 = new Oplog(12, dr.getOplogSet(), new DirectoryHolder(factory, dirs[2], 1000, 1));
    Oplog oplog3 = new Oplog(13, dr.getOplogSet(), new DirectoryHolder(factory, dirs[3], 1000, 2));
    // give these Oplogs some fake "live" entries
    oplog1.incTotalCount();
    oplog1.incLiveCount();
    oplog2.incTotalCount();
    oplog2.incLiveCount();
    oplog3.incTotalCount();
    oplog3.incLiveCount();

    dr.addToBeCompacted(oplog1);
    dr.addToBeCompacted(oplog2);
    dr.addToBeCompacted(oplog3);

    assertEquals(null, dr.getOplogToBeCompacted());

    oplog1.incTotalCount();
    if (oplog1 != dr.getOplogToBeCompacted()[0]) {
      fail(" expected oplog1 to be the first oplog but not the case !");
    }
    dr.removeOplog(oplog1.getOplogId());
    assertEquals(null, dr.getOplogToBeCompacted());

    oplog2.incTotalCount();
    if (oplog2 != dr.getOplogToBeCompacted()[0]) {
      fail(" expected oplog2 to be the first oplog but not the case !");
    }
    dr.removeOplog(oplog2.getOplogId());
    assertEquals(null, dr.getOplogToBeCompacted());

    oplog3.incTotalCount();
    if (oplog3 != dr.getOplogToBeCompacted()[0]) {
      fail(" expected oplog3 to be the first oplog but not the case !");
    }
    dr.removeOplog(oplog3.getOplogId());

    oplog1.destroy();
    oplog2.destroy();
    oplog3.destroy();
    closeDown();
    deleteFiles();
  }

  /**
   * Test method for 'org.apache.geode.internal.cache.ComplexDiskRegion.removeFirstOplog(Oplog)'
   *
   * The test verifies the FIFO property of the oplog set (first oplog to be added should be the
   * firs to be rolled).
   */
  @Test
  public void testRemoveFirstOplog() {
    deleteFiles();
    diskProps.setRolling(false);
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    StatisticsFactory factory = region.getCache().getDistributedSystem();
    Oplog oplog1 = new Oplog(11, dr.getOplogSet(), new DirectoryHolder(factory, dirs[1], 1000, 0));
    Oplog oplog2 = new Oplog(12, dr.getOplogSet(), new DirectoryHolder(factory, dirs[2], 1000, 1));
    Oplog oplog3 = new Oplog(13, dr.getOplogSet(), new DirectoryHolder(factory, dirs[3], 1000, 2));
    // give these Oplogs some fake "live" entries
    oplog1.incTotalCount();
    oplog1.incLiveCount();
    oplog2.incTotalCount();
    oplog2.incLiveCount();
    oplog3.incTotalCount();
    oplog3.incLiveCount();

    dr.addToBeCompacted(oplog1);
    dr.addToBeCompacted(oplog2);
    dr.addToBeCompacted(oplog3);

    if (oplog1 != dr.removeOplog(oplog1.getOplogId())) {
      fail(" expected oplog1 to be the first oplog but not the case !");
    }

    if (oplog2 != dr.removeOplog(oplog2.getOplogId())) {
      fail(" expected oplog2 to be the first oplog but not the case !");
    }
    if (oplog3 != dr.removeOplog(oplog3.getOplogId())) {
      fail(" expected oplog3 to be the first oplog but not the case !");
    }
    oplog1.destroy();
    oplog2.destroy();
    oplog3.destroy();

    closeDown();
    deleteFiles();
  }

}
