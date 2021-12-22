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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.apache.geode.cache.Scope;

/**
 * This test will test that there are no unexpected behaviours if the the region attributes are
 * changed after starting it again.
 *
 * The behaviour should be predictable
 */
public class DiskRegionChangingRegionAttributesJUnitTest extends DiskRegionTestingBase {

  private DiskRegionProperties props;

  @Override
  protected final void postSetUp() throws Exception {
    props = new DiskRegionProperties();
    props.setDiskDirs(dirs);
  }

  private void createOverflowOnly() {
    props.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache, props);
  }

  private void createPersistOnly() {
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, props, Scope.LOCAL);
  }

  private void createPersistAndOverflow() {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, props);
  }

  @Test
  public void testOverflowOnlyAndThenPersistOnly() {
    createOverflowOnly();
    put100Int();
    region.close();
    createPersistOnly();
    assertTrue(region.size() == 0);
  }

  @Test
  public void testPersistOnlyAndThenOverflowOnly() {
    createPersistOnly();
    put100Int();
    region.close();
    try {
      createOverflowOnly();
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    }
    // Asif Recreate the region so that it gets destroyed in teardown
    // clearing up the old Oplogs
    createPersistOnly();

  }

  @Test
  public void testOverflowOnlyAndThenPeristAndOverflow() {
    createOverflowOnly();
    put100Int();
    region.close();
    createPersistAndOverflow();
    assertTrue(region.size() == 0);
  }

  @Test
  public void testPersistAndOverflowAndThenOverflowOnly() {
    createPersistAndOverflow();
    put100Int();
    region.close();
    try {
      createOverflowOnly();
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    }
    createPersistAndOverflow();
  }

  @Test
  public void testPersistOnlyAndThenPeristAndOverflow() {
    createPersistOnly();
    put100Int();
    region.close();
    createPersistAndOverflow();
    assertTrue(region.size() == 100);
  }

  @Test
  public void testPersistAndOverflowAndThenPersistOnly() {
    createPersistAndOverflow();
    put100Int();
    region.close();
    createPersistOnly();
    assertTrue(region.size() == 100);
  }
}
