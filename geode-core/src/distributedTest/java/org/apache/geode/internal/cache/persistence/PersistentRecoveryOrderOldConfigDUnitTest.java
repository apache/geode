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
package org.apache.geode.internal.cache.persistence;

import java.io.File;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskWriteAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.junit.categories.PersistenceTest;

@Category(PersistenceTest.class)
@SuppressWarnings("serial")
public class PersistentRecoveryOrderOldConfigDUnitTest extends PersistentRecoveryOrderDUnitTest {

  /**
   * Override to use deprecated APIs for the creation of disk and region. This test class can
   * probably be deleted.
   */
  @Override
  @SuppressWarnings("deprecation")
  protected void createReplicateRegion(String regionName, File[] diskDirs,
      boolean diskSynchronous) {
    getCache();

    DiskWriteAttributesFactory diskWriteAttributesFactory = new DiskWriteAttributesFactory();
    diskWriteAttributesFactory.setMaxOplogSize(1);
    diskWriteAttributesFactory.setSynchronous(diskSynchronous);

    RegionFactory regionFactory = new RegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);
    regionFactory.setDiskDirs(diskDirs);
    regionFactory.setDiskWriteAttributes(diskWriteAttributesFactory.create());

    regionFactory.create(regionName);
  }

}
