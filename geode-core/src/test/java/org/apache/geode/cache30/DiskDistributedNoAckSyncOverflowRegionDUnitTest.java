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
package org.apache.geode.cache30;

import java.io.File;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class DiskDistributedNoAckSyncOverflowRegionDUnitTest
    extends DiskDistributedNoAckRegionTestCase {

  /** Creates a new instance of DiskDistributedNoAckSyncOverflowRegionDUnitTest */
  public DiskDistributedNoAckSyncOverflowRegionDUnitTest() {
    super();
  }

  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);

    factory.setDiskStoreName(getCache().createDiskStoreFactory().setDiskDirs(getDiskDirs())
        .create(getUniqueName()).getName());

    factory.setEvictionAttributes(
        EvictionAttributes.createLRUMemoryAttributes(1, null, EvictionAction.OVERFLOW_TO_DISK));

    factory.setDiskSynchronous(true);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }

}
