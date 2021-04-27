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

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.logging.internal.OSProcess;


public class DiskDistributedNoAckAsyncRegionDUnitTest extends DiskDistributedNoAckRegionTestCase {

  /** Creates a new instance of DiskDistributedNoAckSyncOverflowRegionTest */
  public DiskDistributedNoAckAsyncRegionDUnitTest() {
    super();
  }

  @Override
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);

    File[] diskDirs = new File[1];
    diskDirs[0] = new File("diskRegionDirs/" + OSProcess.getId());
    diskDirs[0].mkdirs();
    factory.setDiskStoreName(getCache().createDiskStoreFactory().setDiskDirs(getDiskDirs())
        .setTimeInterval(1000).setQueueSize(0).create(getUniqueName()).getName());
    factory.setDiskSynchronous(false);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }

}
