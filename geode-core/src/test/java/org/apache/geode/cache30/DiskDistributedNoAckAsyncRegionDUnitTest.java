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
package org.apache.geode.cache30;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.*;
import java.io.*;
import org.apache.geode.internal.OSProcess;

@Category(DistributedTest.class)
public class DiskDistributedNoAckAsyncRegionDUnitTest extends DiskDistributedNoAckRegionTestCase {
  
  /** Creates a new instance of DiskDistributedNoAckSyncOverflowRegionTest */
  public DiskDistributedNoAckAsyncRegionDUnitTest() {
    super();
  }
  
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    
    File[] diskDirs = new File[1];
    diskDirs[0] = new File("diskRegionDirs/" + OSProcess.getId());
    diskDirs[0].mkdirs();
    factory.setDiskStoreName(getCache().createDiskStoreFactory()
                             .setDiskDirs(getDiskDirs())
                             .setTimeInterval(1000)
                             .setQueueSize(0)
                             .create(getUniqueName())
                             .getName());
    factory.setDiskSynchronous(false);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }
 
}
