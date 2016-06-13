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
package com.gemstone.gemfire.cache30;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import java.io.File;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.OSProcess;

/**
 *
 *
 */
@Category(DistributedTest.class)
public class DiskDistributedNoAckAsyncOverflowRegionDUnitTest extends DiskDistributedNoAckRegionTestCase {
  
  /** Creates a new instance of DiskDistributedNoAckSyncOverflowRegionTest */
  public DiskDistributedNoAckAsyncOverflowRegionDUnitTest() {
    super();
  }
  
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);

    factory.setDiskStoreName(getCache().createDiskStoreFactory()
                             .setDiskDirs(getDiskDirs())
                             .setTimeInterval(1000)
                             .setQueueSize(0)
                             .create(getUniqueName())
                             .getName());
   
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUMemoryAttributes(1, null, EvictionAction.OVERFLOW_TO_DISK));
    factory.setDiskSynchronous(false);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }
}
