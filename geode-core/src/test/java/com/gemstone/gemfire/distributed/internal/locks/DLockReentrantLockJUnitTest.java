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
package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;

@Category(IntegrationTest.class)
public class DLockReentrantLockJUnitTest {
  final long id = 1213L;

  private Cache cache;
  private Region<Long, String> region;

  @Before
  public void setup() {
    cache = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, "").create();
    final RegionFactory<Long, String> regionFactory = cache.createRegionFactory("REPLICATE");
    regionFactory.setScope(Scope.GLOBAL);
    region = regionFactory.create("ReentrantLockRegion");
    region.put(id, new String("TestValue1"));
  }

  @After
  public void tearDown() {
    cache.close();
  }

  /**
   * Tests GEM-96/GEODE-678
   */
  @Test
  public void testReentrantLock() throws Exception {

    Assert.assertEquals(Scope.GLOBAL, region.getAttributes().getScope());

    final Lock lock1 = region.getDistributedLock(id);
    final Lock lock2 = region.getDistributedLock(id);

    for (int i = 0; i < 50; i++) {
      lock1.lock();
      boolean reenteredLock = false;
      try {
        reenteredLock = lock2.tryLock(1, TimeUnit.NANOSECONDS);
        if (!reenteredLock) {
          System.out.println("ERROR: could not reenter lock");
        }
        Assert.assertTrue("Failed getting lock at 2:" + i, reenteredLock);
      } finally {
        if (reenteredLock) {
          lock2.unlock();
        }
        lock1.unlock();
      }
    }
  }
}
