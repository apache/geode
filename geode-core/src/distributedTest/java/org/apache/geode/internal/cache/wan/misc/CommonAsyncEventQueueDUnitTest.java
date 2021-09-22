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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AsyncEventQueueTestBase;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({AEQTest.class})
@RunWith(GeodeParamsRunner.class)
public class CommonAsyncEventQueueDUnitTest extends AsyncEventQueueTestBase {

  private static final long serialVersionUID = 1L;
  private static MyAsyncEventListener myAsyncEventListener = new MyAsyncEventListener();

  public CommonAsyncEventQueueDUnitTest() {
    super();
  }

  interface AEQFactoryTestSupplier extends Serializable {
    AsyncEventQueueFactory get(Cache cache);
  }

  public AEQFactoryTestSupplier[] getAsyncEventQueueParameters() {
    AEQFactoryTestSupplier[] suppliers = new AEQFactoryTestSupplier[2];
    suppliers[0] = (Cache cache) -> {
      AsyncEventQueueFactory serialAEQFactory = cache.createAsyncEventQueueFactory();
      serialAEQFactory.setPersistent(false);
      serialAEQFactory.setMaximumQueueMemory(100);
      serialAEQFactory.setParallel(false);
      return serialAEQFactory;
    };

    suppliers[1] = (Cache cache) -> {
      AsyncEventQueueFactory parallelAEQFactory = cache.createAsyncEventQueueFactory();
      parallelAEQFactory.setPersistent(false);
      parallelAEQFactory.setMaximumQueueMemory(100);
      parallelAEQFactory.setParallel(true);
      return parallelAEQFactory;
    };

    return suppliers;
  }

  @Test
  public void testSameSenderWithNonColocatedRegions() throws Exception {
    IgnoredException.addIgnoredException("cannot have the same parallel async");
    Integer lnPort =
        (Integer) vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));
    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));
    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue("ln", true, 100, 100, false,
        false, null, false));
    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        getTestMethodName() + "_PR1", "ln", isOffHeap()));
    try {
      vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
          getTestMethodName() + "_PR2", "ln", isOffHeap()));
      fail("Expected IllegateStateException : cannot have the same parallel gateway sender");
    } catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException) || !(e.getCause().getMessage()
          .contains("cannot have the same parallel async event queue id"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }

  @Test
  @Parameters(method = "getAsyncEventQueueParameters")
  public void whenAEQBatchBasedOnTimeOnlyThenQueueShouldNotDispatchUntilIntervalIsHit(
      final AEQFactoryTestSupplier aeqSupplier) throws Exception {
    int batchIntervalTime = 5000;
    int numPuts = 100;
    String regionName = getTestMethodName() + "_PR1";

    Integer lnPort =
        (Integer) vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId(1));
    vm1.invoke(() -> AsyncEventQueueTestBase.createCache(lnPort));

    vm1.invoke(() -> {
      // Clear events map in this vm
      myAsyncEventListener.getEventsMap().clear();

      AsyncEventQueueFactory aeqFactory = aeqSupplier.get(cache);
      aeqFactory.setBatchSize(RegionQueue.BATCH_BASED_ON_TIME_ONLY);
      aeqFactory.setBatchTimeInterval(batchIntervalTime);
      aeqFactory.create("ln", myAsyncEventListener);
    });

    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue(
        regionName, "ln", isOffHeap()));
    // do puts
    vm1.invoke(() -> {
      Region r = cache.getRegion(SEPARATOR + regionName);
      for (long i = 0; i < numPuts; i++) {
        r.put(i, "Value_" + i);
      }
    });

    // attempt to prove the absence of a dispatch/ prove a dispatch has not occurred
    // will verify that no events have occurred over a period of time less than batch interval but
    // more than enough
    // for a regular dispatch to have occurred
    vm1.invoke(() -> {
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < batchIntervalTime - 1000) {
        assertEquals(0, myAsyncEventListener.getEventsMap().size());
      }
    });

    // Verify receiver listener events
    vm1.invoke(() -> GeodeAwaitility.await()
        .until(() -> myAsyncEventListener.getEventsMap().size() == numPuts));
  }
}
