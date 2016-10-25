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
package org.apache.geode.internal.cache.wan.misc;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.internal.cache.wan.AsyncEventQueueTestBase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.IgnoredException;

/**
 *
 */
@Category(DistributedTest.class)
public class CommonParallelAsyncEventQueueDUnitTest extends AsyncEventQueueTestBase {
  
  private static final long serialVersionUID = 1L;

  public CommonParallelAsyncEventQueueDUnitTest() {
    super();
  }

  @Test
  public void testSameSenderWithNonColocatedRegions() throws Exception {
    IgnoredException.addIgnoredException("cannot have the same parallel async");
    Integer lnPort = (Integer)vm0.invoke(() -> AsyncEventQueueTestBase.createFirstLocatorWithDSId( 1 ));
    vm1.invoke(() -> AsyncEventQueueTestBase.createCache( lnPort ));
    vm1.invoke(() -> AsyncEventQueueTestBase.createAsyncEventQueue( "ln",
      true, 100, 100, false, false, null, false ));
    vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR1", "ln", isOffHeap()  ));
    try {
      vm1.invoke(() -> AsyncEventQueueTestBase.createPartitionedRegionWithAsyncEventQueue( getTestMethodName() + "_PR2", "ln", isOffHeap()  ));
      fail("Expected IllegateStateException : cannot have the same parallel gateway sender");
    }
    catch (Exception e) {
      if (!(e.getCause() instanceof IllegalStateException)
          || !(e.getCause().getMessage()
              .contains("cannot have the same parallel async event queue id"))) {
        Assert.fail("Expected IllegalStateException", e);
      }
    }
  }
}
