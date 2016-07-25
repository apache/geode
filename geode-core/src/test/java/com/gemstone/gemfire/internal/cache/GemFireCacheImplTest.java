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
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;
import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.jayway.awaitility.Awaitility;

@Category(UnitTest.class)
public class GemFireCacheImplTest {

  @Test
  public void checkThatAsyncEventListenersUseAllThreadsInPool() {
    InternalDistributedSystem ds = Fakes.distributedSystem();
    CacheConfig cc = new CacheConfig();
    TypeRegistry typeRegistry = mock(TypeRegistry.class);
    GemFireCacheImpl gfc = GemFireCacheImpl.createWithAsyncEventListeners(ds, cc, typeRegistry);
    try {
      ThreadPoolExecutor executor = (ThreadPoolExecutor) gfc.getEventThreadPool();
      assertEquals(0, executor.getCompletedTaskCount());
      assertEquals(0, executor.getActiveCount());
      int MAX_THREADS = GemFireCacheImpl.EVENT_THREAD_LIMIT;
      final CountDownLatch cdl = new CountDownLatch(MAX_THREADS);
      for (int i = 1; i <= MAX_THREADS; i++) {
        executor.execute(() -> {
          cdl.countDown();
          try {
            cdl.await();
          } catch (InterruptedException e) {
          }
        });
      }
      Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS).timeout(90, TimeUnit.SECONDS)
      .until(() -> assertEquals(MAX_THREADS, executor.getCompletedTaskCount()));
    } finally {
      gfc.close();
    }
  }
}
