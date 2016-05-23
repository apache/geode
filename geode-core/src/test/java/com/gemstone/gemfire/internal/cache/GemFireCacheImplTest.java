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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.jayway.awaitility.Awaitility;

@Category(UnitTest.class)
public class GemFireCacheImplTest {

  @Test
  public void checkThatAsyncEventListenersUseAllThreadsInPool() {
    
    GemFireCacheImpl gfc = GemFireCacheImpl.createWithAsyncEventListeners(Fakes.distributedSystem(), new CacheConfig());
    try {
    ThreadPoolExecutor executor = (ThreadPoolExecutor) gfc.getEventThreadPool();
    final long initialCount = executor.getCompletedTaskCount();
      int MAX_THREADS = GemFireCacheImpl.EVENT_THREAD_LIMIT;
      final CountDownLatch cdl = new CountDownLatch(MAX_THREADS);
      for (int i = 1; i <= MAX_THREADS; i++) {
        Runnable r = new Runnable() {
          @Override
          public void run() {
            cdl.countDown();
            try {
              cdl.await();
            } catch (InterruptedException e) {
            }
          }
        };
        executor.execute(r);
      }
      Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS).timeout(15, TimeUnit.SECONDS)
      .until(() -> {
        return executor.getCompletedTaskCount() == MAX_THREADS+initialCount;
      });
    } finally {
      gfc.close();
    }
  }
}
