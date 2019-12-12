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
package org.apache.geode.internal.cache;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class CacheFactoryStaticsIntegrationTest {

  private final CountDownLatch latch = new CountDownLatch(1);

  private Cache cache;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() {
    cache = new CacheFactory(new Properties()).create();
  }

  @After
  public void after() {
    cache.close();
  }

  @Test
  public void cacheFactoryStaticsGetAnyInstanceDoesNotRequireSynchronizedLock() throws Exception {
    synchronized (InternalCacheBuilder.class) {
      executorServiceRule.submit(() -> {
        try {
          await().until(() -> CacheFactoryStatics.getAnyInstance() != null);
        } catch (Exception e) {
          errorCollector.addError(e);
        } finally {
          latch.countDown();
        }
      });
      latch.await(getTimeout().getValueInMS(), MILLISECONDS);
    }
  }
}
