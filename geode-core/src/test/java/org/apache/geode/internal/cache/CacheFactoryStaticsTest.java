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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;


public class CacheFactoryStaticsTest {
  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  private CountDownLatch latch;
  private Exception failure;

  @Test
  public void cacheFactoryStaticsGetAnyInstanceDoesNotRequireSynchronizedLock() throws Exception {
    latch = new CountDownLatch(1);
    new CacheFactory(new Properties()).create();
    assertThat(CacheFactory.getAnyInstance()).isNotNull();

    synchronized (InternalCacheBuilder.class) {
      executorServiceRule.submit(() -> cacheFactoryStaticsGetAnyInstanceCanGetCacheInstance());
      latch.await();
    }

    if (failure != null) {
      throw failure;
    }
  }

  private void cacheFactoryStaticsGetAnyInstanceCanGetCacheInstance() {
    try {
      GeodeAwaitility.await().until(() -> (CacheFactoryStatics.getAnyInstance()) != null);
    } catch (Exception exception) {
      failure = exception;
    } finally {
      latch.countDown();
    }
  }
}
