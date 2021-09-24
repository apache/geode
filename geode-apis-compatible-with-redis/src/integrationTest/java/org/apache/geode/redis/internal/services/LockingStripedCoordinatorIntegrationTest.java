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

package org.apache.geode.redis.internal.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class LockingStripedCoordinatorIntegrationTest {

  @Test
  public void concurrentLockRequestsDoNotDeadlock() {
    int keysToLock = 1000;
    int iterations = 10000;
    StripedCoordinator coordinator = new LockingStripedCoordinator();
    List<RedisKey> keyList = new ArrayList<>(keysToLock);
    List<RedisKey> reversedKeyList = new ArrayList<>(keysToLock);

    for (int i = 0; i < keysToLock; i++) {
      RedisKey k = new RedisKey(("" + i).getBytes());
      keyList.add(k);
    }
    for (int i = keysToLock - 1; i >= 0; i--) {
      reversedKeyList.add(keyList.get(i));
    }

    AtomicInteger execCounter = new AtomicInteger(0);
    ConcurrentLoopingThreads loops = new ConcurrentLoopingThreads(iterations,
        i -> {
          List<RedisKey> keys = new ArrayList<>(keyList);
          coordinator.execute(keys, execCounter::incrementAndGet);
        },
        i -> {
          List<RedisKey> keys = new ArrayList<>(reversedKeyList);
          coordinator.execute(keys, execCounter::incrementAndGet);
        });

    loops.start();
    // Will hang if there is a deadlock
    GeodeAwaitility.await().untilAsserted(loops::await);

    assertThat(execCounter.get()).isEqualTo(2 * iterations);
  }
}
