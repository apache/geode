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

package org.apache.geode.redis.internal.executor.set;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.apache.geode.redis.internal.ByteArrayWrapper;

class SynchronizedRedisCommandRunner {
  private final LockManager lockManager = new LockManager();

  public void run(ByteArrayWrapper key,
                  Callable redisCommand,
                  Consumer<Object> resultsSenderCallBack) {
    Object resultOfRedisCommand;
    synchronized (lockManager.getLock(key)) {
      try {
        resultOfRedisCommand = redisCommand.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    resultsSenderCallBack.accept(resultOfRedisCommand);
  }

  private static class LockManager {
    private static final int DEFAULT_LOCK_COUNT = 4093; // use a prime
    private final Object[] locks;

    public LockManager() {
      this(DEFAULT_LOCK_COUNT);
    }

    public LockManager(int maxLocks) {
      locks = new Object[maxLocks];
      for (int i = 0; i < maxLocks; i++) {
        locks[i] = new Object();
      }
    }

    public Object getLock(ByteArrayWrapper key) {
      int hash = key.hashCode();
      if (hash < 0) {
        hash = -hash;
      }
      return locks[hash % locks.length];    }
  }
}
