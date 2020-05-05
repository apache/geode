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

/**
 * Allows users to "stripe" their execution in such a way that all tasks belonging to one stripe are
 * executed in-order.
 */
class StripedExecutor {
  private static final int DEFAULT_CONCURRENCY_LEVEL = 4093; // use a prime
  private final Object[] syncs;

  public StripedExecutor() {
    this(DEFAULT_CONCURRENCY_LEVEL);
  }

  public StripedExecutor(int concurrencyLevel) {
    syncs = new Object[concurrencyLevel];
    for (int i = 0; i < concurrencyLevel; i++) {
      syncs[i] = new Object();
    }
  }

  /**
   * Executes, at some time in the future,
   * the given callable by invoking "call" on it and then passing
   * the result of "call" to "accept" on the given consumer.
   * Concurrent calls of this method with equal keys will invoke their callables sequentially.
   *
   * @param key defines the "stripe"; only equal keys are guaranteed to be run sequentially
   * @param callable the unit of work to do sequentially. May be called after run returns.
   * @param resultConsumer is given the result of the callable.
   */
  public <T> void execute(ByteArrayWrapper key,
      Callable<T> callable,
      Consumer<T> resultConsumer) {
    T resultOfRedisCommand;
    synchronized (getSync(key)) {
      try {
        resultOfRedisCommand = callable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    resultConsumer.accept(resultOfRedisCommand);
  }

  private Object getSync(ByteArrayWrapper key) {
    int hash = key.hashCode();
    if (hash < 0) {
      hash = -hash;
    }
    return syncs[hash % syncs.length];
  }
}
