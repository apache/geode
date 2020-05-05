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


/**
 * Implements {@link StripedExecutor} by using synchronization.
 * The thread that calls execute will also be the thread that
 * does the work. But it will do it under synchronization.
 * The hashCode of the stripeId is used to associate the id with a stripe.
 */
public class SynchronizedStripedExecutor implements StripedExecutor {
  private static final int DEFAULT_CONCURRENCY_LEVEL = 4093; // use a prime
  private final Object[] syncs;

  public SynchronizedStripedExecutor() {
    this(DEFAULT_CONCURRENCY_LEVEL);
  }

  public SynchronizedStripedExecutor(int concurrencyLevel) {
    syncs = new Object[concurrencyLevel];
    for (int i = 0; i < concurrencyLevel; i++) {
      syncs[i] = new Object();
    }
  }

  public <T> void execute(Object stripeId,
      Callable<T> callable,
      Consumer<T> resultConsumer) {
    T resultOfCallable;
    synchronized (getSync(stripeId)) {
      try {
        resultOfCallable = callable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    resultConsumer.accept(resultOfCallable);
  }

  private Object getSync(Object stripeId) {
    int hash = stripeId.hashCode();
    if (hash < 0) {
      hash = -hash;
    }
    return syncs[hash % syncs.length];
  }
}
