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

import java.util.concurrent.Callable;


/**
 * Allows users to "stripe" their execution in such a way that all tasks belonging to one stripe are
 * executed in order. A stripe is associated with an Object called the "stripeId".
 * It is up to the implementor of this interface to decide how to do this association.
 * For example it could use the hashCode of the stripeId or use the equals method.
 * Work submitted for the same stripe is guaranteed to be executed sequentially.
 */
public interface StripedCoordinator {
  /**
   * Executes, at some time in the future,
   * the given callable by invoking "call" on it and then returning the result.
   *
   * @param stripeId defines the "stripe"
   * @param callable the unit of work to do sequentially. May be called after run returns.
   */
  <T> T execute(Object stripeId, Callable<T> callable);

  int compareStripes(Object object1, Object object2);
}
