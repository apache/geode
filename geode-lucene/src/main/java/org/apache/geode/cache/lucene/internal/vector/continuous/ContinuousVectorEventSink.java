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

package org.apache.geode.cache.lucene.internal.vector.continuous;

import java.util.List;

/**
 * Outgoing sink receiving RVV-stamped {@link ContinuousVectorEvent} notifications from
 * {@link RvvMonotonicVectorDispatcher}.
 */
@FunctionalInterface
public interface ContinuousVectorEventSink {

  /**
   * Dispatches a matched continuous vector event to a client or transport channel.
   */
  void onEvent(ContinuousVectorEvent event);

  /**
   * Dispatches a coalesced batch of matched {@link ContinuousVectorEvent}s originating from a
   * single bucket mutation (all sharing identical {@code (bucketId, memberId, regionVersion)}).
   * Network-backed transports should override this method to serialize and flush the entire batch
   * in a single frame rather than performing one flush per matched subscription.
   */
  default void onEventBatch(List<ContinuousVectorEvent> events) {
    for (ContinuousVectorEvent event : events) {
      onEvent(event);
    }
  }
}
