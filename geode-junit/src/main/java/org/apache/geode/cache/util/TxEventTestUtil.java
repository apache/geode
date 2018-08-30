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
package org.apache.geode.cache.util;

import static java.util.Objects.isNull;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;

/**
 * Utility class for getting Transaction events for create, invalidate, put, destroy operations.
 */
public class TxEventTestUtil {

  /**
   * Selects entry create events from a list of cache events.
   *
   * @return list of entry create events from the given cache events
   */
  public static <K, V> List<EntryEvent<K, V>> getCreateEvents(List<CacheEvent<K, V>> cacheEvents) {
    return getEntryEventsWithOperation(cacheEvents, Operation::isCreate);
  }

  /**
   * Selects the entry update events from a list of cache events.
   *
   * @return list of entry update events from the given cache events
   */
  public static <K, V> List<EntryEvent<K, V>> getPutEvents(List<CacheEvent<K, V>> cacheEvents) {
    return getEntryEventsWithOperation(cacheEvents, Operation::isUpdate);
  }

  /**
   * Selects the entry invalidate events from a list of cache events.
   *
   * @return list of entry invalidate events from the given cache events
   */
  public static <K, V> List<EntryEvent<K, V>> getInvalidateEvents(
      List<CacheEvent<K, V>> cacheEvents) {
    return getEntryEventsWithOperation(cacheEvents, Operation::isInvalidate);
  }

  /**
   * Selects the entry destroy events from a list of cache events.
   *
   * @return list of entry destroy events from the given cache events
   */
  public static <K, V> List<EntryEvent<K, V>> getDestroyEvents(List<CacheEvent<K, V>> cacheEvents) {
    return getEntryEventsWithOperation(cacheEvents, Operation::isDestroy);
  }

  /**
   * Selects the entry events whose operation matches the given predicate.
   *
   * @param cacheEvents the cache events from which to select entry events
   * @param operationPredicate tests each event's operation to determine whether to include the
   *        event
   * @return list of entry events whose operations match the given predicate
   * @throws ClassCastException if the predicate matches the operation of an event that is not an
   *         {@code EntryEvent}
   */
  public static <K, V> List<EntryEvent<K, V>> getEntryEventsWithOperation(
      List<CacheEvent<K, V>> cacheEvents,
      Predicate<Operation> operationPredicate) {
    return getEntryEventsMatching(cacheEvents, e -> operationPredicate.test(e.getOperation()));
  }

  /**
   * Selects the entry events that match the given predicate.
   *
   * @param cacheEvents the cache events from which to select entry events
   * @param predicate tests whether to include each event
   * @return list of entry events that match the predicate
   * @throws ClassCastException if the predicate matches an event that is not an {@code EntryEvent}
   */
  public static <K, V> List<EntryEvent<K, V>> getEntryEventsMatching(
      List<CacheEvent<K, V>> cacheEvents,
      Predicate<? super EntryEvent<K, V>> predicate) {
    if (isNull(cacheEvents)) {
      return Collections.emptyList();
    }
    return cacheEvents.stream()
        .filter(EntryEvent.class::isInstance)
        .map(cacheEvent -> (EntryEvent<K, V>) cacheEvent)
        .filter(predicate)
        .collect(Collectors.toList());
  }
}
