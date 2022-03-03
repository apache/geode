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
package org.apache.geode.internal.offheap.annotations;



/**
 * Used for uniquely identifying off-heap annotations.
 */
public enum OffHeapIdentifier {
  /**
   * Default OffHeapIdentifier. Allows for empty off-heap annotations.
   */
  DEFAULT("DEFAULT"),

  ENTRY_EVENT_NEW_VALUE("org.apache.geode.internal.cache.KeyInfo.newValue"),
  ENTRY_EVENT_OLD_VALUE("org.apache.geode.internal.cache.EntryEventImpl.oldValue"),
  TX_ENTRY_STATE("org.apache.geode.internal.cache.originalVersionId"),
  GATEWAY_SENDER_EVENT_IMPL_VALUE(
      "org.apache.geode.internal.cache.wan.GatewaySenderEventImpl.valueObj"),
  TEST_OFF_HEAP_REGION_BASE_LISTENER(
      "org.apache.geode.internal.offheap.OffHeapRegionBase.MyCacheListener.ohOldValue and ohNewValue"),
  REGION_ENTRY_VALUE(""),
  ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE(
      "org.apache.geode.internal.cache.AbstractRegionEntry.prepareValueForCache(...)"),
  ABSTRACT_REGION_ENTRY_FILL_IN_VALUE(
      "org.apache.geode.internal.cache.AbstractRegionEntry.fillInValue(...)"),
  GEMFIRE_TRANSACTION_BYTE_SOURCE(""),

  /**
   * Used to declare possible grouping that are not yet identified.
   */
  UNKNOWN("UNKNOWN"),

  ;

  /**
   * An identifier for a unique grouping of annotations.
   */
  private String id = null;

  /**
   * Creates a new OffHeapIdentifier.
   *
   * @param id a unique identifier.
   */
  OffHeapIdentifier(final String id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return id;
  }
}
