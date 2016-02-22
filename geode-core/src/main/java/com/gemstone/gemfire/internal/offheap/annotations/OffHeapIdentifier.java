/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.offheap.annotations;



/**
 * Used for uniquely identifying off-heap annotations.
 */
public enum OffHeapIdentifier {
  /**
   * Default OffHeapIdentifier.  Allows for empty off-heap annotations.
   */
  DEFAULT("DEFAULT"),
  
  ENTRY_EVENT_NEW_VALUE("com.gemstone.gemfire.internal.cache.KeyInfo.newValue"),
  ENTRY_EVENT_OLD_VALUE("com.gemstone.gemfire.internal.cache.EntryEventImpl.oldValue"),
  TX_ENTRY_STATE("com.gemstone.gemfire.internal.cache.originalVersionId"),
  GATEWAY_SENDER_EVENT_IMPL_VALUE("com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl.valueObj"),
  TEST_OFF_HEAP_REGION_BASE_LISTENER("com.gemstone.gemfire.internal.offheap.OffHeapRegionBase.MyCacheListener.ohOldValue and ohNewValue"),
  COMPACT_COMPOSITE_KEY_VALUE_BYTES("com.vmware.sqlfire.internal.engine.store.CompactCompositeKey.valueBytes"),
  // TODO: HOOTS: Deal with this
  REGION_ENTRY_VALUE(""),
  ABSTRACT_REGION_ENTRY_PREPARE_VALUE_FOR_CACHE("com.gemstone.gemfire.internal.cache.AbstractRegionEntry.prepareValueForCache(...)"),
  ABSTRACT_REGION_ENTRY_FILL_IN_VALUE("com.gemstone.gemfire.internal.cache.AbstractRegionEntry.fillInValue(...)"),
  COMPACT_EXEC_ROW_SOURCE("com.vmware.sqlfire.internal.engine.store.CompactExecRow.source"),
  COMPACT_EXEC_ROW_WITH_LOBS_SOURCE("com.vmware.sqlfire.internal.engine.store.CompactExecRowWithLobs.source"),
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
   * @param id a unique identifier.
   */
  OffHeapIdentifier(final String id) {
    this.id = id;
  }
  
  @Override
  public String toString() {
    return this.id;
  }  
}
