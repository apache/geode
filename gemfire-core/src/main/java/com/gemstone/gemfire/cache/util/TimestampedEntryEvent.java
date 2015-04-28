/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

/**
 * TimestampedEntryEvent is an EntryEvent that has additional information provided
 * to GatewayConflictResolver plugins.  It holds the low 4 bytes of the millisecond
 * clock value from the point of origin of the event and the distributed system ID
 * of the system that caused the change.  It also has this information for the
 * previous change to the entry.
 * @since 7.0
 * @author Bruce Schuchardt
 */
public interface TimestampedEntryEvent extends com.gemstone.gemfire.cache.EntryEvent {
  // note that this interface inherits the following methods, among others:
  // getRegion(), getOperation(), getCallbackArgument()
  // getKey()
  // getOldValue(), getNewValue()
  // getSerializedOldValue(), getSerializedNewValue()

  public int getNewDistributedSystemID();
  public int getOldDistributedSystemID();

  public long getNewTimestamp();
  public long getOldTimestamp();
}
