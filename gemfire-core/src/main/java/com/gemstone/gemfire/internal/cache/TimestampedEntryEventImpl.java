/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.util.TimestampedEntryEvent;

/**
 * A subclass of EntryEventImpl used in WAN conflict resolution
 * 
 * @author Bruce Schuchardt
 */
public class TimestampedEntryEventImpl extends EntryEventImpl implements
    TimestampedEntryEvent {
  
  private int newDSID;
  private int oldDSID;
  private long newTimestamp;
  private long oldTimestamp;

  public TimestampedEntryEventImpl(EntryEventImpl event, int newDSID, int oldDSID, long newTimestamp, long oldTimestamp) {
    super(event);
    this.newDSID = newDSID;
    this.oldDSID = oldDSID;
    this.newTimestamp = newTimestamp;
    this.oldTimestamp = oldTimestamp;
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.util.TimestampedEntryEvent#getNewDistributedSystemID()
   */
  @Override
  public int getNewDistributedSystemID() {
    return this.newDSID;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.util.TimestampedEntryEvent#getOldDistributedSystemID()
   */
  @Override
  public int getOldDistributedSystemID() {
    return this.oldDSID;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.util.TimestampedEntryEvent#getNewTimestamp()
   */
  @Override
  public long getNewTimestamp() {
    return this.newTimestamp;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.util.TimestampedEntryEvent#getOldTimestamp()
   */
  @Override
  public long getOldTimestamp() {
    return this.oldTimestamp;
  }

}
