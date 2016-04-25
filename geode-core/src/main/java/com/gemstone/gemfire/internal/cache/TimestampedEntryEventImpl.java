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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.util.TimestampedEntryEvent;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;

/**
 * A subclass of EntryEventImpl used in WAN conflict resolution
 * 
 */
public class TimestampedEntryEventImpl extends EntryEventImpl implements
    TimestampedEntryEvent {
  
  private int newDSID;
  private int oldDSID;
  private long newTimestamp;
  private long oldTimestamp;

  @Retained
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
