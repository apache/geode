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

import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderQueue;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
/**
 * 
 *
 */
public abstract class AbstractDiskRegionEntry
  extends AbstractRegionEntry
  implements DiskEntry
{
  protected AbstractDiskRegionEntry(RegionEntryContext context, Object value) {
    super(context, value);
  }
  
  @Override
  public  void setValue(RegionEntryContext context, Object v) throws RegionClearedException {
    setValue(context, v, null);
  }
  
  @Override
  public void setValue(RegionEntryContext context, Object value, EntryEventImpl event) throws RegionClearedException {
    Helper.update(this, (LocalRegion) context, value, event);
    setRecentlyUsed(); // fix for bug #42284 - entry just put into the cache is evicted
  }

  /**
   * Sets the value with a {@link RegionEntryContext}.
   * @param context the value's context.
   * @param value an entry value.
   */
  @Override
  public void setValueWithContext(RegionEntryContext context, Object value) {
    _setValue(value);
    if (value != null && context != null && (this instanceof OffHeapRegionEntry) 
        && context instanceof LocalRegion && ((LocalRegion)context).isThisRegionBeingClosedOrDestroyed()) {
      ((OffHeapRegionEntry)this).release();
      ((LocalRegion)context).checkReadiness();
    }
  }
  
  // Do not add any instances fields to this class.
  // Instead add them to the DISK section of LeafRegionEntry.cpp.

  @Override
  public void handleValueOverflow(RegionEntryContext context) {
    if (context instanceof BucketRegionQueue || context instanceof SerialGatewaySenderQueue.SerialGatewaySenderQueueMetaRegion) {
      GatewaySenderEventImpl.release(this._getValue()); // OFFHEAP _getValue ok
    }
  }
}
