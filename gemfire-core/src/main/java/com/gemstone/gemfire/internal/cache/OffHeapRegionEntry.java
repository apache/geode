
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.internal.offheap.Releasable;

/**
 * Any RegionEntry that is stored off heap must
 * implement this interface.
 * 
 * @author darrel
 *
 */
public interface OffHeapRegionEntry extends RegionEntry, Releasable {
  /**
   * OFF_HEAP_FIELD_READER
   * @return OFF_HEAP_ADDRESS
   */
  public long getAddress();
  /**
   * OFF_HEAP_FIELD_WRITER
   * @param expectedAddr OFF_HEAP_ADDRESS
   * @param newAddr OFF_HEAP_ADDRESS
   * @return newAddr OFF_HEAP_ADDRESS
   */
  public boolean setAddress(long expectedAddr, long newAddr);
}
