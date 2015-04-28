/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.lru;

public interface LRUClockNode {

  public void setNextLRUNode( LRUClockNode next );
  public void setPrevLRUNode( LRUClockNode prev );
  
  public LRUClockNode nextLRUNode();
  public LRUClockNode prevLRUNode();
  
  /** compute the new entry size and return the delta from the previous entry size */
  public int updateEntrySize(EnableLRU ccHelper);
  /** compute the new entry size and return the delta from the previous entry size
   * @param value then entry's value
   * @since 6.1.2.9
   */
  public int updateEntrySize(EnableLRU ccHelper, Object value);
  
  public int getEntrySize();
  
  public boolean testRecentlyUsed();
  public void setRecentlyUsed();
  public void unsetRecentlyUsed();

  public void setEvicted();
  public void unsetEvicted();
  public boolean testEvicted();
}
