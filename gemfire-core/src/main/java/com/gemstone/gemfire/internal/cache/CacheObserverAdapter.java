/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.RegionEvent;

/**
 * This class provides 'do-nothing' implementations of all of the methods of
 * interface CacheObserver. See the documentation for class CacheObserverHolder
 * for details. Also the callback is issed only if the boolean
 * ISSUE_CALLBACKS_TO_CACHE_OBSERVER present in
 * com.gemstone.gemfire.internal.cache.LocalRegion is made true
 * 
 * @author ashahid
 */
public class CacheObserverAdapter implements CacheObserver {

  /**
   * Called just after the region is cleared & before Listener callback is
   * issued. The call to this method is synchronous
   * 
   * @param event
   *          RegionEvent object
   */
  public void afterRegionClear(RegionEvent event) {
  }

  public void beforeDiskClear()
  {
    // TODO Auto-generated method stub
  }

  public void goingToFlush()
  {
    // TODO Auto-generated method stub
  }

  public void beforeWritingBytes()
  {
  }

  public void afterWritingBytes()
  {
  }

  public void beforeGoingToCompact()
  {
    // TODO Auto-generated method stub
  }

  public void afterHavingCompacted()
  {
    // TODO Auto-generated method stub
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.internal.cache.CacheObserver#afterConflation(java.nio.ByteBuffer,
   *      java.nio.ByteBuffer)
   */
  public void afterConflation(ByteBuffer origBB, ByteBuffer conflatedBB)
  {
    // TODO Auto-generated method stub
  }
  /*
   *  (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.CacheObserver#afterSettingOplogOffSet()
   */
  public void afterSettingOplogOffSet(long offset) {
    // TODO Auto-generated method stub
  }

  public void beforeSwitchingOplog() {
    // TODO Auto-generated method stub
  }

  public void afterSwitchingOplog() {
    // TODO Auto-generated method stub
  }
  
  public void afterKrfCreated() {
    
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.CacheObserver#beforeStoppingCompacter()
   */
  public void beforeStoppingCompactor()
  {
    // TODO Auto-generated method stub
    
  }
  public void afterStoppingCompactor() {
    
  }
  public void afterSignallingCompactor() {
  }

  public void afterMarkingGIICompleted()
  {
    // TODO Auto-generated method stub
    
  }

  public void afterMarkingGIIStarted()
  {
    // TODO Auto-generated method stub
    
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.CacheObserver#afterSwitchingWriteAndFlushMaps()
   */
  public void afterSwitchingWriteAndFlushMaps()
  {

  }

  public void afterSettingDiskRef()
  {    
  }

  public void beforeSettingDiskRef()
  {    
  }

  public void beforeDeletingCompactedOplog(Oplog compactedOplog)
  {
  }
  public void beforeDeletingEmptyOplog(Oplog emptyOplog)
  {
  }
}
