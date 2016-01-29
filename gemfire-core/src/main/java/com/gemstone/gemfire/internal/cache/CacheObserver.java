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

import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.RegionEvent;

/**
 * This interface is used by testing/debugging code to be notified of query
 * events. See the documentation for class CacheObserverHolder for details. Also
 * the callback is issed only if the boolean ISSUE_CALLBACKS_TO_CACHE_OBSERVER
 * present in com.gemstone.gemfire.internal.cache.LocalRegion is made true
 * 
 * @author Asif
 */
public interface CacheObserver
{

  /**
   * Called just after the region's Map is cleared & before Listener callback is
   * issued. The call to this method is synchronous
   */
  void afterRegionClear(RegionEvent event);
  
  /**
   * Called just beforeclearing the DiskRegion. 
   *
   */
  void beforeDiskClear();

  /**
   * callback to test flushing efficieny. This callback is issued just before
   * the flushing of the buffer that is before writing data to the Oplog, but
   * after setting the logical offsets in the DiskIds contained in the
   * PendingWrite Buffer
   *  
   */
  void goingToFlush();

  /**
   * called immediately after bytes are written to the disk Region. In case of
   * asynch mode, it gets called immedaitely after the asynch writer has written
   * it to disk & just before releasing the ByteBuffer to the pool.
   *  
   */
  public void afterWritingBytes();

  /**
   * 
   * Compacting is about to compact
   */
  public void beforeGoingToCompact();

  /**
   * Just finished Compacting
   *  
   */
  public void afterHavingCompacted();

  /**
   * Callback just after calculating the conflated byte buffer. This function
   * can get called only in the asynch mode where conflation can happen
   * 
   * @param origBB
   *          Original ByteBuffer object for the operation without considering
   *          conflation
   * @param conflatedBB
   *          Resultant ByteBuffer object after conflation
   */
  public void afterConflation(ByteBuffer origBB, ByteBuffer conflatedBB);

  /**
   * Callback just after setting oplog offset . The Oplog Offset will be set to
   * non negative number in case it is a synch mode operation as the offset for
   * synch mode is available in the context of thread performing the operation &
   * to -1 for an asynch mode of operation as in case of asynch mode of
   * operation the actual offset is determined only when asynch writer performs
   * the write operation.
   * 
   * @param offset
   *          A non negative number for synch mode of operation indicating the
   *          start position in the Oplog for the operation & -1 for asynch mode
   *          of operation
   *  
   */
  public void afterSettingOplogOffSet(long offset);

  /**
   * Callback given by the thread performing the operation which causes the
   * switching of the Oplog. This function gets invoked before a new Oplog gets
   * created. Thus if the compacting is on , this function will get called before
   * the compacter thread gets notified
   *  
   */
  public void beforeSwitchingOplog();

  /**
   * Callback given by the thread performing the operation which causes the
   * switching of the Oplog. This function gets invoked after a new Oplog gets
   * created. Thus if the compacting is on , this function will get called after
   * the compacter thread has been notified & the switching thread has been able to
   * create a new Oplog
   * 
   *  
   */
  public void afterSwitchingOplog();
  
  /**
   * Callback given by the thread which creates krfs.
   */
  public void afterKrfCreated();

  /**
   * Callback given immediately before any thread invokes
   * ComplexDiskRegion.OplogCompactor's stopCompactor method. This method normally
   * gets invoked by clear/destory/close methods of the region.
   *  
   */
  public void beforeStoppingCompactor();
  /**
   * Callback given immediately after any thread invokes
   * ComplexDiskRegion.OplogCompactor's stopCompactor method. This method normally
   * gets invoked by clear/destory/close methods of the region.
   *  
   */
  public void afterStoppingCompactor();
  /**
   * Callback given immediately after the
   * ComplexDiskRegion.OplogCompactor's stopCompactor method
   * signals the compactor to stop.
   *  
   */
  public void afterSignallingCompactor();

  /**
   * Called when GII begins
   */
  public void afterMarkingGIIStarted();
  /**
   * Called when GII ends
   */
  public void afterMarkingGIICompleted();
  
  /**
   * Called after the Oplog.WriterThread (asynch writer thread) swaps the
   * pendingFlushMap and pendingWriteMap for flushing.
   */
  public void afterSwitchingWriteAndFlushMaps();
  
  /**
   * Invoked just before setting the LBHTree reference in the thread local.  
   */
  public void beforeSettingDiskRef();
  
  /**
   * Invoked just after setting the LBHTree reference in the thread local.  
   */
  public void afterSettingDiskRef();
  
  /**
   * Invoked by the compactor thread just before deleting a compacted oplog 
   * @param compactedOplog
   */
  public void beforeDeletingCompactedOplog(Oplog compactedOplog);
  /**
   * Invoked just before deleting an empty oplog
   * @param emptyOplog
   */
  public void beforeDeletingEmptyOplog(Oplog emptyOplog);
  
  /**
   * Invoked just before ShutdownAll operation
   */
  void beforeShutdownAll();
}
