/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.Hoplog;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogListener;

/**
 * Objects of this class needs to be created for every region. These objects 
 * listen to the oplog events and take appropriate action.   
 *
 * @author Hemant Bhanawat
 */
public class HoplogListenerForRegion implements HoplogListener {

  private List<HoplogListener> otherListeners = new CopyOnWriteArrayList<HoplogListener>();

  public HoplogListenerForRegion() {
    
  }

  @Override
  public void hoplogCreated(String regionFolder, int bucketId,
      Hoplog... oplogs) throws IOException {
    for (HoplogListener listener : this.otherListeners) {
      listener.hoplogCreated(regionFolder, bucketId, oplogs);
    }
  }

  @Override
  public void hoplogDeleted(String regionFolder, int bucketId,
      Hoplog... oplogs) {
    for (HoplogListener listener : this.otherListeners) {
      try {
        listener.hoplogDeleted(regionFolder, bucketId, oplogs);
      } catch (IOException e) {
        // TODO handle
        throw new HDFSIOException(e.getLocalizedMessage(), e);
      }
    }
  }

  public void addListener(HoplogListener listener) {
    this.otherListeners.add(listener);
  }

  @Override
  public void compactionCompleted(String region, int bucket, boolean isMajor) {
    for (HoplogListener listener : this.otherListeners) {
      listener.compactionCompleted(region, bucket, isMajor);
    }
  }
}
