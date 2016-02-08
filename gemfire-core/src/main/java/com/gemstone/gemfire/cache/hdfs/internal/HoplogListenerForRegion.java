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
