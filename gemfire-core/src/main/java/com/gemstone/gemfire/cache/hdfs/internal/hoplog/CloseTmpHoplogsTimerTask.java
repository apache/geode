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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.util.Collection;

import org.apache.hadoop.fs.FileSystem;

import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.gemstone.gemfire.internal.SystemTimer.SystemTimerTask;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

import org.apache.logging.log4j.Logger;

/**
 * For streaming case, if the bucket traffic goes down after writing few batches of data, 
 * the flush doesn't get called. In that case, the file is left in tmp state
 * until the flush restarts. To avoid this issue, added this timer task 
 * that periodically iterates over the buckets and closes their writer 
 * if the time for rollover has passed.
 * 
 * It also has got an extra responsibility of fixing the file sizes of the files 
 * that weren't closed properly last time. 
 *
 * @author hemantb
 *
 */
class CloseTmpHoplogsTimerTask extends SystemTimerTask {
  
  private HdfsRegionManager hdfsRegionManager;
  private static final Logger logger = LogService.getLogger();
  private FileSystem filesystem; 
  
  public CloseTmpHoplogsTimerTask(HdfsRegionManager hdfsRegionManager) {
    this.hdfsRegionManager = hdfsRegionManager;
    
    // Create a new filesystem 
    // This is added for the following reason:
    // For HDFS, if a file wasn't closed properly last time, 
    // while calling FileSystem.append for this file, FSNamesystem.startFileInternal->
    // FSNamesystem.recoverLeaseInternal function gets called. 
    // This function throws AlreadyBeingCreatedException if there is an open handle, to any other file, 
    // created using the same FileSystem object. This is a bug and is being tracked at: 
    // https://issues.apache.org/jira/browse/HDFS-3848?page=com.atlassian.jira.plugin.system.issuetabpanels:all-tabpanel
    // 
    // The fix for this bug is not yet part of Pivotal HD. So to overcome the bug, 
    // we create a new file system for the timer task so that it does not encounter the bug. 
    this.filesystem = this.hdfsRegionManager.getStore().createFileSystem();
    if (logger.isDebugEnabled()) 
      logger.debug("created a new file system specifically for timer task");
  }

  
  /**
   * Iterates over all the bucket organizers and closes their writer if the time for 
   * rollover has passed. It also has the additional responsibility of fixing the tmp
   * files that were left over in the last unsuccessful run. 
   */
  @Override
  public void run2() {
    Collection<HoplogOrganizer> organizers =  hdfsRegionManager.getBucketOrganizers();
    if (logger.isDebugEnabled()) 
      logger.debug("Starting the close temp logs run.");
    
    for (HoplogOrganizer organizer: organizers) {
      
      HDFSUnsortedHoplogOrganizer unsortedOrganizer = (HDFSUnsortedHoplogOrganizer)organizer;
      long timeSinceLastFlush = (System.currentTimeMillis() - unsortedOrganizer.getLastFlushTime())/1000 ;
      try {
        this.hdfsRegionManager.getRegion().checkReadiness();
      } catch (Exception e) {
        break;
      }
      
      try {
        // the time since last flush has exceeded file rollover interval, roll over the 
        // file. 
        if (timeSinceLastFlush >= unsortedOrganizer.getfileRolloverInterval()) {
          if (logger.isDebugEnabled()) 
            logger.debug("Closing writer for bucket: " + unsortedOrganizer.bucketId);
          unsortedOrganizer.synchronizedCloseWriter(false, timeSinceLastFlush, 0);
        }
        
        // fix the tmp hoplogs, if any. Pass the new file system here. 
        unsortedOrganizer.identifyAndFixTmpHoplogs(this.filesystem);
        
      } catch (Exception e) {
        logger.warn(LocalizedStrings.HOPLOG_CLOSE_FAILED, e);
      }
    }
    
  }
}

