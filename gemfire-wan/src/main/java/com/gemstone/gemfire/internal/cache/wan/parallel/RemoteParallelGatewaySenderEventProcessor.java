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
package com.gemstone.gemfire.internal.cache.wan.parallel;

import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.pooling.ConnectionDestroyedException;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderConfigurationException;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackDispatcher;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventDispatcher;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventRemoteDispatcher;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderException;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderStats;
import com.gemstone.gemfire.internal.logging.LogService;

public class RemoteParallelGatewaySenderEventProcessor extends ParallelGatewaySenderEventProcessor {
  private static final Logger logger = LogService.getLogger();
  
  protected RemoteParallelGatewaySenderEventProcessor(
      AbstractGatewaySender sender) {
    super(sender);
  }
  
  /**
   * use in concurrent scenario where queue is to be shared among all the processors.
   */
  protected RemoteParallelGatewaySenderEventProcessor(AbstractGatewaySender sender,  Set<Region> userRegions, int id, int nDispatcher) {
    super(sender,  userRegions, id, nDispatcher);
  }
  
  @Override
  protected void rebalance() {
    GatewaySenderStats statistics = this.sender.getStatistics();
    long startTime = statistics.startLoadBalance();
    try {
      if (this.dispatcher.isRemoteDispatcher()) {
        GatewaySenderEventRemoteDispatcher remoteDispatcher = (GatewaySenderEventRemoteDispatcher) this.dispatcher;
        if (remoteDispatcher.isConnectedToRemote()) {
          remoteDispatcher.stopAckReaderThread();
          remoteDispatcher.destroyConnection();
        }
      }
    } finally {
      statistics.endLoadBalance(startTime);
    }
  }
  
  public void initializeEventDispatcher() {
    if (logger.isDebugEnabled()) {
      logger.debug(" Creating the GatewayEventRemoteDispatcher");
    }
    if (this.sender.getRemoteDSId() != GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID) {
      this.dispatcher = new GatewaySenderEventRemoteDispatcher(this);
    }
  }
  
  /**
   * Returns if corresponding receiver WAN site of this GatewaySender has
   * GemfireVersion > 7.0.1
   * 
   * @param disp
   * @return true if remote site Gemfire Version is >= 7.0.1
   */
  private boolean shouldSendVersionEvents(GatewaySenderEventDispatcher disp)
      throws GatewaySenderException {
      try {
        GatewaySenderEventRemoteDispatcher remoteDispatcher = (GatewaySenderEventRemoteDispatcher) disp;
        // This will create a new connection if no batch has been sent till
        // now.
        Connection conn = remoteDispatcher.getConnection(false);
        if (conn != null) {
          short remoteSiteVersion = conn.getWanSiteVersion();
          if (Version.GFE_701.compareTo(remoteSiteVersion) <= 0) {
            return true;
          }
        }
      } catch (GatewaySenderException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException
            || e instanceof GatewaySenderConfigurationException
            || cause instanceof ConnectionDestroyedException) {
          try {
            int sleepInterval = GatewaySender.CONNECTION_RETRY_INTERVAL;
            if (logger.isDebugEnabled()) {
              logger.debug("Sleeping for {} milliseconds", sleepInterval);
            }
            Thread.sleep(sleepInterval);
          } catch (InterruptedException ie) {
            // log the exception
            if (logger.isDebugEnabled()){
              logger.debug(ie.getMessage(), ie);
            }
          }
        }
        throw e;
      }
    return false;
  }

}
