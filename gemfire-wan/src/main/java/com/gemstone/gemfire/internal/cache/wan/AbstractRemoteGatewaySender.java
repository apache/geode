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
package com.gemstone.gemfire.internal.cache.wan;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorRequest;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorResponse;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.WanLocatorDiscoverer;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;

public abstract class AbstractRemoteGatewaySender extends AbstractGatewaySender {
  private static final Logger logger = LogService.getLogger();
  
  public AbstractRemoteGatewaySender() {
    
  }
  public AbstractRemoteGatewaySender(Cache cache, GatewaySenderAttributes attrs){
    super(cache, attrs);
  }
  
  /** used to reduce warning logs in case remote locator is down (#47634) */ 
  protected int proxyFailureTries = 0; 
  
  public synchronized void initProxy() {
    // return if it is being used for WBCL or proxy is already created
    if (this.remoteDSId == DEFAULT_DISTRIBUTED_SYSTEM_ID || this.proxy != null
        && !this.proxy.isDestroyed()) {
      return;
    }

    int locatorCount = 0;
    PoolFactoryImpl pf = (PoolFactoryImpl) PoolManager.createFactory();
    pf.setPRSingleHopEnabled(false);
    if (this.locatorDiscoveryCallback != null) {
      pf.setLocatorDiscoveryCallback(locatorDiscoveryCallback);
    }
    pf.setReadTimeout(this.socketReadTimeout);
    pf.setIdleTimeout(connectionIdleTimeOut);
    pf.setSocketBufferSize(socketBufferSize);
    pf.setServerGroup(GatewayReceiver.RECEIVER_GROUP);
    RemoteLocatorRequest request = new RemoteLocatorRequest(this.remoteDSId, pf
        .getPoolAttributes().getServerGroup());
    String locators = ((GemFireCacheImpl) this.cache).getDistributedSystem()
        .getConfig().getLocators();
    if (logger.isDebugEnabled()) {
      logger.debug("Gateway Sender is attempting to configure pool with remote locator information");
    }
    StringTokenizer locatorsOnThisVM = new StringTokenizer(locators, ",");
    while (locatorsOnThisVM.hasMoreTokens()) {
      String localLocator = locatorsOnThisVM.nextToken();
      DistributionLocatorId locatorID = new DistributionLocatorId(localLocator);
      try {
        RemoteLocatorResponse response = (RemoteLocatorResponse) TcpClient
            .requestToServer(locatorID.getHost(), locatorID.getPort(), request,
                WanLocatorDiscoverer.WAN_LOCATOR_CONNECTION_TIMEOUT);

        if (response != null) {
          if (response.getLocators() == null) {
            if (logProxyFailure()) {
              logger.warn(LocalizedMessage.create(
                  LocalizedStrings.AbstractGatewaySender_REMOTE_LOCATOR_FOR_REMOTE_SITE_0_IS_NOT_AVAILABLE_IN_LOCAL_LOCATOR_1,
                      new Object[] { remoteDSId, localLocator }));
            }
            continue;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Received the remote site {} location information:", this.remoteDSId, response.getLocators());
          }
          StringBuffer strBuffer = new StringBuffer();
          Iterator<String> itr = response.getLocators().iterator();
          while (itr.hasNext()) {
            DistributionLocatorId locatorId = new DistributionLocatorId(itr.next());
            pf.addLocator(locatorId.getHost().getHostName(), locatorId.getPort());
            locatorCount++;
          }
          break;
        }
      } catch (IOException ioe) {
        if (logProxyFailure()) {
          // don't print stack trace for connection failures
          String ioeStr = "";
          if (!logger.isDebugEnabled() && ioe instanceof ConnectException) {
            ioeStr = ": " + ioe.toString();
            ioe = null;
          }
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.AbstractGatewaySender_SENDER_0_IS_NOT_ABLE_TO_CONNECT_TO_LOCAL_LOCATOR_1,
                new Object[] { this.id, localLocator + ioeStr  }), ioe);        
        }
        continue;
      } catch (ClassNotFoundException e) {
        if (logProxyFailure()) {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.AbstractGatewaySender_SENDER_0_IS_NOT_ABLE_TO_CONNECT_TO_LOCAL_LOCATOR_1,
                  new Object[] { this.id, localLocator }), e);
        }
        continue;
      }
    }

    if (locatorCount == 0) {
      if (logProxyFailure()) {
        logger.fatal(LocalizedMessage.create(
            LocalizedStrings.AbstractGatewaySender_SENDER_0_COULD_NOT_GET_REMOTE_LOCATOR_INFORMATION_FOR_SITE_1,
                new Object[] { this.id, this.remoteDSId }));
      }
      this.proxyFailureTries++;
      throw new GatewaySenderConfigurationException(
          LocalizedStrings.AbstractGatewaySender_SENDER_0_COULD_NOT_GET_REMOTE_LOCATOR_INFORMATION_FOR_SITE_1
              .toLocalizedString(new Object[] { this.id, this.remoteDSId}));
    }
    pf.init(this);
    this.proxy = ((PoolImpl) pf.create(this.getId()));
    if (this.proxyFailureTries > 0) {
      logger.info(LocalizedMessage.create(LocalizedStrings.AbstractGatewaySender_SENDER_0_GOT_REMOTE_LOCATOR_INFORMATION_FOR_SITE_1,
              new Object[] { this.id, this.remoteDSId, this.proxyFailureTries }));
      this.proxyFailureTries = 0;
    }
  }
  
  protected boolean logProxyFailure() {
    assert Thread.holdsLock(this);
    // always log the first failure
    if (logger.isDebugEnabled() || this.proxyFailureTries == 0) {
      return true;
    } else {
      // subsequent failures will be logged on 30th, 300th, 3000th try
      // each try is at 100millis from higher layer so this accounts for logging
      // after 3s, 30s and then every 5mins
      if (this.proxyFailureTries >= 3000) {
        return (this.proxyFailureTries % 3000) == 0;
      } else {
        return (this.proxyFailureTries == 30 || this.proxyFailureTries == 300);
      }
    }
  }
}
