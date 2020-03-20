/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.wan;


import java.io.IOException;
import java.net.ConnectException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorResponse;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.internal.WanLocatorDiscoverer;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;

public abstract class AbstractRemoteGatewaySender extends AbstractGatewaySender {
  private static final Logger logger = LogService.getLogger();

  /** used to reduce warning logs in case remote locator is down (#47634) */
  protected int proxyFailureTries = 0;

  public AbstractRemoteGatewaySender(InternalCache cache, StatisticsClock statisticsClock,
      GatewaySenderAttributes attrs) {
    super(cache, statisticsClock, attrs);
  }

  @Override
  public synchronized void initProxy() {
    // return if it is being used for WBCL or proxy is already created
    if (this.remoteDSId == DEFAULT_DISTRIBUTED_SYSTEM_ID
        || this.proxy != null && !this.proxy.isDestroyed()) {
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
    RemoteLocatorRequest request =
        new RemoteLocatorRequest(this.remoteDSId, pf.getPoolAttributes().getServerGroup());
    String locators = this.cache.getInternalDistributedSystem().getConfig().getLocators();
    if (logger.isDebugEnabled()) {
      logger
          .debug("Gateway Sender is attempting to configure pool with remote locator information");
    }
    StringTokenizer locatorsOnThisVM = new StringTokenizer(locators, ",");
    while (locatorsOnThisVM.hasMoreTokens()) {
      String localLocator = locatorsOnThisVM.nextToken();
      DistributionLocatorId locatorID = new DistributionLocatorId(localLocator);
      try {
        RemoteLocatorResponse response =
            (RemoteLocatorResponse) new TcpClient(SocketCreatorFactory
                .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
                InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
                InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
                TcpSocketFactory.DEFAULT)
                    .requestToServer(locatorID.getHost(), request,
                        WanLocatorDiscoverer.WAN_LOCATOR_CONNECTION_TIMEOUT, true);

        if (response != null) {
          if (response.getLocators() == null) {
            if (logProxyFailure()) {
              logger.warn(
                  "Remote locator host port information for remote site {} is not available in local locator {}.",
                  new Object[] {remoteDSId, localLocator});
            }
            continue;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Received the remote site {} location information:", this.remoteDSId,
                response.getLocators());
          }
          Iterator<String> itr = response.getLocators().iterator();
          while (itr.hasNext()) {
            String remoteLocator = itr.next();
            try {
              DistributionLocatorId locatorId = new DistributionLocatorId(remoteLocator);
              pf.addLocator(locatorId.getHost().getHostName(), locatorId.getPort());
              locatorCount++;
            } catch (Exception e) {
              if (logProxyFailure()) {
                logger.warn(String.format(
                    "Caught the following exception attempting to add remote locator %s. The locator will be ignored.",
                    new Object[] {remoteLocator}),
                    e);
              }
            }
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
          logger.warn(String.format("GatewaySender %s is not able to connect to local locator %s",
              new Object[] {this.id, localLocator + ioeStr}),
              ioe);
        }
        continue;
      } catch (ClassNotFoundException e) {
        if (logProxyFailure()) {
          logger.warn(String.format("GatewaySender %s is not able to connect to local locator %s",
              new Object[] {this.id, localLocator}),
              e);
        }
        continue;
      }
    }

    if (locatorCount == 0) {
      if (logProxyFailure()) {
        logger.fatal(
            "GatewaySender {} could not get remote locator information for remote site {}.",
            new Object[] {this.id, this.remoteDSId});
      }
      this.proxyFailureTries++;
      throw new GatewaySenderConfigurationException(
          String.format(
              "GatewaySender %s could not get remote locator information for remote site %s.",
              new Object[] {this.id, this.remoteDSId}));
    }
    pf.init(this);
    this.proxy = ((PoolImpl) pf.create(this.getId()));
    if (this.proxyFailureTries > 0) {
      logger.info(
          "GatewaySender {} got remote locator information for remote site {} after {} failures in connecting to remote site.",
          new Object[] {this.id, this.remoteDSId, this.proxyFailureTries});
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
