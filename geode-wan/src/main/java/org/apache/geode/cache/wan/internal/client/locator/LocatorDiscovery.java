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
package org.apache.geode.cache.wan.internal.client.locator;


import static org.apache.geode.distributed.internal.WanLocatorDiscoverer.WAN_LOCATOR_CONNECTION_TIMEOUT;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorJoinRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorJoinResponse;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorPingRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorPingResponse;
import org.apache.geode.distributed.internal.WanLocatorDiscoverer;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.tcp.ConnectionException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This class represents a runnable task which exchanges the locator information with local
 * locators (within the site) as well as remote locators (across the site)
 *
 * @since GemFire 7.0
 */
public class LocatorDiscovery {

  private static final Logger logger = LogService.getLogger();

  private final WanLocatorDiscoverer discoverer;

  private final DistributionLocatorId locatorId;

  private final LocatorMembershipListener locatorListener;

  RemoteLocatorJoinRequest request;

  TcpClient locatorClient;

  public static final int WAN_LOCATOR_CONNECTION_RETRY_ATTEMPT =
      Integer.getInteger("WANLocator.CONNECTION_RETRY_ATTEMPT", 50000);

  public static final int WAN_LOCATOR_CONNECTION_INTERVAL =
      Integer.getInteger("WANLocator.CONNECTION_INTERVAL", 10000);

  public static final int WAN_LOCATOR_PING_INTERVAL =
      Integer.getInteger("WANLocator.PING_INTERVAL", 10000);

  // For testing. When true, Thread.sleep() is not called in exchangeLocalLocators() or
  // exchangeRemoteLocators()
  private final boolean skipWaiting;

  public LocatorDiscovery(WanLocatorDiscoverer discoverer, DistributionLocatorId locator,
      RemoteLocatorJoinRequest request, LocatorMembershipListener locatorListener) {
    this.discoverer = discoverer;
    locatorId = locator;
    this.request = request;
    this.locatorListener = locatorListener;
    locatorClient = new TcpClient(SocketCreatorFactory
        .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);
    skipWaiting = false;
  }

  // Test constructor
  @VisibleForTesting
  LocatorDiscovery(WanLocatorDiscoverer discoverer, DistributionLocatorId locator,
      RemoteLocatorJoinRequest request, LocatorMembershipListener locatorListener,
      TcpClient locatorClient) {
    this.discoverer = discoverer;
    locatorId = locator;
    this.request = request;
    this.locatorListener = locatorListener;
    this.locatorClient = locatorClient;
    skipWaiting = true;
  }

  /**
   * When a batch fails, then this keeps the last time when a failure was logged . We don't want to
   * swamp the logs in retries due to same batch failures.
   */
  private final ConcurrentHashMap<DistributionLocatorId, long[]> failureLogInterval =
      new ConcurrentHashMap<>();

  /**
   * The maximum size of {@link #failureLogInterval} beyond which it will start logging all failure
   * instances. Hopefully this should never happen in practice.
   */
  private static final int FAILURE_MAP_MAXSIZE = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "GatewaySender.FAILURE_MAP_MAXSIZE", 1000000);

  /**
   * The maximum interval for logging failures of the same event in millis.
   */
  private static final int FAILURE_LOG_MAX_INTERVAL = Integer.getInteger(
      GeodeGlossary.GEMFIRE_PREFIX + "LocatorDiscovery.FAILURE_LOG_MAX_INTERVAL", 300000);

  public boolean skipFailureLogging(DistributionLocatorId locatorId) {
    boolean skipLogging = false;
    if (failureLogInterval.size() < FAILURE_MAP_MAXSIZE) {
      long[] logInterval = failureLogInterval.get(locatorId);
      if (logInterval == null) {
        logInterval = failureLogInterval.putIfAbsent(locatorId,
            new long[] {System.currentTimeMillis(), 1000});
      }
      if (logInterval != null) {
        long currentTime = System.currentTimeMillis();
        if ((currentTime - logInterval[0]) < logInterval[1]) {
          skipLogging = true;
        } else {
          logInterval[0] = currentTime;
          if (logInterval[1] <= (FAILURE_LOG_MAX_INTERVAL / 2)) {
            logInterval[1] *= 2;
          }
        }
      }
    }
    return skipLogging;
  }


  public class LocalLocatorDiscovery implements Runnable {
    @Override
    public void run() {
      exchangeLocalLocators();
    }
  }

  public class RemoteLocatorDiscovery implements Runnable {
    @Override
    public void run() {
      exchangeRemoteLocators();
    }
  }

  private void exchangeLocalLocators() {
    int retryAttempt = 1;
    while (!discoverer.isStopped()) {
      try {
        RemoteLocatorJoinResponse response = (RemoteLocatorJoinResponse) locatorClient
            .requestToServer(locatorId.getHost(), request, WAN_LOCATOR_CONNECTION_TIMEOUT, true);
        if (response != null) {
          addExchangedLocators(response);
          logger.info(
              "Locator discovery task for locator {} exchanged locator information with {}: {}.",
              request.getLocator(), locatorId, response.getLocators());
          break;
        }
      } catch (IOException ioe) {
        if (retryAttempt == WAN_LOCATOR_CONNECTION_RETRY_ATTEMPT) {
          ConnectionException coe =
              new ConnectionException("Not able to connect to local locator after "
                  + WAN_LOCATOR_CONNECTION_RETRY_ATTEMPT + " retry attempts", ioe);
          logger.fatal(
              "Locator discovery task for locator {} could not exchange locator information with {} after {} retry attempts.",
              request.getLocator(), locatorId, retryAttempt, coe);
          break;
        }
        if (skipFailureLogging(locatorId)) {
          logger.warn(
              "Locator discovery task for locator {} could not exchange locator information with {} after {} retry attempts. Retrying in {} ms.",
              request.getLocator(), locatorId, retryAttempt, WAN_LOCATOR_CONNECTION_INTERVAL);
        }
        try {
          if (!skipWaiting) {
            Thread.sleep(WAN_LOCATOR_CONNECTION_INTERVAL);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        retryAttempt++;
      } catch (ClassNotFoundException | ClassCastException ex) {
        logger.fatal("Locator discovery task encountered unexpected exception", ex);
        break;
      }
    }
  }

  public void exchangeRemoteLocators() {
    int retryAttempt = 1;
    while (!discoverer.isStopped()) {
      try {
        RemoteLocatorJoinResponse response = (RemoteLocatorJoinResponse) locatorClient
            .requestToServer(locatorId.getHost(), request, WAN_LOCATOR_CONNECTION_TIMEOUT, true);
        if (response != null) {
          addExchangedLocators(response);
          logger.info(
              "Locator discovery task for locator {} exchanged locator information with {}: {}.",
              request.getLocator(), locatorId, response.getLocators());
          RemoteLocatorPingRequest pingRequest = new RemoteLocatorPingRequest("");
          while (true) {
            if (!skipWaiting) {
              Thread.sleep(WAN_LOCATOR_PING_INTERVAL);
            }
            RemoteLocatorPingResponse pingResponse = (RemoteLocatorPingResponse) locatorClient
                .requestToServer(new HostAndPort(locatorId.getHostName(), locatorId.getPort()),
                    pingRequest, WAN_LOCATOR_CONNECTION_TIMEOUT, true);
            if (pingResponse != null) {
              continue;
            }
            break;
          }
        }
      } catch (IOException ioe) {
        if (retryAttempt == WAN_LOCATOR_CONNECTION_RETRY_ATTEMPT) {
          logger.fatal(
              "Locator discovery task for locator {} could not exchange locator information with {} after {} retry attempts.",
              request.getLocator(), locatorId, retryAttempt, ioe);
          break;
        }
        if (skipFailureLogging(locatorId)) {
          logger.warn(
              "Locator discovery task for locator {} could not exchange locator information with {} after {} retry attempts. Retrying in {} ms.",
              new Object[] {request.getLocator(), locatorId, retryAttempt,
                  WAN_LOCATOR_CONNECTION_INTERVAL});
        }
        try {
          if (!skipWaiting) {
            Thread.sleep(WAN_LOCATOR_CONNECTION_INTERVAL);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        retryAttempt++;
      } catch (ClassNotFoundException | ClassCastException ex) {
        logger.fatal("Locator discovery task encountered unexpected exception", ex);
        break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @VisibleForTesting
  void addExchangedLocators(RemoteLocatorJoinResponse response) {
    LocatorHelper.addExchangedLocators(response.getLocators(), locatorListener);
  }

}
