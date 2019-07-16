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
package org.apache.geode.cache.client.internal.locator.wan;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.logging.LogService;

/**
 * An implementation of
 * {@link org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener}
 *
 *
 */
public class LocatorMembershipListenerImpl implements LocatorMembershipListener {
  private static final Logger logger = LogService.getLogger();
  static final String WAN_LOCATORS_DISTRIBUTOR_THREAD_NAME = "locatorsDistributorThread";
  static final int WAN_LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS =
      Integer.getInteger("WANLocator.LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS", 3);
  static final int WAN_LOCATOR_DISTRIBUTION_RETRY_INTERVAL =
      Integer.getInteger("WANLocator.LOCATOR_DISTRIBUTION_RETRY_INTERVAL", 5000);

  private int port;
  private TcpClient tcpClient;
  private DistributionConfig config;
  private ConcurrentMap<Integer, Set<String>> allServerLocatorsInfo = new ConcurrentHashMap<>();
  private ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo =
      new ConcurrentHashMap<>();

  LocatorMembershipListenerImpl(TcpClient tcpClient) {
    this.tcpClient = tcpClient;
  }

  LocatorMembershipListenerImpl() {
    this.tcpClient = new TcpClient();
  }

  @Override
  public void setPort(int port) {
    this.port = port;
  }

  @Override
  public void setConfig(DistributionConfig config) {
    this.config = config;
  }

  /**
   * When the new locator is added to remote locator metadata, inform all other locators in remote
   * locator metadata about the new locator so that they can update their remote locator metadata.
   *
   * @param distributedSystemId DistributedSystemId of the joining locator.
   * @param locator DistributionLocatorId of the joining locator.
   * @param sourceLocator DistributionLocatorId of the locator that notified us about the new
   *        locator.
   */
  @Override
  public void locatorJoined(final int distributedSystemId, final DistributionLocatorId locator,
      final DistributionLocatorId sourceLocator) {
    // DistributionLocatorId for local locator.
    DistributionLocatorId localLocatorId;
    String localLocator = config.getStartLocator();
    if (localLocator.equals(DistributionConfig.DEFAULT_START_LOCATOR)) {
      localLocatorId = new DistributionLocatorId(port, config.getBindAddress());
    } else {
      localLocatorId = new DistributionLocatorId(localLocator);
    }

    // Copy of the current list of known locators.
    ConcurrentMap<Integer, Set<DistributionLocatorId>> remoteLocators = getAllLocatorsInfo();
    Map<Integer, Set<DistributionLocatorId>> localCopy = new HashMap<>();
    for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : remoteLocators.entrySet()) {
      Set<DistributionLocatorId> value = new CopyOnWriteHashSet<>(entry.getValue());
      localCopy.put(entry.getKey(), value);
    }

    // Remove those locators that don't need to be notified (joining, local and source).
    List<DistributionLocatorId> locatorsToRemove =
        Arrays.asList(locator, localLocatorId, sourceLocator);
    for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : localCopy.entrySet()) {
      for (DistributionLocatorId removeLocId : locatorsToRemove) {
        entry.getValue().remove(removeLocId);
      }
    }

    // Launch Locators Distributor thread.
    Runnable distributeLocatorsRunnable = new DistributeLocatorsRunnable(tcpClient, localLocatorId,
        localCopy, distributedSystemId, locator);
    Thread distributeLocatorsThread =
        new Thread(distributeLocatorsRunnable, WAN_LOCATORS_DISTRIBUTOR_THREAD_NAME);
    distributeLocatorsThread.setDaemon(true);
    distributeLocatorsThread.start();
  }

  @Override
  public Object handleRequest(Object request) {
    Object response = null;

    if (request instanceof RemoteLocatorJoinRequest) {
      response = updateAllLocatorInfo((RemoteLocatorJoinRequest) request);
    } else if (request instanceof LocatorJoinMessage) {
      response = informAboutRemoteLocators((LocatorJoinMessage) request);
    } else if (request instanceof RemoteLocatorPingRequest) {
      response = getPingResponse((RemoteLocatorPingRequest) request);
    } else if (request instanceof RemoteLocatorRequest) {
      response = getRemoteLocators((RemoteLocatorRequest) request);
    }

    return response;
  }

  /**
   * A locator from the request is checked against the existing remote locator metadata. If it is
   * not available then added to existing remote locator metadata and LocatorMembershipListener is
   * invoked to inform about the this newly added locator to all other locators available in remote
   * locator metadata. As a response, remote locator metadata is sent.
   *
   */
  private synchronized Object updateAllLocatorInfo(RemoteLocatorJoinRequest request) {
    int distributedSystemId = request.getDistributedSystemId();
    DistributionLocatorId locator = request.getLocator();

    LocatorHelper.addLocator(distributedSystemId, locator, this, null);
    return new RemoteLocatorJoinResponse(this.getAllLocatorsInfo());
  }

  @SuppressWarnings("unused")
  private Object getPingResponse(RemoteLocatorPingRequest request) {
    return new RemoteLocatorPingResponse();
  }

  private Object informAboutRemoteLocators(LocatorJoinMessage request) {
    // TODO: FInd out the importance of list locatorJoinMessages. During
    // refactoring I could not understand its significance
    // synchronized (locatorJoinObject) {
    // if (locatorJoinMessages.contains(request)) {
    // return null;
    // }
    // locatorJoinMessages.add(request);
    // }
    int distributedSystemId = request.getDistributedSystemId();
    DistributionLocatorId locator = request.getLocator();
    DistributionLocatorId sourceLocatorId = request.getSourceLocator();

    LocatorHelper.addLocator(distributedSystemId, locator, this, sourceLocatorId);
    return null;
  }

  private Object getRemoteLocators(RemoteLocatorRequest request) {
    int dsId = request.getDsId();
    Set<String> locators = this.getRemoteLocatorInfo(dsId);
    return new RemoteLocatorResponse(locators);
  }

  @Override
  public Set<String> getRemoteLocatorInfo(int dsId) {
    return this.allServerLocatorsInfo.get(dsId);
  }

  @Override
  public ConcurrentMap<Integer, Set<DistributionLocatorId>> getAllLocatorsInfo() {
    return this.allLocatorsInfo;
  }

  @Override
  public ConcurrentMap<Integer, Set<String>> getAllServerLocatorsInfo() {
    return this.allServerLocatorsInfo;
  }

  @Override
  public void clearLocatorInfo() {
    allLocatorsInfo.clear();
    allServerLocatorsInfo.clear();
  }

  private static class DistributeLocatorsRunnable implements Runnable {
    private final TcpClient tcpClient;
    private final DistributionLocatorId localLocatorId;
    private final Map<Integer, Set<DistributionLocatorId>> remoteLocators;
    private final int distributedSystemId;
    private final DistributionLocatorId locator;

    DistributeLocatorsRunnable(TcpClient tcpClient,
        DistributionLocatorId localLocatorId,
        Map<Integer, Set<DistributionLocatorId>> remoteLocators,
        int distributedSystemId,
        DistributionLocatorId locator) {
      this.tcpClient = tcpClient;
      this.localLocatorId = localLocatorId;
      this.distributedSystemId = distributedSystemId;
      this.locator = locator;
      this.remoteLocators = remoteLocators;
    }

    private void sendMessage(InetSocketAddress ipAddr, LocatorJoinMessage locatorJoinMessage,
        Map<InetSocketAddress, Set<LocatorJoinMessage>> failedMessages) {
      try {
        tcpClient.requestToServer(ipAddr, locatorJoinMessage, 1000, false);
      } catch (Exception exception) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Locator Membership listener could not exchange locator information {}:{} with {}:{}",
              new Object[] {locator.getHostName(), locator.getPort(), ipAddr.getHostName(),
                  ipAddr.getPort(), exception});
        }

        if (!failedMessages.containsKey(ipAddr)) {
          failedMessages.put(ipAddr, new HashSet<>());
        }

        failedMessages.get(ipAddr).add(locatorJoinMessage);
      }
    }

    private boolean retryMessage(InetSocketAddress ipAddr, LocatorJoinMessage locatorJoinMessage,
        int retryAttempt) {
      try {
        tcpClient.requestToServer(ipAddr, locatorJoinMessage, 1000, false);

        return true;
      } catch (Exception exception) {
        if (retryAttempt == WAN_LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS) {
          logger.warn(
              "Locator Membership listener failed to exchange locator information {}:{} with {}:{} after {} retry attempts",
              new Object[] {locator.getHostName(), locator.getPort(), ipAddr.getHostName(),
                  ipAddr.getPort(), retryAttempt, exception});
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "Locator Membership listener could not exchange locator information {}:{} with {}:{} after {} retry attempts",
                new Object[] {locator.getHostName(), locator.getPort(), ipAddr.getHostName(),
                    ipAddr.getPort(), retryAttempt, exception});
          }
        }

        return false;
      }
    }

    @Override
    public void run() {
      Map<InetSocketAddress, Set<LocatorJoinMessage>> failedMessages = new HashMap<>();
      for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : remoteLocators.entrySet()) {
        for (DistributionLocatorId value : entry.getValue()) {
          try {
            LocatorJoinMessage locatorJoinMessage =
                new LocatorJoinMessage(distributedSystemId, locator, localLocatorId, "");
            sendMessage(value.getHost(), locatorJoinMessage, failedMessages);
          } catch (UnknownHostException unknownHostException) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Locator Membership listener could not determine IP address of {}",
                  new Object[] {value, unknownHostException});
            }
          }

          try {
            LocatorJoinMessage locatorJoinMessage =
                new LocatorJoinMessage(entry.getKey(), value, localLocatorId, "");
            sendMessage(locator.getHost(), locatorJoinMessage, failedMessages);
          } catch (UnknownHostException unknownHostException) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Locator Membership listener could not determine IP address of {}",
                  new Object[] {locator, unknownHostException});
            }
          }
        }
      }

      // Retry failed messages and remove those that succeed.
      if (!failedMessages.isEmpty()) {
        // Sleep before trying again.
        try {
          Thread.sleep(WAN_LOCATOR_DISTRIBUTION_RETRY_INTERVAL);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        for (int retryAttempt = 1; retryAttempt <= WAN_LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS;
             retryAttempt++) {
          for (Map.Entry<InetSocketAddress, Set<LocatorJoinMessage>> entry :
              failedMessages.entrySet()) {
            InetSocketAddress targetLocatorAddress = entry.getKey();
            Set<LocatorJoinMessage> joinMessages = entry.getValue();

            for (LocatorJoinMessage locatorJoinMessage : joinMessages) {
              if (retryMessage(targetLocatorAddress, locatorJoinMessage, retryAttempt)) {
                joinMessages.remove(locatorJoinMessage);
              }
            }
          }
        }
      }
    }
  }
}
