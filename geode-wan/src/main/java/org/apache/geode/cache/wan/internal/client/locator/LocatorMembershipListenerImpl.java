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


import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.client.internal.locator.wan.LocatorJoinMessage;
import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorJoinRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorJoinResponse;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorPingRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorPingResponse;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorRequest;
import org.apache.geode.cache.client.internal.locator.wan.RemoteLocatorResponse;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.logging.CoreLoggingExecutors;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * An implementation of
 * {@link org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener}
 *
 *
 */
public class LocatorMembershipListenerImpl implements LocatorMembershipListener {
  private static final Logger logger = LogService.getLogger();
  static final int LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS = 3;
  private static final String LOCATORS_DISTRIBUTOR_THREAD_NAME = "LocatorsDistributorThread";
  private static final String LISTENER_FAILURE_MESSAGE =
      "Locator Membership listener could not exchange locator information {}:{} with {}:{} after {} retry attempts";
  private static final String LISTENER_FINAL_FAILURE_MESSAGE =
      "Locator Membership listener permanently failed to exchange locator information {}:{} with {}:{} after {} retry attempts";

  /**
   * The minimum value of max-pool size
   */
  private static final int MINIMUM_MAX_POOL_SIZE = 16;

  private int port;
  private DistributionConfig config;
  private final TcpClient tcpClient;
  private final ConcurrentMap<Integer, Set<String>> allServerLocatorsInfo =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo =
      new ConcurrentHashMap<>();

  private ExecutorService notifyLocatorJoinPool;


  LocatorMembershipListenerImpl() {
    this.tcpClient = new TcpClient(SocketCreatorFactory
        .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);
    this.notifyLocatorJoinPool = null;
  }

  LocatorMembershipListenerImpl(TcpClient tcpClient) {
    this.tcpClient = tcpClient;
    this.notifyLocatorJoinPool = null;
  }

  @Override
  public void setPort(int port) {
    this.port = port;
  }

  @Override
  public void setConfig(DistributionConfig config) {
    this.config = config;
  }

  @Override
  public void start() {
    // Launch Locators Distributor thread.
    if (notifyLocatorJoinPool == null) {
      notifyLocatorJoinPool = createThreadPool();
    }
  }

  @Override
  public void stop() {
    if (notifyLocatorJoinPool != null) {
      notifyLocatorJoinPool.shutdown();
    }
  }

  @VisibleForTesting
  ExecutorService getNotifyLocatorJoinPool() {
    return notifyLocatorJoinPool;
  }

  void buildLocatorsDistributorThread(DistributionLocatorId localLocatorId,
      Map<Integer, Set<DistributionLocatorId>> remoteLocators, DistributionLocatorId joiningLocator,
      int joiningLocatorDistributedSystemId) {
    Runnable distributeLocatorsRunnable =
        new DistributeLocatorsRunnable(config.getMemberTimeout(), tcpClient, localLocatorId,
            remoteLocators, joiningLocator, joiningLocatorDistributedSystemId);
    try {
      notifyLocatorJoinPool.execute(distributeLocatorsRunnable);
    } catch (RejectedExecutionException rejected) {
      logger.warn(
          "Locator {} joining notification was rejected by pool possibly due to thread exhaustion",
          joiningLocator);
    }

  }

  /**
   * When the new locator is added to remote locator metadata, inform all other locators in remote
   * locator metadata about the new locator so that they can update their remote locator metadata.
   *
   * @param distributedSystemId Id of the joining locator.
   * @param locator Id of the joining locator.
   * @param sourceLocator Id of the locator that notified this locator about the new one.
   */
  @Override
  public void locatorJoined(final int distributedSystemId, final DistributionLocatorId locator,
      final DistributionLocatorId sourceLocator) {
    // DistributionLocatorId for local locator.
    DistributionLocatorId localLocatorId;

    String memberName = config.getName();
    String localLocator = config.getStartLocator();
    if (localLocator.equals(DistributionConfig.DEFAULT_START_LOCATOR)) {
      localLocatorId =
          new DistributionLocatorId(port, config.getBindAddress(), null, memberName);
    } else {
      localLocatorId = new DistributionLocatorId(localLocator, memberName);
    }

    // Make a local copy of the current list of known locators.
    ConcurrentMap<Integer, Set<DistributionLocatorId>> remoteLocators = getAllLocatorsInfo();
    Map<Integer, Set<DistributionLocatorId>> localCopy = new HashMap<>();
    for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : remoteLocators.entrySet()) {
      Set<DistributionLocatorId> value = new CopyOnWriteHashSet<>(entry.getValue());
      localCopy.put(entry.getKey(), value);
    }

    // Remove locators that don't need to be notified (myself, the joining one and the one that
    // notified myself).
    List<DistributionLocatorId> ignoreList = Arrays.asList(locator, localLocatorId, sourceLocator);
    for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : localCopy.entrySet()) {
      for (DistributionLocatorId removeLocId : ignoreList) {
        entry.getValue().remove(removeLocId);
      }
    }

    buildLocatorsDistributorThread(localLocatorId, localCopy, locator, distributedSystemId);
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

  private ExecutorService createThreadPool() {
    int numLocalLocs = 1;
    String localLocators = config.getLocators();
    if (localLocators != null) {
      numLocalLocs += StringUtils.countMatches(localLocators, ",");
    }
    String remoteLocators = config.getRemoteLocators();
    int numRemoteLocs = 0;
    if (remoteLocators != null) {
      numRemoteLocs = 1;
      numRemoteLocs += StringUtils.countMatches(remoteLocators, ",");
    }

    int maxPoolsize = numLocalLocs + numRemoteLocs;
    if (maxPoolsize < MINIMUM_MAX_POOL_SIZE) {
      maxPoolsize = MINIMUM_MAX_POOL_SIZE;
    }

    return CoreLoggingExecutors.newThreadPoolWithSynchronousFeed(0, maxPoolsize,
        0, SECONDS, LOCATORS_DISTRIBUTOR_THREAD_NAME);
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

  static class DistributeLocatorsRunnable implements Runnable {
    private final int memberTimeout;
    private final TcpClient tcpClient;
    private final DistributionLocatorId localLocatorId;
    private final Map<Integer, Set<DistributionLocatorId>> remoteLocators;
    private final DistributionLocatorId joiningLocator;
    private final int joiningLocatorDistributedSystemId;

    DistributeLocatorsRunnable(int memberTimeout,
        TcpClient tcpClient,
        DistributionLocatorId localLocatorId,
        Map<Integer, Set<DistributionLocatorId>> remoteLocators,
        DistributionLocatorId joiningLocator,
        int joiningLocatorDistributedSystemId) {

      this.memberTimeout = memberTimeout;
      this.tcpClient = tcpClient;
      this.localLocatorId = localLocatorId;
      this.remoteLocators = remoteLocators;
      this.joiningLocator = joiningLocator;
      this.joiningLocatorDistributedSystemId = joiningLocatorDistributedSystemId;
    }

    void sendMessage(DistributionLocatorId targetLocator, LocatorJoinMessage locatorJoinMessage,
        Map<DistributionLocatorId, Set<LocatorJoinMessage>> failedMessages) {
      DistributionLocatorId advertisedLocator = locatorJoinMessage.getLocator();

      try {
        tcpClient.requestToServer(targetLocator.getHost(),
            locatorJoinMessage, memberTimeout,
            false);
      } catch (Exception exception) {
        if (logger.isDebugEnabled()) {
          logger.debug(LISTENER_FAILURE_MESSAGE,
              new Object[] {advertisedLocator.getHostName(), advertisedLocator.getPort(),
                  targetLocator.getHostName(), targetLocator.getPort(), 1, exception});
        }

        if (!failedMessages.containsKey(targetLocator)) {
          failedMessages.put(targetLocator, new HashSet<>());
        }

        failedMessages.get(targetLocator).add(locatorJoinMessage);
      }
    }

    boolean retryMessage(DistributionLocatorId targetLocator, LocatorJoinMessage locatorJoinMessage,
        int retryAttempt) {
      DistributionLocatorId advertisedLocator = locatorJoinMessage.getLocator();

      try {
        tcpClient.requestToServer(targetLocator.getHost(),
            locatorJoinMessage, memberTimeout,
            false);

        return true;
      } catch (Exception exception) {
        if (retryAttempt == LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS) {
          logger.warn(LISTENER_FINAL_FAILURE_MESSAGE,
              new Object[] {advertisedLocator.getHostName(), advertisedLocator.getPort(),
                  targetLocator.getHostName(), targetLocator.getPort(), retryAttempt, exception});
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(LISTENER_FAILURE_MESSAGE,
                new Object[] {advertisedLocator.getHostName(), advertisedLocator.getPort(),
                    targetLocator.getHostName(), targetLocator.getPort(), retryAttempt, exception});
          }
        }

        return false;
      }
    }

    public void notifyRemoteLocatorAndJoiningLocator(DistributionLocatorId remoteLocator,
        int remoteLocatorDistributedSystemId, DistributionLocatorId joiningLocator,
        Map<DistributionLocatorId, Set<LocatorJoinMessage>> failedMessages) {
      if (!remoteLocator.equals(joiningLocator)) {
        // Notify known remote locator about the advertised locator.
        LocatorJoinMessage advertiseNewLocatorMessage = new LocatorJoinMessage(
            joiningLocatorDistributedSystemId, joiningLocator, localLocatorId, "");
        sendMessage(remoteLocator, advertiseNewLocatorMessage, failedMessages);

        // Notify the advertised locator about remote known locator.
        LocatorJoinMessage advertiseKnownLocatorMessage =
            new LocatorJoinMessage(remoteLocatorDistributedSystemId, remoteLocator, localLocatorId,
                "");
        sendMessage(joiningLocator, advertiseKnownLocatorMessage, failedMessages);
      }
    }

    @Override
    public void run() {
      Map<DistributionLocatorId, Set<LocatorJoinMessage>> failedMessages = new HashMap<>();
      for (Map.Entry<Integer, Set<DistributionLocatorId>> entry : remoteLocators.entrySet()) {
        for (DistributionLocatorId remoteLocator : entry.getValue()) {
          notifyRemoteLocatorAndJoiningLocator(remoteLocator, entry.getKey(), joiningLocator,
              failedMessages);
        }
      }

      // Retry failed messages and remove those that succeed.
      if (!failedMessages.isEmpty()) {
        for (int attempt = 1; attempt <= LOCATOR_DISTRIBUTION_RETRY_ATTEMPTS; attempt++) {

          for (Map.Entry<DistributionLocatorId, Set<LocatorJoinMessage>> entry : failedMessages
              .entrySet()) {
            DistributionLocatorId targetLocator = entry.getKey();
            Set<LocatorJoinMessage> joinMessages = entry.getValue();

            for (Iterator<LocatorJoinMessage> iterator = joinMessages.iterator(); iterator
                .hasNext();) {
              LocatorJoinMessage locatorJoinMessage = iterator.next();
              if (retryMessage(targetLocator, locatorJoinMessage, attempt)) {
                iterator.remove();
              } else {
                // Sleep between retries.
                try {
                  Thread.sleep(memberTimeout);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  logger.warn(
                      "Locator Membership listener permanently failed to exchange locator information due to interruption.");
                  return;
                }
              }
            }
          }
        }
      }
    }
  }
}
