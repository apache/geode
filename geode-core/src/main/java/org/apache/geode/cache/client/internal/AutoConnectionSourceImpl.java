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
package org.apache.geode.cache.client.internal;


import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.ToDataException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.client.NoAvailableLocatorsException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.cache.client.internal.PoolImpl.PoolTask;
import org.apache.geode.cache.client.internal.locator.ClientConnectionRequest;
import org.apache.geode.cache.client.internal.locator.ClientConnectionResponse;
import org.apache.geode.cache.client.internal.locator.ClientReplacementRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersResponse;
import org.apache.geode.cache.client.internal.locator.LocatorListRequest;
import org.apache.geode.cache.client.internal.locator.LocatorListResponse;
import org.apache.geode.cache.client.internal.locator.QueueConnectionRequest;
import org.apache.geode.cache.client.internal.locator.QueueConnectionResponse;
import org.apache.geode.cache.client.internal.locator.ServerLocationRequest;
import org.apache.geode.cache.client.internal.locator.ServerLocationResponse;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * A connection source which uses locators to find the least loaded server.
 *
 * @since GemFire 5.7
 *
 */
public class AutoConnectionSourceImpl implements ConnectionSource {

  private static final Logger logger = LogService.getLogger();

  private final TcpClient tcpClient;

  @Immutable
  private static final LocatorListRequest LOCATOR_LIST_REQUEST = new LocatorListRequest();

  private final List<HostAndPort> initialLocators;

  private final String serverGroup;
  private final AtomicReference<LocatorList> locators = new AtomicReference<>();
  private final AtomicReference<LocatorList> onlineLocators = new AtomicReference<>();
  protected InternalPool pool;
  private final int connectionTimeout;
  private long locatorUpdateInterval;
  private volatile LocatorDiscoveryCallback locatorCallback = new LocatorDiscoveryCallbackAdapter();
  private volatile boolean isBalanced = true;
  /**
   * key is the InetSocketAddress of the locator. value will be an exception if we have already
   * found the locator to be dead. value will be null if we last saw it alive.
   */
  private final Map<InetSocketAddress, Exception> locatorState = new HashMap<>();

  public AutoConnectionSourceImpl(final @NotNull List<HostAndPort> contacts,
      final @NotNull String serverGroup,
      final int handshakeTimeout, final @NotNull SocketFactory socketFactory) {
    this(contacts, serverGroup, handshakeTimeout, new TcpClient(SocketCreatorFactory
        .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        socketFactory::createSocket));
  }

  AutoConnectionSourceImpl(final @NotNull List<HostAndPort> contacts,
      final @NotNull String serverGroup,
      final int handshakeTimeout, final @NotNull TcpClient tcpClient) {
    locators.set(new LocatorList(new ArrayList<>(contacts)));
    onlineLocators.set(new LocatorList(Collections.emptyList()));
    initialLocators = Collections.unmodifiableList(locators.get().getLocatorAddresses());
    connectionTimeout = handshakeTimeout;
    this.serverGroup = serverGroup;
    this.tcpClient = tcpClient;
  }

  @Override
  public boolean isBalanced() {
    return isBalanced;
  }

  @Override
  public List<ServerLocation> getAllServers() {
    if (PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return null;
    }
    GetAllServersRequest request = new GetAllServersRequest(serverGroup);
    GetAllServersResponse response = (GetAllServersResponse) queryLocators(request);
    if (response != null) {
      return response.getServers();
    } else {
      return null;
    }
  }

  @Override
  public ServerLocation findReplacementServer(ServerLocation currentServer,
      Set<ServerLocation> excludedServers) {
    if (PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return null;
    }
    ClientReplacementRequest request =
        new ClientReplacementRequest(currentServer, excludedServers, serverGroup);
    ClientConnectionResponse response = (ClientConnectionResponse) queryLocators(request);
    if (response == null) {
      throw new NoAvailableLocatorsException(
          "Unable to connect to any locators in the list " + locators);
    }
    if (!response.hasResult()) {
      throw new NoAvailableServersException("No servers found");
    }
    return response.getServer();
  }

  @Override
  public ServerLocation findServer(Set<ServerLocation> excludedServers) {
    if (PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return null;
    }
    ClientConnectionRequest request = new ClientConnectionRequest(excludedServers, serverGroup);
    ClientConnectionResponse response = (ClientConnectionResponse) queryLocators(request);
    if (response == null) {
      throw new NoAvailableLocatorsException(
          "Unable to connect to any locators in the list " + locators);
    }
    if (!response.hasResult()) {
      throw new NoAvailableServersException("No servers found");
    }
    return response.getServer();
  }

  @Override
  public List<ServerLocation> findServersForQueue(Set<ServerLocation> excludedServers,
      int numServers, ClientProxyMembershipID proxyId, boolean findDurableQueue) {
    if (PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return new ArrayList<>();
    }
    QueueConnectionRequest request = new QueueConnectionRequest(proxyId, numServers,
        excludedServers, serverGroup, findDurableQueue);
    QueueConnectionResponse response = (QueueConnectionResponse) queryLocators(request);
    if (response == null) {
      throw new NoAvailableLocatorsException(
          "Unable to connect to any locators in the list " + locators);
    }
    if (!response.hasResult()) {
      throw new NoAvailableServersException("No servers found");
    }
    return response.getServers();
  }

  @Override
  public List<InetSocketAddress> getOnlineLocators() {
    if (PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(new ArrayList<>(onlineLocators.get().getLocators()));
  }


  private ServerLocationResponse queryOneLocator(HostAndPort locator,
      ServerLocationRequest request) {
    return queryOneLocatorUsingConnection(locator, request, tcpClient);
  }


  ServerLocationResponse queryOneLocatorUsingConnection(HostAndPort locator,
      ServerLocationRequest request,
      TcpClient locatorConnection) {
    Object returnObj = null;
    try {
      pool.getStats().incLocatorRequests();
      returnObj = locatorConnection.requestToServer(locator, request, connectionTimeout, true);
      ServerLocationResponse response = (ServerLocationResponse) returnObj;
      pool.getStats().incLocatorResponses();
      if (response != null) {
        reportLiveLocator(locator.getSocketInetAddress());
      }
      return response;
    } catch (IOException | ToDataException ioe) {
      if (ioe instanceof ToDataException) {
        logger.warn("Encountered ToDataException when communicating with a locator.  "
            + "This is expected if the locator is shutting down.", ioe);
      }
      reportDeadLocator(locator.getSocketInetAddress(), ioe);
      return null;
    } catch (ClassNotFoundException e) {
      logger.warn("Received exception from locator {}", locator, e);
      return null;
    } catch (ClassCastException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Received odd response object from the locator: {}", returnObj);
      }
      reportDeadLocator(locator.getSocketInetAddress(), e);
      return null;
    }
  }

  ServerLocationResponse queryLocators(ServerLocationRequest request) {
    ServerLocationResponse response = null;
    final boolean isDebugEnabled = logger.isDebugEnabled();

    for (final HostAndPort locator : locators.get()) {
      if (isDebugEnabled) {
        logger.debug("Sending query to locator {}: {}", locator, request);
      }
      ServerLocationResponse tempResponse = queryOneLocator(locator, request);
      if (isDebugEnabled) {
        logger.debug("Received query response from locator {}: {}", locator, tempResponse);
      }
      if (tempResponse != null) {
        response = tempResponse;
        if (response.hasResult()) {
          break;
        }
      }
    }

    return response;
  }

  private void updateLocatorList(LocatorListResponse response) {
    if (response == null) {
      return;
    }
    isBalanced = response.isBalanced();
    List<ServerLocation> locatorResponse = response.getLocators();

    List<HostAndPort> newLocatorAddresses = new ArrayList<>(locatorResponse.size());
    List<HostAndPort> newOnlineLocators = new ArrayList<>(locatorResponse.size());

    Set<HostAndPort> badLocators = new HashSet<>(initialLocators);

    for (ServerLocation locator : locatorResponse) {
      HostAndPort hostAddress = new HostAndPort(locator.getHostName(), locator.getPort());
      newLocatorAddresses.add(hostAddress);
      newOnlineLocators.add(hostAddress);
      badLocators.remove(hostAddress);
    }

    addBadLocators(newLocatorAddresses, badLocators);

    LocatorList newLocatorList = new LocatorList(newLocatorAddresses);

    LocatorList oldLocators = locators.getAndSet(newLocatorList);
    onlineLocators.set(new LocatorList(newOnlineLocators));
    pool.getStats().setLocatorCount(newLocatorAddresses.size());

    if (logger.isInfoEnabled()
        || !locatorCallback.getClass().equals(LocatorDiscoveryCallbackAdapter.class)) {
      List<InetSocketAddress> newLocators = newLocatorList.getLocators();
      ArrayList<InetSocketAddress> removedLocators = new ArrayList<>(oldLocators.getLocators());
      removedLocators.removeAll(newLocators);

      ArrayList<InetSocketAddress> addedLocators = new ArrayList<>(newLocators);
      addedLocators.removeAll(oldLocators.getLocators());
      if (!addedLocators.isEmpty()) {
        locatorCallback.locatorsDiscovered(Collections.unmodifiableList(addedLocators));
        logger.info("AutoConnectionSource discovered new locators {}", addedLocators);
      }
      if (!removedLocators.isEmpty()) {
        locatorCallback.locatorsRemoved(Collections.unmodifiableList(removedLocators));
        logger.info("AutoConnectionSource dropping previously discovered locators {}",
            removedLocators);
      }
    }

  }

  /**
   * This method will add bad locator only when locator with hostname and port is not already in
   * list.
   */
  protected void addBadLocators(List<HostAndPort> newLocators, Set<HostAndPort> badLocators) {
    for (HostAndPort badLocator : badLocators) {
      boolean addIt = true;
      for (HostAndPort goodLocator : newLocators) {
        boolean isSameHost = badLocator.getHostName().equals(goodLocator.getHostName());
        if (isSameHost && badLocator.getPort() == goodLocator.getPort()) {
          // ip has been changed so don't add this in current
          // list
          addIt = false;
          break;

        }
      }
      if (addIt) {
        newLocators.add(badLocator);
      }
    }
  }

  @Override
  public void start(InternalPool pool) {
    this.pool = pool;
    pool.getStats().setInitialContacts((locators.get()).size());
    locatorUpdateInterval = Long.getLong(
        GeodeGlossary.GEMFIRE_PREFIX + "LOCATOR_UPDATE_INTERVAL", pool.getPingInterval());

    if (locatorUpdateInterval > 0) {
      pool.getBackgroundProcessor().scheduleWithFixedDelay(new UpdateLocatorListTask(), 0,
          locatorUpdateInterval, TimeUnit.MILLISECONDS);
      logger.info("AutoConnectionSource UpdateLocatorListTask started with interval={} ms.",
          locatorUpdateInterval);
    }
  }

  @Override
  public void stop() {

  }

  public void setLocatorDiscoveryCallback(LocatorDiscoveryCallback callback) {
    locatorCallback = callback;
  }

  private synchronized void reportLiveLocator(InetSocketAddress l) {
    Object prevState = locatorState.put(l, null);
    if (prevState != null) {
      logger.info("Communication has been restored with locator {}.", l);
    }
  }

  private synchronized void reportDeadLocator(InetSocketAddress l, Exception ex) {
    Object prevState = locatorState.put(l, ex);
    if (prevState == null) {
      if (ex instanceof ConnectException) {
        logger.info("locator {} is not running.", l, ex);
      } else {
        logger.info("Communication with locator {} failed", l, ex);
      }
    }
  }

  long getLocatorUpdateInterval() {
    return locatorUpdateInterval;
  }

  protected class UpdateLocatorListTask extends PoolTask {
    @Override
    public void run2() {
      if (pool.getCancelCriterion().isCancelInProgress()) {
        return;
      }
      LocatorListResponse response = (LocatorListResponse) queryLocators(LOCATOR_LIST_REQUEST);
      updateLocatorList(response);
    }
  }
}
