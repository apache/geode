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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.cache.GatewayConfigurationException;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.cache.client.internal.ServerBlackList.FailureTracker;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.CacheClientUpdater;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.security.GemFireSecurityException;

/**
 * Creates connections, using a connection source to determine which server to connect to.
 *
 * @since GemFire 5.7
 *
 */
public class ConnectionFactoryImpl implements ConnectionFactory {

  private static final Logger logger = LogService.getLogger();

  // TODO - GEODE-1746, the handshake holds state. It seems like the code depends
  // on all of the handshake operations happening in a single thread. I don't think we
  // want that, need to refactor.
  private final ServerBlackList blackList;
  private ConnectionSource source;
  private PoolImpl pool;
  private final CancelCriterion cancelCriterion;
  private final ConnectionConnector connectionConnector;


  /**
   * Test hook for client version support
   *
   * @since GemFire 5.7
   */

  public static boolean testFailedConnectionToServer = false;

  public ConnectionFactoryImpl(ConnectionSource source, EndpointManager endpointManager,
      InternalDistributedSystem sys, int socketBufferSize, int handshakeTimeout, int readTimeout,
      ClientProxyMembershipID proxyId, CancelCriterion cancelCriterion, boolean usedByGateway,
      GatewaySender sender, long pingInterval, boolean multiuserSecureMode, PoolImpl pool) {
    this(
        new ConnectionConnector(endpointManager, sys, socketBufferSize, handshakeTimeout,
            readTimeout, cancelCriterion, usedByGateway, sender,
            (usedByGateway || sender != null) ? SocketCreatorFactory
                .getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY)
                : SocketCreatorFactory
                    .getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER),
            new ClientSideHandshakeImpl(proxyId, sys, sys.getSecurityService(),
                multiuserSecureMode)),
        source, pingInterval, pool, cancelCriterion);
  }

  public ConnectionFactoryImpl(ConnectionConnector connectionConnector, ConnectionSource source,
      long pingInterval, PoolImpl pool, CancelCriterion cancelCriterion) {
    this.source = source;
    this.blackList = new ServerBlackList(pingInterval);
    this.pool = pool;
    this.cancelCriterion = cancelCriterion;
    this.connectionConnector = connectionConnector;
  }

  public void start(ScheduledExecutorService background) {
    blackList.start(background);
  }

  public ServerBlackList getBlackList() {
    return blackList;
  }

  public Connection createClientToServerConnection(ServerLocation location, boolean forQueue)
      throws GemFireSecurityException {
    FailureTracker failureTracker = blackList.getFailureTracker(location);

    boolean initialized = false;
    Connection connection = null;
    try {
      connection = connectionConnector.connectClientToServer(location, forQueue);
      initialized = true;
      failureTracker.reset();
      authenticateIfRequired(connection);
    } catch (GemFireConfigException | CancelException | GemFireSecurityException
        | GatewayConfigurationException e) {
      throw e;
    } catch (ServerRefusedConnectionException src) {
      // propagate this up, don't retry
      logger.warn(LocalizedMessage.create(
          LocalizedStrings.AutoConnectionSourceImpl_COULD_NOT_CREATE_A_NEW_CONNECTION_TO_SERVER_0,
          src.getMessage()));
      testFailedConnectionToServer = true;
      throw src;
    } catch (Exception e) {
      if (e.getMessage() != null && (e.getMessage().equals("Connection refused")
          || e.getMessage().equals("Connection reset"))) { // this is the most common case, so don't
                                                           // print an exception
        if (logger.isDebugEnabled()) {
          logger.debug("Unable to connect to {}: connection refused", location);
        }
      } else {// print a warning with the exception stack trace
        logger.warn(LocalizedMessage
            .create(LocalizedStrings.ConnectException_COULD_NOT_CONNECT_TO_0, location), e);
      }
      testFailedConnectionToServer = true;
    } finally {
      if (!initialized && connection != null) {
        connection.destroy();
        failureTracker.addFailure();
        connection = null;
      }
    }

    return connection;
  }

  private void authenticateIfRequired(Connection conn) {
    cancelCriterion.checkCancelInProgress(null);
    if (!pool.isUsedByGateway() && !pool.getMultiuserAuthentication()) {
      ServerLocation server = conn.getServer();
      if (server.getRequiresCredentials()) {
        if (server.getUserId() == -1) {
          Long uniqueID = (Long) AuthenticateUserOp.executeOn(conn, pool);
          server.setUserId(uniqueID);
          if (logger.isDebugEnabled()) {
            logger.debug("CFI.authenticateIfRequired() Completed authentication on {}", conn);
          }
        }
      }
    }
  }

  public ServerLocation findBestServer(ServerLocation currentServer, Set excludedServers) {
    if (currentServer != null && source.isBalanced()) {
      return currentServer;
    }
    final Set origExcludedServers = excludedServers;
    excludedServers = new HashSet(excludedServers);
    Set blackListedServers = blackList.getBadServers();
    excludedServers.addAll(blackListedServers);
    ServerLocation server = source.findReplacementServer(currentServer, excludedServers);
    if (server == null) {
      // Nothing worked! Let's try without the blacklist.
      if (excludedServers.size() > origExcludedServers.size()) {
        // We had some guys black listed so lets give this another whirl.
        server = source.findReplacementServer(currentServer, origExcludedServers);
      }
    }
    if (server == null && logger.isDebugEnabled()) {
      logger.debug("Source was unable to findForReplacement any servers");
    }
    return server;
  }

  public Connection createClientToServerConnection(Set excludedServers)
      throws GemFireSecurityException {
    final Set origExcludedServers = excludedServers;
    excludedServers = new HashSet(excludedServers);
    Set blackListedServers = blackList.getBadServers();
    excludedServers.addAll(blackListedServers);
    Connection conn = null;
    RuntimeException fatalException = null;
    boolean tryBlackList = true;

    do {
      ServerLocation server = source.findServer(excludedServers);
      if (server == null) {

        if (tryBlackList) {
          // Nothing worked! Let's try without the blacklist.
          tryBlackList = false;
          int size = excludedServers.size();
          excludedServers.removeAll(blackListedServers);
          // make sure we didn't remove any of the ones that the caller set not to use
          excludedServers.addAll(origExcludedServers);
          if (excludedServers.size() < size) {
            // We are able to remove some exclusions, so lets give this another whirl.
            continue;
          }
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Source was unable to locate any servers");
        }
        if (fatalException != null) {
          throw fatalException;
        }
        return null;
      }

      try {
        conn = createClientToServerConnection(server, false);
      } catch (CancelException e) {
        // propagate this up immediately
        throw e;
      } catch (GemFireSecurityException e) {
        // propagate this up immediately
        throw e;
      } catch (GatewayConfigurationException e) {
        // propagate this up immediately
        throw e;
      } catch (ServerRefusedConnectionException srce) {
        fatalException = srce;
        if (logger.isDebugEnabled()) {
          logger.debug("ServerRefusedConnectionException attempting to connect to {}", server,
              srce);
        }
      } catch (Exception e) {
        logger.warn(LocalizedMessage
            .create(LocalizedStrings.ConnectException_COULD_NOT_CONNECT_TO_0, server), e);
      }

      excludedServers.add(server);
    } while (conn == null);

    return conn;
  }

  public ClientUpdater createServerToClientConnection(Endpoint endpoint, QueueManager qManager,
      boolean isPrimary, ClientUpdater failedUpdater) {
    String clientUpdateName = CacheClientUpdater.CLIENT_UPDATER_THREAD_NAME + " on "
        + endpoint.getMemberId() + " port " + endpoint.getLocation().getPort();
    if (logger.isDebugEnabled()) {
      logger.debug("Establishing: {}", clientUpdateName);
    }
    // Launch the thread
    CacheClientUpdater updater = connectionConnector.connectServerToClient(endpoint, qManager,
        isPrimary, failedUpdater, clientUpdateName);

    return updater;
  }
}
