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
package org.apache.geode.distributed.internal.membership.gms.locator;


import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorStatistics;
import org.apache.geode.distributed.internal.membership.gms.GMSMembership;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.ProtocolChecker;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

public class MembershipLocatorImpl<ID extends MemberIdentifier> implements MembershipLocator<ID> {
  private static final Logger logger = LogService.getLogger();

  private final TcpServer server;
  /**
   * the TcpHandler used for peer location
   */
  private final PrimaryHandler handler;
  private final GMSLocator<ID> gmsLocator;
  private final TcpClient locatorClient;

  public MembershipLocatorImpl(int port, InetAddress bindAddress,
      ProtocolChecker protocolChecker,
      Supplier<ExecutorService> executorServiceSupplier,
      TcpSocketCreator socketCreator,
      ObjectSerializer objectSerializer,
      ObjectDeserializer objectDeserializer,
      TcpHandler fallbackHandler,
      boolean locatorsAreCoordinators,
      MembershipLocatorStatistics locatorStats, Path workingDirectory,
      MembershipConfig config)
      throws MembershipConfigurationException, UnknownHostException {
    handler = new PrimaryHandler(fallbackHandler, config.getLocatorWaitTime());
    String host = bindAddress == null ? LocalHostUtil.getLocalHostName()
        : bindAddress.getHostName();
    String threadName = "Distribution Locator on " + host + ": " + port;

    this.server = new TcpServer(port, bindAddress, handler,
        threadName, protocolChecker,
        locatorStats::getStatTime,
        executorServiceSupplier,
        socketCreator,
        objectSerializer,
        objectDeserializer,
        GeodeGlossary.GEMFIRE_PREFIX + "TcpServer.READ_TIMEOUT",
        GeodeGlossary.GEMFIRE_PREFIX + "TcpServer.BACKLOG");

    locatorClient = new TcpClient(socketCreator,
        objectSerializer,
        objectDeserializer);
    gmsLocator =
        new GMSLocator<>(bindAddress, config.getLocators(), locatorsAreCoordinators,
            config.isNetworkPartitionDetectionEnabled(),
            locatorStats, config.getSecurityUDPDHAlgo(), workingDirectory, locatorClient,
            objectSerializer,
            objectDeserializer);

    handler.addHandler(PeerLocatorRequest.class, gmsLocator);
    handler.addHandler(FindCoordinatorRequest.class, gmsLocator);
    handler.addHandler(GetViewRequest.class, gmsLocator);
  }

  @Override
  public int start() throws IOException {
    if (!isAlive()) {
      server.start();
    }
    return getPort();
  }

  @Override
  public boolean isAlive() {
    return server.isAlive();
  }

  @Override
  public int getPort() {
    return server.getPort();
  }

  @Override
  public boolean isShuttingDown() {
    return server.isShuttingDown();
  }

  @Override
  public void waitToShutdown(long waitTime) throws InterruptedException {
    server.join(waitTime);
  }

  @Override
  public void waitToShutdown() throws InterruptedException {
    server.join();
  }

  @Override
  public void restarting() throws IOException {
    server.restarting();
  }

  @Override
  public SocketAddress getSocketAddress() {
    return server.getSocketAddress();
  }

  @Override
  public void setMembership(final Membership<ID> membership) {
    final GMSMembership<ID> gmsMembership = (GMSMembership<ID>) membership;
    setServices(gmsMembership.getServices());
  }

  @Override
  public void addHandler(Class<?> clazz, TcpHandler handler) {
    this.handler.addHandler(clazz, handler);
  }

  @Override
  public boolean isHandled(Class<?> clazz) {
    return this.handler.isHandled(clazz);
  }

  @VisibleForTesting
  public GMSLocator<ID> getGMSLocator() {
    return this.gmsLocator;
  }

  /**
   * Services is a class internal to the membership module. As such, the ability to setServices
   * is available ony within the module. It's not part of the external API.
   */
  public void setServices(final Services<ID> services) {
    gmsLocator.setServices(services);
  }

  public void stop() {
    if (isAlive()) {
      logger.info("Stopping {}", this);
      try {
        locatorClient
            .stop(
                new HostAndPort(((InetSocketAddress) getSocketAddress()).getHostString(),
                    getPort()));
      } catch (ConnectException ignore) {
        // must not be running
      }

      boolean interrupted = Thread.interrupted();
      long waitTimeMillis = TcpServer.SHUTDOWN_WAIT_TIME * 2;
      try {
        // TcpServer up to SHUTDOWN_WAIT_TIME for its executor pool to shut down.
        // We wait 2 * SHUTDOWN_WAIT_TIME here to account for that shutdown, and then our own.
        waitToShutdown(waitTimeMillis);

      } catch (InterruptedException ex) {
        interrupted = true;
        logger.warn("Interrupted while stopping {}", this, ex);

        // Continue running -- doing our best to stop everything...
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }

      if (isAlive()) {
        logger.fatal("Could not stop {} in {} seconds", this,
            TimeUnit.MILLISECONDS.toSeconds(waitTimeMillis));
      }
    }
  }

  @Override
  public String toString() {
    return "Locator on " + getSocketAddress() + ":" + getPort();
  }
}
