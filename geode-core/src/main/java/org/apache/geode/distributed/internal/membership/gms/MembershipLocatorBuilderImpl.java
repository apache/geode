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
package org.apache.geode.distributed.internal.membership.gms;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorBuilder;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorStatistics;
import org.apache.geode.distributed.internal.membership.gms.locator.MembershipLocatorImpl;
import org.apache.geode.distributed.internal.tcpserver.ProtocolChecker;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.internal.serialization.ObjectSerializer;

public final class MembershipLocatorBuilderImpl<ID extends MemberIdentifier> implements
    MembershipLocatorBuilder<ID> {
  private int port = 0;
  private InetAddress bindAddress = null;
  private ProtocolChecker protocolChecker = (socket, input, firstByte) -> false;
  private TcpHandler fallbackHandler = new TcpHandlerNoOp();
  private MembershipLocatorStatistics locatorStats = new MembershipLocatorStatisticsNoOp();
  private boolean locatorsAreCoordinators = true;
  private TcpSocketCreator socketCreator;
  private ObjectSerializer objectSerializer;
  private ObjectDeserializer objectDeserializer;
  private Path workingDirectory;
  private MembershipConfig config;
  private Supplier<ExecutorService> executorServiceSupplier;

  @Override
  public MembershipLocatorBuilder<ID> setPort(int port) {
    this.port = port;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setBindAddress(InetAddress bindAddress) {
    this.bindAddress = bindAddress;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setProtocolChecker(ProtocolChecker protocolChecker) {
    this.protocolChecker = protocolChecker;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setExecutorServiceSupplier(
      Supplier<ExecutorService> executorServiceSupplier) {
    this.executorServiceSupplier = executorServiceSupplier;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setSocketCreator(TcpSocketCreator socketCreator) {
    this.socketCreator = socketCreator;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setObjectSerializer(ObjectSerializer objectSerializer) {
    this.objectSerializer = objectSerializer;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setObjectDeserializer(ObjectDeserializer objectDeserializer) {
    this.objectDeserializer = objectDeserializer;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setFallbackHandler(TcpHandler fallbackHandler) {
    this.fallbackHandler = fallbackHandler;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setLocatorsAreCoordinators(boolean locatorsAreCoordinators) {
    this.locatorsAreCoordinators = locatorsAreCoordinators;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setLocatorStats(MembershipLocatorStatistics locatorStats) {
    this.locatorStats = locatorStats;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setWorkingDirectory(Path workingDirectory) {
    this.workingDirectory = workingDirectory;
    return this;
  }

  @Override
  public MembershipLocatorBuilder<ID> setConfig(MembershipConfig config) {
    this.config = config;
    return this;
  }

  @Override
  public MembershipLocator<ID> create()
      throws UnknownHostException, MembershipConfigurationException {
    return new MembershipLocatorImpl<ID>(port, bindAddress, protocolChecker,
        executorServiceSupplier,
        socketCreator, objectSerializer, objectDeserializer, fallbackHandler,
        locatorsAreCoordinators, locatorStats, workingDirectory, config);
  }

}
