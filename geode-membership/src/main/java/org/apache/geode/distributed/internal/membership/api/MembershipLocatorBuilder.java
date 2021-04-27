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
package org.apache.geode.distributed.internal.membership.api;

import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.apache.geode.distributed.internal.membership.gms.MembershipLocatorBuilderImpl;
import org.apache.geode.distributed.internal.tcpserver.HostAddress;
import org.apache.geode.distributed.internal.tcpserver.ProtocolChecker;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.serialization.DSFIDSerializer;

public interface MembershipLocatorBuilder<ID extends MemberIdentifier> {
  MembershipLocatorBuilder<ID> setPort(int port);

  MembershipLocatorBuilder<ID> setBindAddress(HostAddress bindAddress);

  MembershipLocatorBuilder<ID> setConfig(MembershipConfig membershipConfig);

  MembershipLocatorBuilder<ID> setProtocolChecker(ProtocolChecker protocolChecker);

  MembershipLocatorBuilder<ID> setFallbackHandler(TcpHandler fallbackHandler);

  MembershipLocatorBuilder<ID> setLocatorsAreCoordinators(boolean locatorsAreCoordinators);

  MembershipLocatorBuilder<ID> setLocatorStats(MembershipLocatorStatistics locatorStats);

  MembershipLocator<ID> create()
      throws UnknownHostException, MembershipConfigurationException;

  static <ID extends MemberIdentifier> MembershipLocatorBuilder<ID> newLocatorBuilder(
      final TcpSocketCreator socketCreator,
      final DSFIDSerializer serializer,
      final Path workingDirectory,
      final Supplier<ExecutorService> executorServiceSupplier) {
    return new MembershipLocatorBuilderImpl<ID>(socketCreator, serializer,
        workingDirectory, executorServiceSupplier);
  }
}
