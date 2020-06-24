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


import org.apache.geode.distributed.internal.membership.gms.MembershipBuilderImpl;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.services.module.ModuleService;

/**
 * A builder for creating a new {@link Membership}. Use this builder to configure a
 * new Membership and to install various sorts of listeners Listeners receive lifecycle,
 * cluster and peer-to-peer message notification.
 */
public interface MembershipBuilder<ID extends MemberIdentifier> {
  MembershipBuilder<ID> setAuthenticator(Authenticator<ID> authenticator);

  MembershipBuilder<ID> setStatistics(MembershipStatistics statistics);

  MembershipBuilder<ID> setMembershipListener(MembershipListener<ID> membershipListener);

  MembershipBuilder<ID> setMembershipLocator(MembershipLocator<ID> membershipLocator);

  MembershipBuilder<ID> setMessageListener(MessageListener<ID> messageListener);

  MembershipBuilder<ID> setConfig(MembershipConfig membershipConfig);

  MembershipBuilder<ID> setLifecycleListener(LifecycleListener<ID> lifecycleListener);

  Membership<ID> create() throws MembershipConfigurationException;

  static <ID extends MemberIdentifier> MembershipBuilder<ID> newMembershipBuilder(
      final TcpSocketCreator socketCreator,
      final TcpClient locatorClient,
      final DSFIDSerializer serializer,
      final MemberIdentifierFactory<ID> memberFactory, ModuleService moduleService) {
    return new MembershipBuilderImpl<>(
        socketCreator, locatorClient, serializer, memberFactory, moduleService);
  }
}
