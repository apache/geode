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


import org.apache.geode.distributed.internal.membership.api.Authenticator;
import org.apache.geode.distributed.internal.membership.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.api.Membership;
import org.apache.geode.distributed.internal.membership.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.distributed.internal.membership.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.api.MembershipLocator;
import org.apache.geode.distributed.internal.membership.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.api.MessageListener;
import org.apache.geode.distributed.internal.membership.gms.locator.MembershipLocatorImpl;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.services.module.ModuleService;

/**
 * MembershipBuilderImpl is the implementation of MembershipBuilder. It can construct
 * a GMSMembership.
 */
public class MembershipBuilderImpl<ID extends MemberIdentifier> implements MembershipBuilder<ID> {
  private final TcpSocketCreator socketCreator;
  private final TcpClient locatorClient;
  private MembershipListener<ID> membershipListener = new MembershipListenerNoOp();
  private MessageListener<ID> messageListener = message -> {
  };
  private MembershipStatistics statistics = new DefaultMembershipStatistics();
  private Authenticator<ID> authenticator = new AuthenticatorNoOp();
  private MembershipConfig membershipConfig = new MembershipConfig() {};
  private final DSFIDSerializer serializer;
  private final MemberIdentifierFactory<ID> memberFactory;
  private LifecycleListener<ID> lifecycleListener = new LifecycleListenerNoOp();

  private MembershipLocatorImpl<ID> membershipLocator;

  private ModuleService moduleService;

  public MembershipBuilderImpl(
      final TcpSocketCreator socketCreator,
      final TcpClient locatorClient,
      final DSFIDSerializer serializer,
      final MemberIdentifierFactory<ID> memberFactory,
      final ModuleService moduleService) {
    this.socketCreator = socketCreator;
    this.locatorClient = locatorClient;
    this.serializer = serializer;
    this.memberFactory = memberFactory;
    this.moduleService = moduleService;
  }

  @Override
  public MembershipBuilder<ID> setAuthenticator(Authenticator<ID> authenticator) {
    this.authenticator = authenticator;
    return this;
  }

  @Override
  public MembershipBuilder<ID> setStatistics(MembershipStatistics statistics) {
    this.statistics = statistics;
    return this;
  }

  @Override
  public MembershipBuilder<ID> setMembershipListener(MembershipListener<ID> membershipListener) {
    this.membershipListener = membershipListener;
    return this;
  }

  @Override
  public MembershipBuilder<ID> setMembershipLocator(
      final MembershipLocator<ID> membershipLocator) {
    this.membershipLocator = (MembershipLocatorImpl<ID>) membershipLocator;
    return this;
  }

  @Override
  public MembershipBuilder<ID> setMessageListener(MessageListener<ID> messageListener) {
    this.messageListener = messageListener;
    return this;
  }

  @Override
  public MembershipBuilder<ID> setConfig(MembershipConfig membershipConfig) {
    this.membershipConfig = membershipConfig;
    return this;
  }

  @Override
  public MembershipBuilder<ID> setLifecycleListener(
      LifecycleListener<ID> lifecycleListener) {
    this.lifecycleListener = lifecycleListener;
    return this;
  }

  @Override
  public Membership<ID> create() throws MembershipConfigurationException {
    GMSMembership<ID> gmsMembership =
        new GMSMembership<>(membershipListener, messageListener, lifecycleListener);
    final Services<ID> services =
        new Services<>(gmsMembership.getGMSManager(), statistics, authenticator,
            membershipConfig, serializer, memberFactory, locatorClient, socketCreator);
    if (membershipLocator != null) {
      services.setLocators(membershipLocator.getGMSLocator(), membershipLocator);
    }
    services.init(moduleService);
    return gmsMembership;
  }

}
