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


import org.apache.geode.distributed.internal.membership.gms.api.Authenticator;
import org.apache.geode.distributed.internal.membership.gms.api.LifecycleListener;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.gms.api.Membership;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.api.MessageListener;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.serialization.DSFIDSerializer;

public class MembershipBuilderImpl<ID extends MemberIdentifier> implements MembershipBuilder<ID> {
  private TcpClient locatorClient;
  private MembershipListener<ID> membershipListener;
  private MessageListener<ID> messageListener;
  private MembershipStatistics statistics;
  private Authenticator<ID> authenticator;
  private MembershipConfig membershipConfig;
  private DSFIDSerializer serializer;
  private MemberIdentifierFactory<ID> memberFactory;
  private LifecycleListener<ID> lifecycleListener;

  public MembershipBuilderImpl() {}

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
  public MembershipBuilder<ID> setSerializer(DSFIDSerializer serializer) {
    this.serializer = serializer;
    return this;
  }

  @Override
  public MembershipBuilder<ID> setMemberIDFactory(MemberIdentifierFactory<ID> memberFactory) {
    this.memberFactory = memberFactory;
    return this;
  }

  @Override
  public MembershipBuilder<ID> setLocatorClient(final TcpClient locatorClient) {
    this.locatorClient = locatorClient;
    return this;
  }

  @Override
  public MembershipBuilder<ID> setLifecycleListener(
      LifecycleListener<ID> lifecycleListener) {
    this.lifecycleListener = lifecycleListener;
    return this;
  }

  @Override
  public Membership<ID> create() {
    GMSMembership<ID> gmsMembership =
        new GMSMembership<>(membershipListener, messageListener, lifecycleListener);
    Services<ID> services =
        new Services<>(gmsMembership.getGMSManager(), statistics, authenticator,
            membershipConfig, serializer, memberFactory, locatorClient);
    services.init();
    return gmsMembership;
  }

}
