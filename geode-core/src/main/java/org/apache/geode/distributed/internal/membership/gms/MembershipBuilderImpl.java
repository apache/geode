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


import org.apache.geode.GemFireConfigException;
import org.apache.geode.SystemConnectException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionException;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.adapter.GMSMembershipManager;
import org.apache.geode.distributed.internal.membership.gms.api.Authenticator;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipBuilder;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipListener;
import org.apache.geode.distributed.internal.membership.gms.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.api.MessageListener;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.tcp.ConnectionException;
import org.apache.geode.security.GemFireSecurityException;

public class MembershipBuilderImpl implements MembershipBuilder {
  private MembershipListener membershipListener;
  private MessageListener messageListener;
  private MembershipStatistics statistics;
  private Authenticator authenticator;
  private ClusterDistributionManager dm;
  private MembershipConfig membershipConfig;
  private DSFIDSerializer serializer;
  private MemberIdentifierFactory memberFactory = new MemberIdentifierFactoryImpl();

  public MembershipBuilderImpl(ClusterDistributionManager dm) {
    this.dm = dm;
  }

  @Override
  public MembershipBuilder setAuthenticator(Authenticator authenticator) {
    this.authenticator = authenticator;
    return this;
  }

  @Override
  public MembershipBuilder setStatistics(MembershipStatistics statistics) {
    this.statistics = statistics;
    return this;
  }

  @Override
  public MembershipBuilder setMembershipListener(MembershipListener membershipListener) {
    this.membershipListener = membershipListener;
    return this;
  }

  @Override
  public MembershipBuilder setMessageListener(MessageListener messageListener) {
    this.messageListener = messageListener;
    return this;
  }

  @Override
  public MembershipBuilder setConfig(MembershipConfig membershipConfig) {
    this.membershipConfig = membershipConfig;
    return this;
  }

  @Override
  public MembershipBuilder setSerializer(DSFIDSerializer serializer) {
    this.serializer = serializer;
    return this;
  }

  @Override
  public MembershipBuilder setMemberIDFactory(MemberIdentifierFactory memberFactory) {
    this.memberFactory = memberFactory;
    return this;
  }

  @Override
  public MembershipManager create() {
    GMSMembershipManager gmsMembershipManager =
        new GMSMembershipManager(membershipListener, messageListener, dm);
    Services services =
        new Services(gmsMembershipManager.getGMSManager(), statistics, authenticator,
            membershipConfig, serializer, memberFactory);
    try {
      services.init();
      services.start();
    } catch (ConnectionException e) {
      throw new DistributionException(
          "Unable to create membership manager",
          e);
    } catch (GemFireConfigException | SystemConnectException | GemFireSecurityException e) {
      throw e;
    } catch (RuntimeException e) {
      Services.getLogger().error("Unexpected problem starting up membership services", e);
      throw new SystemConnectException("Problem starting up membership services", e);
    }
    return gmsMembershipManager;
  }



}
