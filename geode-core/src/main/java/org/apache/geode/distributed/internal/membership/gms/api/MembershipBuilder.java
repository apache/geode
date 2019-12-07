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
package org.apache.geode.distributed.internal.membership.gms.api;


import org.apache.geode.distributed.internal.membership.gms.MembershipBuilderImpl;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.serialization.DSFIDSerializer;

/**
 * Builder for creating a new {@link Membership}.
 *
 */
public interface MembershipBuilder<ID extends MemberIdentifier> {
  MembershipBuilder<ID> setAuthenticator(Authenticator<ID> authenticator);

  MembershipBuilder<ID> setStatistics(MembershipStatistics statistics);

  MembershipBuilder<ID> setMembershipListener(MembershipListener<ID> membershipListener);

  MembershipBuilder<ID> setMessageListener(MessageListener<ID> messageListener);

  MembershipBuilder<ID> setConfig(MembershipConfig membershipConfig);

  MembershipBuilder<ID> setSerializer(DSFIDSerializer serializer);

  MembershipBuilder<ID> setMemberIDFactory(MemberIdentifierFactory<ID> memberFactory);

  MembershipBuilder<ID> setLifecycleListener(LifecycleListener<ID> lifecycleListener);

  MembershipBuilder<ID> setLocatorClient(final TcpClient tcpClient);


  Membership<ID> create();

  static <ID extends MemberIdentifier> MembershipBuilder<ID> newMembershipBuilder() {
    return new MembershipBuilderImpl<>();
  }
}
