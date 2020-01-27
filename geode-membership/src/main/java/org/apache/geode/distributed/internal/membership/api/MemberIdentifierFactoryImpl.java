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

import java.util.Comparator;

import org.apache.geode.distributed.internal.membership.gms.MemberIdentifierImpl;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.serialization.DSFIDSerializer;

/**
 * The default {@link MemberIdentifierFactory} which produces {@link MemberIdentifier} objects. For
 * use in
 * {@link MembershipBuilder#newMembershipBuilder(TcpSocketCreator, TcpClient, DSFIDSerializer, MemberIdentifierFactory)}
 */
public class MemberIdentifierFactoryImpl
    implements MemberIdentifierFactory<MemberIdentifier> {

  @Override
  public MemberIdentifier create(MemberData memberInfo) {
    return new MemberIdentifierImpl(memberInfo);
  }

  @Override
  public Comparator<MemberIdentifier> getComparator() {
    return (identifier, other) -> identifier.compareTo(other, false, true);
  }
}
