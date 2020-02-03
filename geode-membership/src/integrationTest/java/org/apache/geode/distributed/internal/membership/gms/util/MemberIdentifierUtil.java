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
package org.apache.geode.distributed.internal.membership.gms.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.jgroups.util.UUID;

import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.distributed.internal.membership.gms.GMSMemberData;
import org.apache.geode.internal.serialization.Version;

public class MemberIdentifierUtil {
  public static MemberIdentifier createMemberID(short version, int viewId, long msb, long lsb)
      throws UnknownHostException {
    MemberData memberData = MemberDataBuilder.newBuilderForLocalHost("localhost")
        .setVersionOrdinal(version)
        .setVmViewId(viewId)
        .setUuidMostSignificantBits(msb)
        .setUuidLeastSignificantBits(lsb)
        .build();
    MemberIdentifier gmsMember = new MemberIdentifierFactoryImpl().create(memberData);
    return gmsMember;
  }

  /**
   * Create a new membership ID that is preferred for coordinator and has a UUID established
   */
  public static MemberIdentifier createMemberID(int port) {
    UUID uuid = UUID.randomUUID();
    try {
      return new MemberIdentifierFactoryImpl().create(new GMSMemberData(InetAddress.getLocalHost(),
          port, Version.CURRENT_ORDINAL, uuid.getMostSignificantBits(),
          uuid.getLeastSignificantBits(), -1));
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a new membership ID that is preferred for coordinator and has no UUID
   */
  public static MemberIdentifier createMemberIDWithNoUUID(int port) {
    try {
      return new MemberIdentifierFactoryImpl().create(new GMSMemberData(InetAddress.getLocalHost(),
          port, Version.CURRENT_ORDINAL, 0l, 0l, -1));
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

}
