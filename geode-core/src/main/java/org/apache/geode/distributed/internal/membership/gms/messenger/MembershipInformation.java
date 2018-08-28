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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import java.util.Set;

import org.jgroups.JChannel;

import org.apache.geode.distributed.DistributedMember;

/**
 * Class MembershipInformation is used to pass membership data from a GMS that was
 * kicked out of the cluster to a new one during auto-reconnect operations.
 */
public class MembershipInformation {
  private final JChannel channel;
  private final Set<DistributedMember> membershipIdentifiers;

  protected MembershipInformation(JChannel channel,
      Set<DistributedMember> oldMembershipIdentifiers) {

    this.channel = channel;
    this.membershipIdentifiers = oldMembershipIdentifiers;
  }

  public JChannel getChannel() {
    return channel;
  }

  public Set<DistributedMember> getMembershipIdentifiers() {
    return membershipIdentifiers;
  }
}
