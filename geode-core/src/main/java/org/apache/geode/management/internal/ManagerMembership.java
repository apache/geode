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
package org.apache.geode.management.internal;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * Membership operations for Geode JMX managers.
 */
public interface ManagerMembership {

  /**
   * This method will be invoked from MembershipListener which is registered when the member becomes
   * a Management node.
   */
  void addMember(InternalDistributedMember member);

  /**
   * This method will be invoked from MembershipListener which is registered when the member becomes
   * a Management node.
   */
  void removeMember(DistributedMember member, boolean crashed);

  /**
   * This method will be invoked from MembershipListener which is registered when the member becomes
   * a Management node.
   */
  void suspectMember(DistributedMember member, InternalDistributedMember whoSuspected,
      String reason);
}
