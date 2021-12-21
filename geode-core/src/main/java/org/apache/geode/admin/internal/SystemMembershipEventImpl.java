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
package org.apache.geode.admin.internal;

import org.apache.geode.admin.SystemMembershipEvent;
import org.apache.geode.admin.SystemMembershipListener;
import org.apache.geode.distributed.DistributedMember;

/**
 * An event delivered to a {@link SystemMembershipListener} when a member has joined or left the
 * distributed system.
 *
 * @since GemFire 5.0
 */
public class SystemMembershipEventImpl implements SystemMembershipEvent {

  /** The id of the member that generated this event */
  private final DistributedMember id;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>SystemMembershipEvent</code> for the member with the given id.
   */
  protected SystemMembershipEventImpl(DistributedMember id) {
    this.id = id;
  }

  ///////////////////// Instance Methods /////////////////////

  @Override
  public String getMemberId() {
    return id.toString();
  }

  @Override
  public DistributedMember getDistributedMember() {
    return id;
  }

  @Override
  public String toString() {
    return "Member " + getMemberId();
  }

}
