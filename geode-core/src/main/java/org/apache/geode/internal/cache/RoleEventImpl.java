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
package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RoleEvent;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.membership.InternalRole;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Implementation of a RoleEvent. Super class is DataSerializable but this class is probably never
 * on the wire, however, it does support it.
 *
 * @since GemFire 5.0
 */
public class RoleEventImpl extends RegionEventImpl implements RoleEvent {

  private static final long serialVersionUID = 1306615015229258945L;

  private Set requiredRoles;

  /**
   * Zero-argument constructor required by DataSerializable.
   */
  public RoleEventImpl() {}

  /**
   * Constructs new RoleEventImpl.
   *
   * @param requiredRoles set of required roles that are affected by this event
   */
  RoleEventImpl(Region region, Operation op, Object callbackArgument, boolean originRemote,
      DistributedMember distributedMember, Set requiredRoles) {
    super(region, op, callbackArgument, originRemote, distributedMember);
    this.requiredRoles = Collections.unmodifiableSet(requiredRoles);
  }

  @Override
  public Set getRequiredRoles() {
    return requiredRoles; // already unmodifiableSet
  }

  @Override
  public int getDSFID() {
    return ROLE_EVENT;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    String[] requiredRoleNames = new String[requiredRoles.size()];
    Iterator iter = requiredRoles.iterator();
    for (int i = 0; i < requiredRoleNames.length; i++) {
      Role role = (Role) iter.next();
      requiredRoleNames[i] = role.getName();
    }
    DataSerializer.writeStringArray(requiredRoleNames, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    String[] requiredRoleNames = DataSerializer.readStringArray(in);
    Set requiredRolesSet = new HashSet(requiredRoleNames.length);
    for (int i = 0; i < requiredRoleNames.length; i++) {
      Role role = InternalRole.getRole(requiredRoleNames[i]);
      requiredRolesSet.add(role);
    }
    requiredRoles = Collections.unmodifiableSet(requiredRolesSet);
  }

}
