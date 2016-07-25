/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import java.io.*;
import java.util.*;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RoleEvent;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.distributed.internal.membership.InternalRole;

/**
 * Implementation of a RoleEvent.  Super class is DataSerializable but
 * this class is probably never on the wire, however, it does support it.
 *
 * @since GemFire 5.0
 */
public final class RoleEventImpl extends RegionEventImpl
implements RoleEvent, DataSerializable {

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
  RoleEventImpl(Region region, Operation op, Object callbackArgument,
                       boolean originRemote, 
                       DistributedMember distributedMember, Set requiredRoles) {
    super(region, op, callbackArgument, originRemote, distributedMember);
    this.requiredRoles = Collections.unmodifiableSet(requiredRoles);
  }
  
  public Set getRequiredRoles() {
    return this.requiredRoles; // already unmodifiableSet
  }

  @Override  
  public int getDSFID() {
    return ROLE_EVENT;
  }

  @Override  
  public void toData(DataOutput out) throws IOException  {
    super.toData(out);
    String[] requiredRoleNames = new String[this.requiredRoles.size()];
    Iterator iter = this.requiredRoles.iterator();
    for (int i = 0; i < requiredRoleNames.length; i++) {
      Role role = (Role) iter.next();
      requiredRoleNames[i] = role.getName();
    }
    DataSerializer.writeStringArray(requiredRoleNames, out);
  }

  @Override  
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    super.fromData(in);
    String[] requiredRoleNames = DataSerializer.readStringArray(in);
    Set requiredRolesSet = new HashSet(requiredRoleNames.length);
    for (int i = 0; i < requiredRoleNames.length; i++) {
      Role role = InternalRole.getRole(requiredRoleNames[i]);
      requiredRolesSet.add(role);
    }
    this.requiredRoles = Collections.unmodifiableSet(requiredRolesSet);
  }

}

