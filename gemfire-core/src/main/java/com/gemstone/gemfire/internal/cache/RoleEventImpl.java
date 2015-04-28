/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author Kirk Lund
 * @since 5.0
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

