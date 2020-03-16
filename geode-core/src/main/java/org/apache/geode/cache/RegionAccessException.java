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
package org.apache.geode.cache;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.membership.InternalRole;

/**
 * Indicates that an attempt to access the region has failed. Failure is due to one or more missing
 * {@link MembershipAttributes#getRequiredRoles required roles}. Region operations may throw this
 * exception when the {@link MembershipAttributes} have been configured with
 * {@link LossAction#NO_ACCESS} or {@link LossAction#LIMITED_ACCESS}.
 *
 * @deprecated the MembershipAttributes API is scheduled to be removed
 */
public class RegionAccessException extends RegionRoleException {
  private static final long serialVersionUID = 3142958723089038406L;

  /**
   * Set of missing required roles causing access to the region to fail. missingRoles is transient
   * to avoid NotSerializableException. See {@see #writeObject} and {@see #readObject} for custom
   * serialization.
   */
  private transient Set missingRoles = Collections.EMPTY_SET;

  /**
   * Constructs a <code>RegionAccessException</code> with a message.
   *
   * @param s the String message
   * @param regionFullPath full path of region for which access was attempted
   * @param missingRoles the missing required roles that caused this exception
   */
  public RegionAccessException(String s, String regionFullPath, Set missingRoles) {
    super(s, regionFullPath);
    this.missingRoles = missingRoles;
    if (this.missingRoles == null) {
      this.missingRoles = Collections.EMPTY_SET;
    }
  }

  /**
   * Constructs a <code>RegionAccessException</code> with a message and a cause.
   *
   * @param s the String message
   * @param regionFullPath full path of region for which access was attempted
   * @param missingRoles the missing required roles that caused this exception
   * @param ex the Throwable cause
   */
  public RegionAccessException(String s, String regionFullPath, Set missingRoles, Throwable ex) {
    super(s, regionFullPath, ex);
    this.missingRoles = missingRoles;
    if (this.missingRoles == null) {
      this.missingRoles = Collections.EMPTY_SET;
    }
  }

  /**
   * Returns the missing required roles that caused this exception.
   *
   * @return the missing required roles that caused this exception
   */
  public Set getMissingRoles() {
    return this.missingRoles;
  }

  /**
   * Override writeObject which is used in serialization. Customize serialization of this exception
   * to avoid escape of InternalRole which is not Serializable.
   */
  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    // transform roles to string names which are serializable...
    Set mr = this.missingRoles;
    if (mr == null) {
      mr = Collections.EMPTY_SET;
    }
    Set roleNames = new HashSet(mr.size());
    for (Iterator iter = mr.iterator(); iter.hasNext();) {
      String name = ((Role) iter.next()).getName();
      roleNames.add(name);
    }
    out.writeObject(roleNames);
  }

  /**
   * Override readObject which is used in serialization. Customize serialization of this exception
   * to avoid escape of InternalRole which is not Serializable.
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    // transform string names which are serializable back into roles...
    Set roleNames = (Set) in.readObject();
    Set roles = new HashSet(roleNames.size());
    for (Iterator iter = roleNames.iterator(); iter.hasNext();) {
      String name = (String) iter.next();
      roles.add(InternalRole.getRole(name));
    }
    this.missingRoles = roles;
  }

}
