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
 * Indicates that an attempt to send a distributed cache event to one or more
 * {@link MembershipAttributes#getRequiredRoles required roles} may have failed. Failure may be
 * caused by departure of one or more required roles while sending the message to them. If the
 * region scope is {@linkplain org.apache.geode.cache.Scope#DISTRIBUTED_NO_ACK DISTRIBUTED_NO_ACK}
 * or {@linkplain org.apache.geode.cache.Scope#GLOBAL GLOBAL} then failure may be caused by one or
 * more required roles not acknowledging receipt of the message.
 *
 * @deprecated the MembershipAttributes API is scheduled to be removed
 */
public class RegionDistributionException extends RegionRoleException {
  private static final long serialVersionUID = -5950359426786805646L;

  /**
   * Set of missing required roles causing access to the region to fail. failedRoles is transient to
   * avoid NotSerializableException. See {@see #writeObject} and {@see #readObject} for custom
   * serialization.
   */
  private transient Set failedRoles = Collections.EMPTY_SET;

  /**
   * Constructs a <code>RegionDistributionException</code> with a message.
   *
   * @param s the String message
   * @param regionFullPath full path of region for which access was attempted
   * @param failedRoles the required roles that caused this exception
   */
  public RegionDistributionException(String s, String regionFullPath, Set failedRoles) {
    super(s, regionFullPath);
    this.failedRoles = failedRoles;
    if (this.failedRoles == null) {
      this.failedRoles = Collections.EMPTY_SET;
    }
  }

  /**
   * Constructs a <code>RegionDistributionException</code> with a message and a cause.
   *
   * @param s the String message
   * @param regionFullPath full path of region for which access was attempted
   * @param failedRoles the required roles that caused this exception
   * @param ex the Throwable cause
   */
  public RegionDistributionException(String s, String regionFullPath, Set failedRoles,
      Throwable ex) {
    super(s, regionFullPath, ex);
    this.failedRoles = failedRoles;
    if (this.failedRoles == null) {
      this.failedRoles = Collections.EMPTY_SET;
    }
  }

  /**
   * Returns the required roles that caused this exception. One or more roles failed to receive a
   * cache distribution message or acknowledge receipt of that message.
   *
   * @return the required roles that caused this exception
   */
  public Set getFailedRoles() {
    return this.failedRoles;
  }

  /**
   * Override writeObject which is used in serialization. Customize serialization of this exception
   * to avoid escape of InternalRole which is not Serializable.
   */
  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    // transform roles to string names which are serializable...
    Set fr = this.failedRoles;
    if (fr == null) {
      fr = Collections.EMPTY_SET;
    }
    Set roleNames = new HashSet(fr.size());
    for (Iterator iter = fr.iterator(); iter.hasNext();) {
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
    this.failedRoles = roles;
  }

}
