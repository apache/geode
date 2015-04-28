/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

import java.io.*;
import java.util.*;
import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.distributed.internal.membership.InternalRole;

/**
 * Indicates that an attempt to access the region has failed.  Failure is
 * due to one or more missing {@link MembershipAttributes#getRequiredRoles 
 * required roles}.  Region operations may throw this exception when the 
 * {@link MembershipAttributes} have been configured with {@link 
 * LossAction#NO_ACCESS} or {@link LossAction#LIMITED_ACCESS}.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class RegionAccessException extends RegionRoleException {
private static final long serialVersionUID = 3142958723089038406L;
  
  /** 
   * Set of missing required roles causing access to the region to fail.
   * missingRoles is transient to avoid NotSerializableException. See {@link
   * #writeObject} and {@link #readObject} for custom serialization.
   */
  private transient Set missingRoles = Collections.EMPTY_SET;
  
  /** 
   * Constructs a <code>RegionAccessException</code> with a message.
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
   * Constructs a <code>RegionAccessException</code> with a message and
   * a cause.
   * @param s the String message
   * @param regionFullPath full path of region for which access was attempted
   * @param missingRoles the missing required roles that caused this exception
   * @param ex the Throwable cause
   */
  public RegionAccessException(String s,  String regionFullPath, Set missingRoles, Throwable ex) {
    super(s, regionFullPath, ex);
    this.missingRoles = missingRoles;
    if (this.missingRoles == null) {
      this.missingRoles = Collections.EMPTY_SET;
    }
  }
  
  /** 
   * Returns the missing required roles that caused this exception.
   * @return the missing required roles that caused this exception
   */
  public Set getMissingRoles() {
    return this.missingRoles;
  }
  
  /** 
   * Override writeObject which is used in serialization. Customize 
   * serialization of this exception to avoid escape of InternalRole
   * which is not Serializable. 
   */
  private void writeObject(java.io.ObjectOutputStream out)
  throws IOException {
    out.defaultWriteObject();
    // transform roles to string names which are serializable...
    Set roleNames = new HashSet(this.missingRoles.size());
    for (Iterator iter = this.missingRoles.iterator(); iter.hasNext();) {
      String name = ((Role)iter.next()).getName();
      roleNames.add(name);
    }
    out.writeObject(roleNames);
  }
  
  /** 
   * Override readObject which is used in serialization. Customize 
   * serialization of this exception to avoid escape of InternalRole
   * which is not Serializable. 
   */
  private void readObject(java.io.ObjectInputStream in)
  throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    // transform string names which are serializable back into roles...
    Set roleNames = (Set)in.readObject();
    Set roles = new HashSet(roleNames.size());
    for (Iterator iter = roleNames.iterator(); iter.hasNext();) {
      String name = (String) iter.next();
      roles.add(InternalRole.getRole(name));
    }
    this.missingRoles = roles;
  }
     
}

