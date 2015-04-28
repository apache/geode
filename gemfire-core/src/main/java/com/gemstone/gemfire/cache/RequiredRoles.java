/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.*;

/**
 * Provides information on presence or absence of a <code>Region</code>'s 
 * required roles. Configuration of required roles is accomplished using the
 * <code>Region</code>'s {@link MembershipAttributes}. 
 *
 * A {@link com.gemstone.gemfire.distributed.Role Role} may be present in the
 * distributed system even if it the <code>Role</code> is not present in the
 * <code>Region</code> membership. This would occur if none of the members
 * filling that <code>Role</code> currently have a <code>Cache</code> or the
 * specific <code>Region</code> created. In this case the <code>Role</code> is
 * considered to be absent for that <code>Region</code>.
 *
 * @author Kirk Lund
 * @see com.gemstone.gemfire.distributed.Role
 * @since 5.0
 */
public class RequiredRoles {
  
  /**
   * Returns a set of any currently missing required roles for the
   * specified region.  If the region is not configured to require roles
   * an empty set will always be returned.
   *
   * @param region the region to check for missing required roles
   * @return set of required roles that are currently missing
   * @throws IllegalStateException if region is not configured with required roles
   */
  public static Set<Role> checkForRequiredRoles(Region<?,?> region) {
    try {
      return waitForRequiredRoles(region, 0);
    }
    catch (InterruptedException ie) {
      // This could happen if we were in an interrupted state
      // upon method entry
      Thread.currentThread().interrupt();
      ((LocalRegion)region).getCancelCriterion().checkCancelInProgress(ie);
      Assert.assertTrue(false, 
          "checkForRequiredRoles cannot throw InterruptedException");
      return Collections.emptySet(); // keep compiler happy
    }
  }

  /**
   * Returns a set of any currently missing required roles for the
   * specified region.  This will wait the specified timeout in milliseconds 
   * for any missing required roles to be filled.  If there are no missing 
   * required roles or if the region is not configured to require any roles
   * then an empty set will immediately be returned.
   *
   * @param region the region to check for missing required roles
   * @param timeout milliseconds to wait for any missing required roles
   * @return set of required roles that are currently missing
   * @throws NullPointerException if region is null
   * @throws InterruptedException if thread is interrupted while waiting
   * @throws IllegalStateException if region is not configured with required roles
   */
  public static Set<Role> waitForRequiredRoles(Region<?,?> region, long timeout)
  throws InterruptedException {
//    if (Thread.interrupted()) throw new InterruptedException(); not necessary waitForRequiredRoles does this
    if (region == null) {
      throw new NullPointerException(LocalizedStrings.RequiredRoles_REGION_MUST_BE_SPECIFIED.toLocalizedString());
    }
    if (!(region instanceof DistributedRegion)) {
      throw new IllegalStateException(LocalizedStrings.RequiredRoles_REGION_HAS_NOT_BEEN_CONFIGURED_WITH_REQUIRED_ROLES.toLocalizedString());
    }
    DistributedRegion dr = (DistributedRegion) region;
    return dr.waitForRequiredRoles(timeout);
  }
  
  /**
   * Returns true if the {@link com.gemstone.gemfire.distributed.Role Role}
   * is currently present in the {@link Region} membership. This returns true
   * only if one or more members filling this role actually have the region
   * currently created. The role may be present in the distributed system even
   * if the role is not present in the region membership.
   *
   * @param region the region whose membership will be searched
   * @param role the role to check for
   */
  public static boolean isRoleInRegionMembership(Region<?,?> region, Role role) {
    if (region instanceof DistributedRegion) {
      DistributedRegion dr = (DistributedRegion) region;
      return dr.isRoleInRegionMembership(role);
    }
    else {
      return role.isPresent();
    }
  }
  
}

