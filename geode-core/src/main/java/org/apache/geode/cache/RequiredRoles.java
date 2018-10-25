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

import java.util.Collections;
import java.util.Set;

import org.apache.geode.distributed.Role;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.InternalRegion;

/**
 * Provides information on presence or absence of a <code>Region</code>'s required roles.
 * Configuration of required roles is accomplished using the <code>Region</code>'s
 * {@link MembershipAttributes}.
 *
 * A {@link org.apache.geode.distributed.Role Role} may be present in the distributed system even if
 * it the <code>Role</code> is not present in the <code>Region</code> membership. This would occur
 * if none of the members filling that <code>Role</code> currently have a <code>Cache</code> or the
 * specific <code>Region</code> created. In this case the <code>Role</code> is considered to be
 * absent for that <code>Region</code>.
 *
 * @deprecated this feature is scheduled to be removed
 * @see org.apache.geode.distributed.Role
 */
public class RequiredRoles {

  /**
   * Returns a set of any currently missing required roles for the specified region. If the region
   * is not configured to require roles an empty set will always be returned.
   *
   * @param region the region to check for missing required roles
   * @return set of required roles that are currently missing
   * @throws IllegalStateException if region is not configured with required roles
   */
  public static Set<Role> checkForRequiredRoles(Region<?, ?> region) {
    try {
      return waitForRequiredRoles(region, 0);
    } catch (InterruptedException ie) {
      // This could happen if we were in an interrupted state
      // upon method entry
      Thread.currentThread().interrupt();
      ((InternalRegion) region).getCancelCriterion().checkCancelInProgress(ie);
      Assert.assertTrue(false, "checkForRequiredRoles cannot throw InterruptedException");
      return Collections.emptySet(); // keep compiler happy
    }
  }

  /**
   * Returns a set of any currently missing required roles for the specified region. This will wait
   * the specified timeout in milliseconds for any missing required roles to be filled. If there are
   * no missing required roles or if the region is not configured to require any roles then an empty
   * set will immediately be returned.
   *
   * @param region the region to check for missing required roles
   * @param timeout milliseconds to wait for any missing required roles
   * @return set of required roles that are currently missing
   * @throws NullPointerException if region is null
   * @throws InterruptedException if thread is interrupted while waiting
   * @throws IllegalStateException if region is not configured with required roles
   */
  public static Set<Role> waitForRequiredRoles(Region<?, ?> region, long timeout)
      throws InterruptedException {
    if (region == null) {
      throw new NullPointerException(
          "Region must be specified");
    }
    if (!(region instanceof DistributedRegion)) {
      throw new IllegalStateException(
          "Region has not been configured with required roles.");
    }
    DistributedRegion dr = (DistributedRegion) region;
    return dr.waitForRequiredRoles(timeout);
  }

  /**
   * Returns true if the {@link org.apache.geode.distributed.Role Role} is currently present in the
   * {@link Region} membership. This returns true only if one or more members filling this role
   * actually have the region currently created. The role may be present in the distributed system
   * even if the role is not present in the region membership.
   *
   * @param region the region whose membership will be searched
   * @param role the role to check for
   */
  public static boolean isRoleInRegionMembership(Region<?, ?> region, Role role) {
    if (region instanceof DistributedRegion) {
      DistributedRegion dr = (DistributedRegion) region;
      return dr.isRoleInRegionMembership(role);
    } else {
      return role.isPresent();
    }
  }

}
