/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.Role;

/**
 * Composite data type used to distribute the membership attributes for
 * a {@link Region}.
 * 
 * @author rishim
 * @since 7.0
 * 
 */
public class MembershipAttributesData {

  /**
   * Array of required role names by this process for reliable access to the
   * region
   */
  private Set<String> requiredRoles;

  /**
   * The configuration defining how this process behaves when there are missing
   * required roles
   */
  private String lossAction;

  /**
   * The action to take when missing required roles return to the system
   */
  private String resumptionAction;

  @ConstructorProperties( { "requiredRoles", "lossAction", "resumptionAction"

  })
  public MembershipAttributesData(Set<String> requiredRoles, String lossAction,
      String resumptionAction) {
    this.requiredRoles = requiredRoles;
    this.lossAction = lossAction;
    this.resumptionAction = resumptionAction;

  }
  
  /**
   * Returns the set of {@linkplain Role}s that are required for the reliability
   * of this region.
   */
  public Set<String> getRequiredRoles() {
    return requiredRoles;
  }

  /**
   * Returns the policy that describes the action to take if any required
   * roles are missing.
   */
  public String getLossAction() {
    return lossAction;
  }

  /**
   * Returns the policy that describes the action to take when resuming
   * from missing roles.
   */
  public String getResumptionAction() {
    return resumptionAction;
  }

  @Override
  public String toString() {
    return "MembershipAttributesData [lossAction=" + lossAction
        + ", requiredRoles=" + requiredRoles + ", resumptionAction="
        + resumptionAction + "]";
  }



}
