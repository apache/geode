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
package org.apache.geode.management;

import java.beans.ConstructorProperties;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.Role;

/**
 * Composite data type used to distribute the membership attributes for a {@link Region}.
 *
 * @deprecated this API is scheduled to be removed
 */
public class MembershipAttributesData {

  /**
   * Array of required role names by this process for reliable access to the region
   */
  private final Set<String> requiredRoles;

  /**
   * The configuration defining how this process behaves when there are missing required roles
   */
  private final String lossAction;

  /**
   * The action to take when missing required roles return to the system
   */
  private final String resumptionAction;

  /**
   * This constructor is to be used by internal JMX framework only. User should not try to create an
   * instance of this class.
   *
   * @param requiredRoles Array of required role names by this process for reliable access to the
   *        region
   * @param lossAction The configuration defining how this process behaves when there are missing
   *        required roles
   * @param resumptionAction The action to take when missing required roles return to the system
   */
  @ConstructorProperties({"requiredRoles", "lossAction", "resumptionAction"

  })
  public MembershipAttributesData(Set<String> requiredRoles, String lossAction,
      String resumptionAction) {
    this.requiredRoles = requiredRoles;
    this.lossAction = lossAction;
    this.resumptionAction = resumptionAction;

  }

  /**
   * Returns the set of {@linkplain Role}s that are required for the reliability of this region.
   */
  public Set<String> getRequiredRoles() {
    return requiredRoles;
  }

  /**
   * Returns the policy that describes the action to take if any required roles are missing.
   */
  public String getLossAction() {
    return lossAction;
  }

  /**
   * Returns the policy that describes the action to take when resuming from missing roles.
   */
  public String getResumptionAction() {
    return resumptionAction;
  }

  /**
   * String representation of MembershipAttributesData
   */
  @Override
  public String toString() {
    return "MembershipAttributesData [lossAction=" + lossAction + ", requiredRoles=" + requiredRoles
        + ", resumptionAction=" + resumptionAction + "]";
  }



}
