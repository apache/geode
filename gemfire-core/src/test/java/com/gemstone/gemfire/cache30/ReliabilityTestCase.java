/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import java.util.*;

/**
 * Provides functionality helpful to testing Reliability and RequiredRoles.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public abstract class ReliabilityTestCase extends CacheTestCase {

  public ReliabilityTestCase(String name) {
    super(name);
  }

  /** Asserts that the specified roles are missing */
  protected void assertMissingRoles(String regionName, String[] roles) {
    Region region = getRootRegion(regionName);
    Set missingRoles = RequiredRoles.checkForRequiredRoles(region);
    assertNotNull(missingRoles);
    assertEquals(roles.length, missingRoles.size());
    for (Iterator iter = missingRoles.iterator(); iter.hasNext();) {
      Role role = (Role) iter.next();
      boolean found = false;
      for (int i = 0; i < roles.length; i++) {
        if (role.getName().equals(roles[i])) {
          found = true;
          break;
        }
      }
      assertTrue("Unexpected missing role: " + role.getName(), found);
    }
  }
  
  protected void waitForMemberTimeout() {
    // TODO implement me
  }
  
}

