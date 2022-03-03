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
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RequiredRoles;
import org.apache.geode.distributed.Role;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Provides functionality helpful to testing Reliability and RequiredRoles.
 *
 * @since GemFire 5.0
 */
public abstract class ReliabilityTestCase extends JUnit4CacheTestCase {

  /** Asserts that the specified roles are missing */
  protected void assertMissingRoles(String regionName, String[] roles) {
    Region region = getRootRegion(regionName);
    Set missingRoles = RequiredRoles.checkForRequiredRoles(region);
    assertNotNull(missingRoles);
    assertEquals(roles.length, missingRoles.size());
    for (final Object missingRole : missingRoles) {
      Role role = (Role) missingRole;
      boolean found = false;
      for (final String s : roles) {
        if (role.getName().equals(s)) {
          found = true;
          break;
        }
      }
      assertTrue("Unexpected missing role: " + role.getName(), found);
    }
  }

  protected void waitForMemberTimeout() {}
}
