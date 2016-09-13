/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache30;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RequiredRoles;
import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;

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
  }
}
