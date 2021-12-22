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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.ROLES;
import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.InternalRole;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests the setting of Roles in a DistributedSystem
 */
@Category({MembershipTest.class})
public class RoleDUnitTest extends JUnit4DistributedTestCase {

  static Properties distributionProperties = new Properties();

  @Override
  public Properties getDistributedSystemProperties() {
    return distributionProperties;
  }

  /**
   * Tests usage of Roles in a Loner vm.
   */
  @Test
  public void testRolesInLonerVM() {
    final String rolesProp = "A,B,C,D,E,F,G";
    final String[] rolesArray = new String[] {"A", "B", "C", "D", "E", "F", "G"};

    distributionProperties = new Properties();
    distributionProperties.setProperty(MCAST_PORT, "0");
    distributionProperties.setProperty(LOCATORS, "");
    distributionProperties.setProperty(ROLES, rolesProp);

    InternalDistributedSystem system = getSystem(distributionProperties);
    try {
      DistributionManager dm = system.getDistributionManager();
      Set allRoles = dm.getAllRoles();
      assertEquals(rolesArray.length, allRoles.size());

      InternalDistributedMember member = dm.getDistributionManagerId();
      Set roles = member.getRoles();
      assertEquals(rolesArray.length, roles.size());

      Role roleA = InternalRole.getRole("roleA");
      assertEquals(false, roleA.isPresent());
      assertEquals(0, roleA.getCount());

      for (final Object o : roles) {
        Role role = (Role) o;
        assertEquals(true, role.isPresent());
        assertEquals(1, role.getCount());
      }
    } finally {
      system.disconnect();
    }
  }

  /**
   * Tests usage of Roles in four distributed vms.
   */
  @Test
  public void testRolesInDistributedVMs() {
    // connect all four vms...
    final String[] vmRoles = new String[] {"VM_A", "BAR", "Foo,BAR", "Bip,BAM"};
    final Object[][] roleCounts = new Object[][] {{"VM_A", 1}, {"BAR", 2},
        {"Foo", 1}, {"Bip", 1}, {"BAM", 1}};

    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable("create system") {
        @Override
        public void run() {
          disconnectFromDS();
          Properties config = new Properties();
          config.setProperty(ROLES, vmRoles[vm]);
          config.setProperty(LOG_LEVEL, "fine");
          distributionProperties = config;
          getSystem();
        }
      });
    }

    // validate roles from each vm...
    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable("verify roles") {
        @Override
        public void run() {
          InternalDistributedSystem sys = getSystem();
          DistributionManager dm = sys.getDistributionManager();

          Set allRoles = dm.getAllRoles();
          assertEquals(
              "allRoles is " + allRoles.size() + " but roleCounts should be " + roleCounts.length,
              roleCounts.length, allRoles.size());

          for (final Object allRole : allRoles) {
            // match role with string in roleCounts
            Role role = (Role) allRole;
            for (final Object[] roleCount : roleCounts) {
              if (role.getName().equals(roleCount[0])) {
                // parse count
                int count = (Integer) roleCount[1];
                // assert count
                assertEquals("count for role " + role + " is wrong", count, dm.getRoleCount(role));
                assertEquals("isRolePresent for role " + role + " should be true", true,
                    dm.isRolePresent(role));
              }
            }
          }
        }
      });
    }
    System.out.println("testRolesInDistributedVMs completed");
  }

  /**
   * Tests that specifying duplicate role names results in just one Role.
   */
  @Test
  public void testDuplicateRoleNames() {
    final String rolesProp = "A,A";

    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(ROLES, rolesProp);
    distributionProperties = config;

    InternalDistributedSystem system = getSystem();
    try {
      DistributionManager dm = system.getDistributionManager();
      InternalDistributedMember member = dm.getDistributionManagerId();

      Set roles = member.getRoles();
      assertEquals(1, roles.size());

      Role role = (Role) roles.iterator().next();
      assertEquals(true, role.isPresent());
      assertEquals(1, role.getCount());
    } finally {
      system.disconnect();
    }
  }

}
