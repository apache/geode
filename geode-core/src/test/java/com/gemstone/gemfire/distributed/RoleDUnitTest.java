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
package com.gemstone.gemfire.distributed;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalRole;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

/**
 * Tests the functionality of the {@link DistributedMember} class.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class RoleDUnitTest extends DistributedTestCase {

  public RoleDUnitTest(String name) {
    super(name);
  }
  
  /**
   * Tests usage of Roles in a Loner vm.
   */
  public void testRolesInLonerVM() {
    final String rolesProp = "A,B,C,D,E,F,G";
    final String[] rolesArray = new String[] {"A","B","C","D","E","F","G"};
//    final List rolesList = Arrays.asList(rolesArray);
    
    Properties config = new Properties();
    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
    config.setProperty(DistributionConfig.ROLES_NAME, rolesProp);

    InternalDistributedSystem system = getSystem(config);
    try {
      DM dm = system.getDistributionManager();
      Set allRoles = dm.getAllRoles();
      assertEquals(rolesArray.length, allRoles.size());
      
      InternalDistributedMember member = dm.getDistributionManagerId();
      Set roles = member.getRoles();
      assertEquals(rolesArray.length, roles.size());
      
      Role roleA = InternalRole.getRole("roleA");
      assertEquals(false, roleA.isPresent());
      assertEquals(0, roleA.getCount());
      
      for (Iterator iter = roles.iterator(); iter.hasNext();) {
        Role role = (Role) iter.next();
        assertEquals(true, role.isPresent());
        assertEquals(1, role.getCount());
      }
    } 
    finally {
      system.disconnect();
    }
  }

  /**
   * Tests usage of Roles in four distributed vms.
   */
  public void testRolesInDistributedVMs() {  
    // connect all four vms...
    final String[] vmRoleNames = new String[] 
        {"VM_A", "BAR", "Foo,BAR", "Bip,BAM"};
    final String[][] vmRoles = new String[][] 
        {{"VM_A"}, {"BAR"}, {"Foo","BAR"}, {"Bip","BAM"}};
    final Object[][] roleCounts = new Object[][]
        {{"VM_A", new Integer(1)}, {"BAR", new Integer(2)},
         {"Foo", new Integer(1)}, {"Bip", new Integer(1)},
         {"BAM", new Integer(1)}};

    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable() {
        public void run() {
          disconnectFromDS();
          Properties config = new Properties();
          config.setProperty(DistributionConfig.ROLES_NAME, vmRoleNames[vm]);
          getSystem(config);
        }
      });
    }
    
    // validate roles from each vm...
    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable() {
        public void run() {
          InternalDistributedSystem sys = getSystem();
          DM dm = sys.getDistributionManager();
          
          Set allRoles = dm.getAllRoles();
          assertEquals("allRoles is " + allRoles.size() + 
              " but roleCounts should be " + roleCounts.length, 
              roleCounts.length, allRoles.size());
          
          for (Iterator iter = allRoles.iterator(); iter.hasNext();) {
            // match role with string in roleCounts
            Role role = (Role) iter.next();
            for (int j = 0; j < roleCounts.length; j++) {
              if (role.getName().equals(roleCounts[j][0])) {
                // parse count
                int count = ((Integer) roleCounts[j][1]).intValue();
                // assert count
                assertEquals("count for role " + role + " is wrong",
                    count, dm.getRoleCount(role));
                assertEquals("isRolePresent for role " + role + " should be true",
                    true, dm.isRolePresent(role));
              }
            }
          }
        }
      });
    }
  }
  
  /** 
   * Tests that specifying duplicate role names results in just one Role.
   */
  public void testDuplicateRoleNames() {
    final String rolesProp = "A,A";
    
    Properties config = new Properties();
    config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    config.setProperty(DistributionConfig.LOCATORS_NAME, "");
    config.setProperty(DistributionConfig.ROLES_NAME, rolesProp);

    InternalDistributedSystem system = getSystem(config);
    try {
      DM dm = system.getDistributionManager();
      InternalDistributedMember member = dm.getDistributionManagerId();
      
      Set roles = member.getRoles();
      assertEquals(1, roles.size());
      
      Role role = (Role) roles.iterator().next();
      assertEquals(true, role.isPresent());
      assertEquals(1, role.getCount());
    } 
    finally {
      system.disconnect();
    }
  }
  
}

