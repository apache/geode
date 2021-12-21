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

import static org.apache.geode.distributed.ConfigurationProperties.ROLES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.LossAction;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionRoleListener;
import org.apache.geode.cache.ResumptionAction;
import org.apache.geode.cache.RoleEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.RegionRoleListenerAdapter;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.membership.InternalRole;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;

/**
 * Tests the functionality of the {@link RegionRoleListener} class.
 *
 * @since GemFire 5.0
 */

public class RegionReliabilityListenerDUnitTest extends ReliabilityTestCase {

  protected static transient Set rolesGain = null;
  protected static transient Set rolesLoss = null;

  public RegionReliabilityListenerDUnitTest() {
    super();
  }

  /**
   * Tests the notification of afterRoleGain and afterRoleLoss
   */
  @Test
  public void testRoleGainAndLoss() throws Exception {
    final String name = getUniqueName();
    final int vm0 = 0;
    final int vm1 = 1;
    final int vm2 = 2;
    final int vm3 = 3;

    final String roleA = name + "-A";
    final String roleC = name + "-C";
    final String roleD = name + "-D";

    // assign names to 4 vms...
    final String[] requiredRoles = {roleA, roleC, roleD};
    final String[] rolesProp = {"", roleA, roleA, roleC + "," + roleD};
    final String[][] vmRoles = new String[][] {{}, {roleA}, {roleA}, {roleC, roleD}};
    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          Properties config = new Properties();
          config.setProperty(ROLES, rolesProp[vm]);
          getSystem(config);
        }
      });
    }

    // define RegionRoleListener
    final RegionRoleListener listener = new RegionRoleListenerAdapter() {
      @Override
      public void afterRoleGain(RoleEvent event) {
        RegionReliabilityListenerDUnitTest.rolesGain = event.getRequiredRoles();
      }

      @Override
      public void afterRoleLoss(RoleEvent event) {
        RegionReliabilityListenerDUnitTest.rolesLoss = event.getRequiredRoles();
      }
    };

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(ROLES, "");
    getSystem(config);

    // create region in controller...
    MembershipAttributes ra =
        new MembershipAttributes(requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);

    AttributesFactory fac = new AttributesFactory();
    fac.addCacheListener(listener);
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);

    RegionAttributes attr = fac.create();
    createRootRegion(name, attr);

    // wait for memberTimeout to expire
    waitForMemberTimeout();

    // assert in state of role loss... test all are missing according to RequiredRoles
    assertMissingRoles(name, requiredRoles);

    // assert gain is fired...
    SerializableRunnable create = new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    };

    // create region in vm0... no gain for no role
    Host.getHost(0).getVM(vm0).invoke(create);
    assertNull(rolesGain);
    assertNull(rolesLoss);
    assertMissingRoles(name, requiredRoles);

    // create region in vm1... gain for 1st instance of redundant role
    Host.getHost(0).getVM(vm1).invoke(create);
    assertNotNull(rolesGain);
    assertEquals(vmRoles[vm1].length, rolesGain.size());
    for (int i = 0; i < vmRoles[vm1].length; i++) {
      Role role = InternalRole.getRole(vmRoles[vm1][i]);
      assertEquals(true, rolesGain.contains(role));
    }
    assertNull(rolesLoss);
    rolesGain = null;
    assertMissingRoles(name, vmRoles[vm3]); // only vm3 has missing roles

    // create region in vm2... no gain for 2nd instance of redundant role
    Host.getHost(0).getVM(vm2).invoke(create);
    assertNull(rolesGain);
    assertNull(rolesLoss);
    assertMissingRoles(name, vmRoles[vm3]); // only vm3 has missing roles

    // create region in vm3... gain for 2 roles
    Host.getHost(0).getVM(vm3).invoke(create);
    assertNotNull(rolesGain);
    assertEquals(vmRoles[vm3].length, rolesGain.size());
    for (int i = 0; i < vmRoles[vm3].length; i++) {
      Role role = InternalRole.getRole(vmRoles[vm3][i]);
      assertEquals(true, rolesGain.contains(role));
    }
    assertNull(rolesLoss);
    rolesGain = null;
    assertMissingRoles(name, new String[0]); // no missing roles

    // assert loss is fired...
    SerializableRunnable destroy = new CacheSerializableRunnable("Destroy Region") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        region.localDestroyRegion();
      }
    };

    // destroy region in vm0... no loss of any role
    Host.getHost(0).getVM(vm0).invoke(destroy);
    assertNull(rolesGain);
    assertNull(rolesLoss);
    assertMissingRoles(name, new String[0]); // no missing roles

    // destroy region in vm1... nothing happens in 1st removal of redundant role
    Host.getHost(0).getVM(vm1).invoke(destroy);
    assertNull(rolesGain);
    assertNull(rolesLoss);
    assertMissingRoles(name, new String[0]); // no missing roles

    // destroy region in vm2... 2nd removal of redundant role is loss
    Host.getHost(0).getVM(vm2).invoke(destroy);
    assertNull(rolesGain);
    assertNotNull(rolesLoss);
    assertEquals(vmRoles[vm2].length, rolesLoss.size());
    for (int i = 0; i < vmRoles[vm2].length; i++) {
      Role role = InternalRole.getRole(vmRoles[vm2][i]);
      assertEquals(true, rolesLoss.contains(role));
    }
    rolesLoss = null;
    assertMissingRoles(name, vmRoles[vm2]); // only vm2 has missing roles

    // destroy region in vm3... two more roles are in loss
    Host.getHost(0).getVM(vm3).invoke(destroy);
    assertNull(rolesGain);
    assertNotNull(rolesLoss);
    assertEquals(vmRoles[vm3].length, rolesLoss.size());
    for (int i = 0; i < vmRoles[vm3].length; i++) {
      Role role = InternalRole.getRole(vmRoles[vm3][i]);
      assertEquals(true, rolesLoss.contains(role));
    }
    rolesLoss = null;
    assertMissingRoles(name, requiredRoles); // all roles are missing
  }

}
