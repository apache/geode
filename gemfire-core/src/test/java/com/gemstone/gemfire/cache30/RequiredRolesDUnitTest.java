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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.LossAction;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RequiredRoles;
import com.gemstone.gemfire.cache.ResumptionAction;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.InternalRole;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

/**
 * Tests the functionality of the {@link RequiredRoles} class.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class RequiredRolesDUnitTest extends ReliabilityTestCase {

  protected transient volatile boolean startTestWaitForRequiredRoles = false;
  protected transient volatile boolean finishTestWaitForRequiredRoles = false;
  protected transient volatile boolean failTestWaitForRequiredRoles = false;
  protected transient Set rolesTestWaitForRequiredRoles = new HashSet();
  
  public RequiredRolesDUnitTest(String name) {
    super(name);
  }
  
  /**
   * Tests that RequiredRoles detects missing roles.
   */
  public void testRequiredRolesInLoss() throws Exception {
    String name = this.getUniqueName();
    
    final String roleA = name+"-A";
    final String roleC = name+"-C";
    final String roleD = name+"-D";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA,roleC,roleD};
    Set requiredRolesSet = new HashSet();
    for (int i = 0; i < requiredRoles.length; i++) {
      requiredRolesSet.add(InternalRole.getRole(requiredRoles[i]));
    }
    assertEquals(requiredRoles.length, requiredRolesSet.size());

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);
    
    RegionAttributes attr = fac.create();
    Region region = createRootRegion(name, attr);
    
    RegionAttributes rattr = region.getAttributes();
    assertEquals(true, rattr.getMembershipAttributes().hasRequiredRoles());
    
    Set roles = rattr.getMembershipAttributes().getRequiredRoles();
    assertNotNull(roles);
    assertEquals(false, roles.isEmpty());
    assertEquals(requiredRolesSet.size(), roles.size());
    assertEquals(true, roles.containsAll(requiredRolesSet));
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();
    
    // assert all are missing according to RequiredRoles...
    Set missingRoles = RequiredRoles.checkForRequiredRoles(region);
    assertNotNull(missingRoles);
    assertEquals(requiredRolesSet.size(), missingRoles.size());
    assertEquals(true, missingRoles.containsAll(requiredRolesSet));
    
    // assert isPresent is false on each missing role...
    for (Iterator iter = missingRoles.iterator(); iter.hasNext();) {
      Role role = (Role) iter.next();
      assertEquals(false, role.isPresent());
    }
  }
  
  /**
   * Tests RequiredRoles.waitForRequiredRoles().
   */
  public void testWaitForRequiredRoles() throws Exception {
    final String name = this.getUniqueName();
    final int vm0 = 0;
    final int vm1 = 1;
    final int vm2 = 2;
    final int vm3 = 3;
    
    final String roleA = name+"-A";
    final String roleC = name+"-C";
    final String roleD = name+"-D";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA,roleC,roleD};
    final String[] rolesProp = {"",roleA,roleA,roleC+","+roleD};
    final String[][] vmRoles = new String[][] {{},{roleA},{roleA},{roleC,roleD}};
    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable() {
        public void run() {
          Properties config = new Properties();
          config.setProperty(DistributionConfig.ROLES_NAME, rolesProp[vm]);
          getSystem(config);
        }
      });
    }

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);
    
    RegionAttributes attr = fac.create();
    final Region region = createRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();
    
    // assert in state of role loss... test all are missing according to RequiredRoles
    assertMissingRoles(name, requiredRoles);
    
    // create thread to call waitForRequiredRoles
    Runnable runWaitForRequiredRoles = new Runnable() {
      public void run() {
        startTestWaitForRequiredRoles = true;
        try {
          rolesTestWaitForRequiredRoles = 
              RequiredRoles.waitForRequiredRoles(region, -1);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          failTestWaitForRequiredRoles = true;
        }
        finishTestWaitForRequiredRoles = true;
      }
    };
    
    // assert thread is waiting
    Thread threadA = new Thread(group, runWaitForRequiredRoles);
    threadA.start();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return RequiredRolesDUnitTest.this.startTestWaitForRequiredRoles;
      }
      public String description() {
        return "waiting for test start";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
    assertTrue(this.startTestWaitForRequiredRoles);
    assertFalse(this.finishTestWaitForRequiredRoles);
    
    // create region in vms and assert impact on threadA
    SerializableRunnable create = new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    };
        
    // create region in vm0... no gain for no role
    Host.getHost(0).getVM(vm0).invoke(create);
    assertFalse(this.finishTestWaitForRequiredRoles);
        
    // create region in vm1... gain for 1st instance of redundant role
    Host.getHost(0).getVM(vm1).invoke(create);
    assertFalse(this.finishTestWaitForRequiredRoles);
        
    // create region in vm2... no gain for 2nd instance of redundant role
    Host.getHost(0).getVM(vm2).invoke(create);
    assertFalse(this.finishTestWaitForRequiredRoles);
        
    // create region in vm3... gain for 2 roles
    Host.getHost(0).getVM(vm3).invoke(create);
    DistributedTestCase.join(threadA, 30 * 1000, getLogWriter());
    assertTrue(this.finishTestWaitForRequiredRoles);
    assertTrue(this.rolesTestWaitForRequiredRoles.isEmpty());
    
    // assert loss is fired...
    SerializableRunnable destroy = new CacheSerializableRunnable("Destroy Region") {
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        region.localDestroyRegion();
      }
    };
        
    // destroy region in vm0... no loss of any role
    Host.getHost(0).getVM(vm0).invoke(destroy);

    // assert new call to RequiredRoles doesn't wait (no role in vm0)
    this.startTestWaitForRequiredRoles = false;
    this.finishTestWaitForRequiredRoles = false;
    threadA = new Thread(group, runWaitForRequiredRoles);
    threadA.start();
    DistributedTestCase.join(threadA, 30 * 1000, getLogWriter());
    assertTrue(this.startTestWaitForRequiredRoles);
    assertTrue(this.finishTestWaitForRequiredRoles);
    assertTrue(this.rolesTestWaitForRequiredRoles.isEmpty());
    
    // destroy region in vm1... nothing happens in 1st removal of redundant role
    Host.getHost(0).getVM(vm1).invoke(destroy);
        
    // assert new call to RequiredRoles doesn't wait (redundant role in vm1)
    this.startTestWaitForRequiredRoles = false;
    this.finishTestWaitForRequiredRoles = false;
    threadA = new Thread(group, runWaitForRequiredRoles);
    threadA.start();
    DistributedTestCase.join(threadA, 30 * 1000, getLogWriter());
    assertTrue(this.startTestWaitForRequiredRoles);
    assertTrue(this.finishTestWaitForRequiredRoles);
    assertTrue(this.rolesTestWaitForRequiredRoles.isEmpty());
    
    // destroy region in vm2... 2nd removal of redundant role is loss
    Host.getHost(0).getVM(vm2).invoke(destroy);

    // assert new call to RequiredRoles does wait (lost role in vm2)
    this.startTestWaitForRequiredRoles = false;
    this.finishTestWaitForRequiredRoles = false;
    threadA = new Thread(group, runWaitForRequiredRoles);
    threadA.start();
    
    // assert thread is waiting
    ev = new WaitCriterion() {
      public boolean done() {
        return RequiredRolesDUnitTest.this.startTestWaitForRequiredRoles;
      }
      public String description() {
        return "waiting for test start";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
    assertTrue(this.startTestWaitForRequiredRoles);
    assertFalse(this.finishTestWaitForRequiredRoles);
    assertMissingRoles(name, vmRoles[vm2]);
    
    // end the wait and make sure no roles are missing
    Host.getHost(0).getVM(vm2).invoke(create);
    DistributedTestCase.join(threadA, 30 * 1000, getLogWriter());
    assertTrue(this.startTestWaitForRequiredRoles);
    assertTrue(this.finishTestWaitForRequiredRoles);
    assertTrue(this.rolesTestWaitForRequiredRoles.isEmpty());
    assertMissingRoles(name, new String[] {});
    
    assertFalse(failTestWaitForRequiredRoles);
  }
  
  /**
   * Tests RequiredRoles.isRoleInRegionMembership().
   */
  public void testIsRoleInRegionMembership() throws Exception {
    final String name = this.getUniqueName();
    final int vm0 = 0;
    final int vm1 = 1;
    final int vm2 = 2;
    final int vm3 = 3;
    
    final String roleA = name+"-A";
    final String roleC = name+"-C";
    final String roleD = name+"-D";
    
    // assign names to 4 vms...
    final String[] requiredRoles = {roleA,roleC,roleD};
    final String[] rolesProp = {"",roleA,roleA,roleC+","+roleD};
    final String[][] vmRoles = new String[][] {{},{roleA},{roleA},{roleC,roleD}};
    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable() {
        public void run() {
          Properties config = new Properties();
          config.setProperty(DistributionConfig.ROLES_NAME, rolesProp[vm]);
          getSystem(config);
        }
      });
    }

    // connect controller to system...
    Properties config = new Properties();
    config.setProperty(DistributionConfig.ROLES_NAME, "");
    getSystem(config);
    
    // create region in controller...
    MembershipAttributes ra = new MembershipAttributes(
        requiredRoles, LossAction.FULL_ACCESS, ResumptionAction.NONE);
    
    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);
    
    RegionAttributes attr = fac.create();
    Region region = createRootRegion(name, attr);
    
    // wait for memberTimeout to expire
    waitForMemberTimeout();
    
    // assert each role is missing
    final Set requiredRolesSet = 
        region.getAttributes().getMembershipAttributes().getRequiredRoles();
    for (Iterator iter = requiredRolesSet.iterator(); iter.hasNext();) {
      Role role = (Role) iter.next();
      assertFalse(RequiredRoles.isRoleInRegionMembership(region, role));
    }
    
    SerializableRunnable create = new CacheSerializableRunnable("Create Region") {
      public void run2() throws CacheException {
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        RegionAttributes attr = fac.create();
        createRootRegion(name, attr);
      }
    };
        
    // create region in vm0... no gain for no role
    Host.getHost(0).getVM(vm0).invoke(create);
    for (Iterator iter = requiredRolesSet.iterator(); iter.hasNext();) {
      Role role = (Role) iter.next();
      assertFalse(RequiredRoles.isRoleInRegionMembership(region, role));
    }
        
    // create region in vm1... gain for 1st instance of redundant role
    Host.getHost(0).getVM(vm1).invoke(create);
    for (int i = 0; i < vmRoles[vm1].length; i++) {
      Role role = InternalRole.getRole(vmRoles[vm1][i]);
      assertTrue(RequiredRoles.isRoleInRegionMembership(region, role));
    }
        
    // create region in vm2... no gain for 2nd instance of redundant role
    Host.getHost(0).getVM(vm2).invoke(create);
    for (int i = 0; i < vmRoles[vm2].length; i++) {
      Role role = InternalRole.getRole(vmRoles[vm2][i]);
      assertTrue(RequiredRoles.isRoleInRegionMembership(region, role));
    }
        
    // create region in vm3... gain for 2 roles
    Host.getHost(0).getVM(vm3).invoke(create);
    for (int i = 0; i < vmRoles[vm3].length; i++) {
      Role role = InternalRole.getRole(vmRoles[vm3][i]);
      assertTrue(RequiredRoles.isRoleInRegionMembership(region, role));
    }
        
    SerializableRunnable destroy = new CacheSerializableRunnable("Destroy Region") {
      public void run2() throws CacheException {
        Region region = getRootRegion(name);
        region.localDestroyRegion();
      }
    };
        
    // destroy region in vm0... no loss of any role
    Host.getHost(0).getVM(vm0).invoke(destroy);
    for (Iterator iter = requiredRolesSet.iterator(); iter.hasNext();) {
      Role role = (Role) iter.next();
      assertTrue(RequiredRoles.isRoleInRegionMembership(region, role));
    }
        
    // destroy region in vm1... nothing happens in 1st removal of redundant role
    Host.getHost(0).getVM(vm1).invoke(destroy);
    for (Iterator iter = requiredRolesSet.iterator(); iter.hasNext();) {
      Role role = (Role) iter.next();
      assertTrue(RequiredRoles.isRoleInRegionMembership(region, role));
    }
        
    // destroy region in vm2... 2nd removal of redundant role is loss
    Host.getHost(0).getVM(vm2).invoke(destroy);
    for (int i = 0; i < vmRoles[vm2].length; i++) {
      Role role = InternalRole.getRole(vmRoles[vm2][i]);
      assertFalse(RequiredRoles.isRoleInRegionMembership(region, role));
    }
        
    // destroy region in vm3... two more roles are in loss
    Host.getHost(0).getVM(vm3).invoke(destroy);
    for (Iterator iter = requiredRolesSet.iterator(); iter.hasNext();) {
      Role role = (Role) iter.next();
      assertFalse(RequiredRoles.isRoleInRegionMembership(region, role));
    }
  }
  
  private transient final ThreadGroup group = 
      new ThreadGroup("RequiredRolesDUnitTest Threads") {
        public void uncaughtException(Thread t, Throwable e)
        {
          if (e instanceof VirtualMachineError) {
            SystemFailure.setFailure((VirtualMachineError)e); // don't throw
          }
          String s = "Uncaught exception in thread " + t;
          getLogWriter().error(s, e);
          fail(s);
        }
      };
}

