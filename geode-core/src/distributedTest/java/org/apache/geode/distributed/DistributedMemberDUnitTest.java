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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.ROLES;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.IncompatibleSystemException;
import org.apache.geode.distributed.internal.Distribution;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityAckedMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.distributed.internal.membership.gms.GMSMembership;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests the functionality of the {@link DistributedMember} class.
 *
 */
@Category({MembershipTest.class})
public class DistributedMemberDUnitTest extends JUnit4DistributedTestCase {

  /**
   * Tests default settings.
   */
  @Test
  public void testDefaults() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(ROLES, "");
    config.setProperty(GROUPS, "");
    config.setProperty(NAME, "");

    InternalDistributedSystem system = getSystem(config);
    try {
      assertTrue(system.getConfig().getRoles().equals(DistributionConfig.DEFAULT_ROLES));
      assertTrue(system.getConfig().getGroups().equals(DistributionConfig.DEFAULT_ROLES));
      assertTrue(system.getConfig().getName().equals(DistributionConfig.DEFAULT_NAME));

      DistributionManager dm = system.getDistributionManager();
      InternalDistributedMember member = dm.getDistributionManagerId();

      Set roles = member.getRoles();
      assertEquals(0, roles.size());
      assertEquals("", member.getName());
      assertEquals(Collections.emptyList(), member.getGroups());
    } finally {
      system.disconnect();
    }
  }

  @Test
  public void testNonDefaultName() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(NAME, "nondefault");

    InternalDistributedSystem system = getSystem(config);
    try {
      assertEquals("nondefault", system.getConfig().getName());

      DistributionManager dm = system.getDistributionManager();
      InternalDistributedMember member = dm.getDistributionManagerId();

      assertEquals("nondefault", member.getName());
    } finally {
      system.disconnect();
    }
  }

  /**
   * Tests the configuration of many Roles and groups in one vm. Confirms no runtime distinction
   * between roles and groups.
   */
  @Test
  public void testRolesInOneVM() {
    final String rolesProp = "A,B,C";
    final String groupsProp = "D,E,F,G";
    final List bothList = Arrays.asList(new String[] {"A", "B", "C", "D", "E", "F", "G"});

    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(ROLES, rolesProp);
    config.setProperty(GROUPS, groupsProp);

    InternalDistributedSystem system = getSystem(config);
    try {
      assertEquals(rolesProp, system.getConfig().getRoles());
      assertEquals(groupsProp, system.getConfig().getGroups());

      DistributionManager dm = system.getDistributionManager();
      InternalDistributedMember member = dm.getDistributionManagerId();

      Set roles = member.getRoles();
      assertEquals(bothList.size(), roles.size());

      for (Iterator iter = roles.iterator(); iter.hasNext();) {
        Role role = (Role) iter.next();
        assertTrue(bothList.contains(role.getName()));
      }

      assertEquals(bothList, member.getGroups());
    } finally {
      system.disconnect();
    }
  }

  @Test
  public void testTwoMembersSameName() {
    disconnectFromDS(); // or assertion on # members fails when run-dunit-tests
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Properties config = new Properties();
        config.setProperty(NAME, "name0");
        getSystem(config);
      }
    });
    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Properties config = new Properties();
        config.setProperty(NAME, "name1");
        getSystem(config);
      }
    });
    Host.getHost(0).getVM(2).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Properties config = new Properties();
        config.setProperty(NAME, "name0");
        try {
          getSystem(config);
          fail("expected IncompatibleSystemException");
        } catch (IncompatibleSystemException expected) {
        }
      }
    });
  }

  /**
   * Tests the configuration of one unique Role in each of four vms. Verifies that each vm is aware
   * of the other vms' Roles.
   */
  @Test
  public void testRolesInAllVMs() {
    disconnectAllFromDS(); // or assertion on # members fails when run-dunit-tests

    // connect all four vms...
    final String[] vmRoles = new String[] {"VM_A", "VM_B", "VM_C", "VM_D"};
    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          // disconnectFromDS();
          Properties config = new Properties();
          config.setProperty(ROLES, vmRoles[vm]);
          config.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
          getSystem(config);
        }
      });
    }

    // validate roles from each vm...
    for (int i = 0; i < vmRoles.length; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          InternalDistributedSystem sys = getSystem();
          assertNotNull(sys.getConfig().getRoles());
          assertTrue(sys.getConfig().getRoles().equals(vmRoles[vm]));

          DistributionManager dm = sys.getDistributionManager();
          InternalDistributedMember self = dm.getDistributionManagerId();

          Set myRoles = self.getRoles();
          assertEquals(1, myRoles.size());

          Role myRole = (Role) myRoles.iterator().next();
          assertTrue(vmRoles[vm].equals(myRole.getName()));

          await()
              .until(() -> dm.getOtherNormalDistributionManagerIds().size() == 3);
          Set<InternalDistributedMember> members = dm.getOtherNormalDistributionManagerIds();

          for (Iterator iterMembers = members.iterator(); iterMembers.hasNext();) {
            InternalDistributedMember member = (InternalDistributedMember) iterMembers.next();
            Set roles = member.getRoles();
            assertEquals(1, roles.size());
            for (Iterator iterRoles = roles.iterator(); iterRoles.hasNext();) {
              Role role = (Role) iterRoles.next();
              assertTrue(!role.getName().equals(myRole.getName()));
              boolean foundRole = false;
              for (int j = 0; j < vmRoles.length; j++) {
                if (vmRoles[j].equals(role.getName())) {
                  foundRole = true;
                  break;
                }
              }
              assertTrue(foundRole);
            }
          }
        }
      });
    }
  }


  private InternalDistributedMember connectAndSetUpPartialID() throws Exception {
    Properties properties = new Properties();
    properties.put("name", "myName");
    InternalDistributedSystem system = getSystem(properties);
    assertTrue(system == basicGetSystem()); // senders will use basicGetSystem()
    InternalDistributedMember internalDistributedMember = system.getDistributedMember();

    internalDistributedMember.setName(null);
    HeapDataOutputStream outputStream = new HeapDataOutputStream(100);
    internalDistributedMember.writeEssentialData(outputStream);
    DataInputStream dataInputStream =
        new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
    InternalDistributedMember partialID =
        InternalDistributedMember.readEssentialData(dataInputStream);
    return partialID;
  }

  /**
   * GEODE-3043 surprise member added when member is already in the cluster
   *
   * This test ensures that a partial ID (with no "name") is equal to its equivalent non-partial ID.
   *
   */
  @Test
  public void testMemberNameIgnoredInPartialID() throws Exception {
    InternalDistributedMember partialID = connectAndSetUpPartialID();

    InternalDistributedMember internalDistributedMember = basicGetSystem().getDistributedMember();
    assertTrue(partialID.isPartial());
    assertFalse(internalDistributedMember.isPartial());

    assertEquals(internalDistributedMember, partialID);
    assertEquals(partialID, internalDistributedMember);
  }

  /**
   * GEODE-3043 surprise member added when member is already in the cluster
   *
   * This test ensures that the membership manager can detect and replace a partial ID with one that
   * is not partial
   *
   */
  @Test
  public void testPartialIDInMessageReplacedWithFullID() throws Exception {
    InternalDistributedMember partialID = connectAndSetUpPartialID();

    HighPriorityAckedMessage message = new HighPriorityAckedMessage();
    message.setSender(partialID);

    Distribution manager =
        MembershipManagerHelper.getDistribution(basicGetSystem());
    ((GMSMembership) manager.getMembership()).replacePartialIdentifierInMessage(message);

    assertFalse(message.getSender().isPartial());
  }

  private static String makeOddEvenString(int vm) {
    return ((vm % 2) == 0) ? "EVENS" : "ODDS";
  }

  private static String makeGroupsString(int vm) {
    return "" + vm + ", " + makeOddEvenString(vm);
  }

  /**
   * Tests the configuration of one unique group in each of four vms. Verifies that each vm is aware
   * of the other vms' groups.
   */
  @Test
  public void testGroupsInAllVMs() {
    disconnectFromDS(); // or assertion on # members fails when run-dunit-tests

    // connect all four vms...
    for (int i = 0; i < 4; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          // disconnectFromDS();
          Properties config = new Properties();
          config.setProperty(GROUPS, makeGroupsString(vm));
          getSystem(config);
        }
      });
    }

    // validate group from each vm...
    for (int i = 0; i < 4; i++) {
      final int vm = i;
      Host.getHost(0).getVM(vm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          InternalDistributedSystem sys = getSystem();
          final String expectedMyGroup = makeGroupsString(vm);
          assertEquals(expectedMyGroup, sys.getConfig().getGroups());

          DistributionManager dm = sys.getDistributionManager();
          DistributedMember self = sys.getDistributedMember();

          List<String> myGroups = self.getGroups();

          assertEquals(Arrays.asList("" + vm, makeOddEvenString(vm)), myGroups);

          await()
              .until(() -> dm.getOtherNormalDistributionManagerIds().size() == 3);
          Set<InternalDistributedMember> members = dm.getOtherNormalDistributionManagerIds();

          // Make sure getAllOtherMembers returns a set
          // containing our three peers plus an admin member.
          Set<DistributedMember> others = sys.getAllOtherMembers();
          assertEquals(4, others.size());
          others.removeAll(dm.getOtherNormalDistributionManagerIds());
          assertEquals(1, others.size());
          // test getGroupMembers
          HashSet<DistributedMember> evens = new HashSet<DistributedMember>();
          HashSet<DistributedMember> odds = new HashSet<DistributedMember>();
          boolean isEvens = true;
          for (String groupName : Arrays.asList("0", "1", "2", "3")) {
            Set<DistributedMember> gm = sys.getGroupMembers(groupName);
            if (isEvens) {
              evens.addAll(gm);
            } else {
              odds.addAll(gm);
            }
            isEvens = !isEvens;
            if (groupName.equals("" + vm)) {
              assertEquals(Collections.singleton(self), gm);
            } else {
              assertEquals(1, gm.size());
              assertEquals("members=" + members + " gm=" + gm, true, members.removeAll(gm));
            }
          }
          assertEquals(Collections.emptySet(), members);
          assertEquals(evens, sys.getGroupMembers("EVENS"));
          assertEquals(odds, sys.getGroupMembers("ODDS"));
        }
      });
    }
  }

  /**
   * The method getId() returns a string that is used as a key identifier in the JMX and Admin APIs.
   * This test asserts that it matches the expected format. If you change DistributedMember.getId()
   * or DistributedSystem. getMemberId() you will need to look closely at the format, decide if it
   * is appropriate for JMX as the id for SystemMember mbeans and then adjust this test as needed.
   *
   * Changing the id can result in bad keys in JMX and can result in numerous errors in Admin/JMX
   * tests.
   */
  @Test
  public void testGetId() {
    Properties config = new Properties();
    config.setProperty(MCAST_PORT, "0");
    config.setProperty(LOCATORS, "");
    config.setProperty(NAME, "foobar");

    InternalDistributedSystem system = getSystem(config);
    try {

      DistributionManager dm = system.getDistributionManager();
      DistributedMember member = dm.getDistributionManagerId();

      assertEquals(member.getId(), system.getMemberId());
      assertTrue(member.getId().contains("foobar"));
    } finally {
      system.disconnect();
    }
  }

  @Test
  public void testFindMemberByName() {
    disconnectAllFromDS(); // or assertion on # members fails when run-dunit-tests
    VM vm0 = Host.getHost(0).getVM(0);
    VM vm1 = Host.getHost(0).getVM(1);
    VM vm2 = Host.getHost(0).getVM(2);
    final DistributedMember member0 = createSystemAndGetId(vm0, "name0");
    final DistributedMember member1 = createSystemAndGetId(vm1, "name1");
    final DistributedMember member2 = createSystemAndGetId(vm2, "name2");
    vm0.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        DistributedSystem system = getSystem();
        assertEquals(member0, system.findDistributedMember("name0"));
        assertEquals(member1, system.findDistributedMember("name1"));
        assertEquals(member2, system.findDistributedMember("name2"));
        assertNull(system.findDistributedMember("name3"));

        Set<DistributedMember> members;
        try {
          members = system.findDistributedMembers(InetAddress.getByName(member0.getHost()));
          HashSet expected = new HashSet();
          expected.add(member0);
          expected.add(member1);
          expected.add(member2);

          // Members will contain the locator as well. Just make sure it has
          // the members we're looking for.
          assertTrue("Expected" + expected + " got " + members, members.containsAll(expected));
          assertEquals(4, members.size());
        } catch (UnknownHostException e) {
          fail("Unable to get IpAddress", e);
        }
      }
    });
  }

  private DistributedMember createSystemAndGetId(VM vm, final String name) {
    return (DistributedMember) vm.invoke(new SerializableCallable("create system and get member") {

      @Override
      public Object call() throws Exception {
        Properties config = new Properties();
        config.setProperty(NAME, name);
        DistributedSystem ds = getSystem(config);
        return ds.getDistributedMember();
      }
    });
  }

}
