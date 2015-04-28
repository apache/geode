/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.junit.UnitTest;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.stack.IpAddress;

import junit.framework.TestCase;

@Category(UnitTest.class)
public class MembershipJUnitTest extends TestCase {

  public MembershipJUnitTest(String name) {
    super(name);
  }

  protected void setUp() throws Exception {
    super.setUp();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Test that failed weight calculations are correctly performed.  See bug #47342
   * @throws Exception
   */
  public void testFailedWeight() throws Exception {
    // in #47342 a new view was created that contained a member that was joining but
    // was no longer reachable.  The member was included in the failed-weight and not
    // in the previous view-weight, causing a spurious network partition to be declared
    IpAddress members[] = new IpAddress[] {
        new IpAddress("localhost", 1), new IpAddress("localhost", 2), new IpAddress("localhost", 3),
        new IpAddress("localhost", 4), new IpAddress("localhost", 5), new IpAddress("localhost", 6)};
    int i = 0;
    // weight 3
    members[i].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    members[i++].shouldntBeCoordinator(false);
    // weight 3
    members[i].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    members[i++].shouldntBeCoordinator(false);
    // weight 15 (cache+leader)
    members[i].setVmKind(DistributionManager.NORMAL_DM_TYPE);
    members[i++].shouldntBeCoordinator(true);
    // weight 0
    members[i].setVmKind(DistributionManager.ADMIN_ONLY_DM_TYPE);
    members[i++].shouldntBeCoordinator(true);
    // weight 0
    members[i].setVmKind(DistributionManager.ADMIN_ONLY_DM_TYPE);
    members[i++].shouldntBeCoordinator(true);
    // weight 10
    members[i].setVmKind(DistributionManager.NORMAL_DM_TYPE);
    members[i++].shouldntBeCoordinator(true);
    
    ViewId vid = new ViewId(members[0], 4);
    Vector<IpAddress> vmbrs = new Vector<IpAddress>();
    for (i=0; i<members.length; i++) {
      vmbrs.add(members[i]);
    }
    View lastView = new View(vid, vmbrs);
    IpAddress leader = members[2];
    assertTrue(!leader.preferredForCoordinator());
    
    IpAddress joiningMember = new IpAddress("localhost", 7);
    joiningMember.setVmKind(DistributionManager.NORMAL_DM_TYPE);
    joiningMember.shouldntBeCoordinator(true);
    
    // have the joining member and another cache process (weight 10) in the failed members
    // collection and check to make sure that the joining member is not included in failed
    // weight calcs.
    Set<IpAddress> failedMembers = new HashSet<IpAddress>();
    failedMembers.add(joiningMember);
    failedMembers.add(members[members.length-1]); // cache
    failedMembers.add(members[members.length-2]); // admin
    int failedWeight = GMS.processFailuresAndGetWeight(lastView, leader, failedMembers);
//    System.out.println("last view = " + lastView);
//    System.out.println("failed mbrs = " + failedMembers);
//    System.out.println("failed weight = " + failedWeight);
    assertEquals("failure weight calculation is incorrect", 10, failedWeight);
    assertTrue(!failedMembers.contains(members[members.length-2]));
  }

}
