/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class MembershipJUnitTest extends TestCase {
  Level baseLogLevel;
  
  public MembershipJUnitTest(String name) {
    super(name);
  }

  @Before
  protected void setUp() throws Exception {
    baseLogLevel = LogService.getBaseLogLevel();
//    LogService.setBaseLogLevel(Level.DEBUG);
    super.setUp();
  }

  @After
  protected void tearDown() throws Exception {
//    LogService.setBaseLogLevel(baseLogLevel);
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
    InternalDistributedMember members[] = new InternalDistributedMember[] {
        new InternalDistributedMember("localhost", 1), new InternalDistributedMember("localhost", 2), new InternalDistributedMember("localhost", 3),
        new InternalDistributedMember("localhost", 4), new InternalDistributedMember("localhost", 5), new InternalDistributedMember("localhost", 6)};
    int i = 0;
    // weight 3
    members[i].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(true);
    // weight 3
    members[i].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(true);
    // weight 15 (cache+leader)
    members[i].setVmKind(DistributionManager.NORMAL_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(false);
    // weight 0
    members[i].setVmKind(DistributionManager.ADMIN_ONLY_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(false);
    // weight 0
    members[i].setVmKind(DistributionManager.ADMIN_ONLY_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(false);
    // weight 10
    members[i].setVmKind(DistributionManager.NORMAL_DM_TYPE);
    members[i++].getNetMember().setPreferredForCoordinator(false);
    
    List<InternalDistributedMember> vmbrs = new ArrayList(members.length);
    for (i=0; i<members.length; i++) {
      vmbrs.add(members[i]);
    }
    NetView lastView = new NetView(members[0], 4, vmbrs, Collections.EMPTY_LIST, Collections.EMPTY_LIST);
    InternalDistributedMember leader = members[2];
    assertTrue(!leader.getNetMember().preferredForCoordinator());
    
    InternalDistributedMember joiningMember = new InternalDistributedMember("localhost", 7);
    joiningMember.setVmKind(DistributionManager.NORMAL_DM_TYPE);
    joiningMember.getNetMember().setPreferredForCoordinator(false);
    
    // have the joining member and another cache process (weight 10) in the failed members
    // collection and check to make sure that the joining member is not included in failed
    // weight calcs.
    List<InternalDistributedMember> failedMembers = new ArrayList<InternalDistributedMember>(3);
    failedMembers.add(joiningMember);
    failedMembers.add(members[members.length-1]); // cache
    failedMembers.add(members[members.length-2]); // admin
    List<InternalDistributedMember> newMbrs = new ArrayList<InternalDistributedMember>(lastView.getMembers());
    newMbrs.removeAll(failedMembers);
    NetView newView = new NetView(members[0], 5, newMbrs, Collections.EMPTY_LIST, failedMembers);
    
    int failedWeight = newView.getCrashedMemberWeight(lastView);
//    System.out.println("last view = " + lastView);
//    System.out.println("failed mbrs = " + failedMembers);
//    System.out.println("failed weight = " + failedWeight);
    assertEquals("failure weight calculation is incorrect", 10, failedWeight);
    List<InternalDistributedMember> actual = newView.getActualCrashedMembers(lastView);
    assertTrue(!actual.contains(members[members.length-2]));
  }
  
}
