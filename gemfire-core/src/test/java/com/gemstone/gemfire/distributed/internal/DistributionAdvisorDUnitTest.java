/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.distributed.internal;

import dunit.*;
import java.util.*;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.membership.*;


/**
 *
 * @author Eric Zoerner
 *
 */
public class DistributionAdvisorDUnitTest extends DistributedTestCase {
  private transient DistributionAdvisor.Profile profiles[];
  protected transient DistributionAdvisor advisor;
  
  public DistributionAdvisorDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    // connect to distributed system in every VM
    invokeInEveryVM(new SerializableRunnable("DistributionAdvisorDUnitTest: SetUp") {
      public void run() {
        getSystem();
      }
    });
    
    // reinitialize the advisor
    this.advisor = DistributionAdvisor.createDistributionAdvisor(new DistributionAdvisee() {
        public DistributionAdvisee getParentAdvisee() { return null; }
        public InternalDistributedSystem getSystem() { return DistributionAdvisorDUnitTest.this.getSystem(); }
        public String getName() {return "DistributionAdvisorDUnitTest";}
        public String getFullPath() {return getName();}
        public DM getDistributionManager() {return getSystem().getDistributionManager();}
        public DistributionAdvisor getDistributionAdvisor() {return DistributionAdvisorDUnitTest.this.advisor;}
        public DistributionAdvisor.Profile getProfile() {return null;}
        public void fillInProfile(DistributionAdvisor.Profile profile) {}
        public int getSerialNumber() {return 0;}
        public CancelCriterion getCancelCriterion() {
          return DistributionAdvisorDUnitTest.this.getSystem().getCancelCriterion();
        }
      });
    Set ids = getSystem().getDistributionManager().getOtherNormalDistributionManagerIds();
    assertEquals(4, ids.size());
    List profileList = new ArrayList();
    
    int i = 0;
    for (Iterator itr = ids.iterator(); itr.hasNext(); i++) {
      InternalDistributedMember id = (InternalDistributedMember)itr.next();
      DistributionAdvisor.Profile profile = new DistributionAdvisor.Profile(id, 0);      

      // add profile to advisor
      advisor.putProfile(profile);
      profileList.add(profile);
    }
    this.profiles = (DistributionAdvisor.Profile[])profileList.toArray(
                    new DistributionAdvisor.Profile[profileList.size()]);
  }
    
  public void tearDown2() throws Exception {
    this.advisor.close();
    super.tearDown2();
  }
  
    
  public void testGenericAdvice() {
    Set expected = new HashSet();
    for (int i = 0; i < profiles.length; i++) {
      expected.add(profiles[i].getDistributedMember());
    }
    assertEquals(expected, advisor.adviseGeneric());
  }
}
