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
package com.gemstone.gemfire.distributed.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

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
