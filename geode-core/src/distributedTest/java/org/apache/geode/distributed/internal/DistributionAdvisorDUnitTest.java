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
package org.apache.geode.distributed.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class DistributionAdvisorDUnitTest extends JUnit4DistributedTestCase {

  private transient DistributionAdvisor.Profile profiles[];
  protected transient DistributionAdvisor advisor;

  @Override
  public final void postSetUp() throws Exception {
    // connect to distributed system in every VM
    Invoke.invokeInEveryVM(new SerializableRunnable("DistributionAdvisorDUnitTest: SetUp") {
      public void run() {
        getSystem();
      }
    });

    DistributionAdvisee advisee = mock(DistributionAdvisee.class);
    when(advisee.getName()).thenReturn("DistributionAdvisorDUnitTest");
    when(advisee.getSystem()).thenReturn(getSystem());
    when(advisee.getFullPath()).thenReturn(getName());
    when(advisee.getDistributionManager()).thenReturn(getSystem().getDistributionManager());
    when(advisee.getCancelCriterion()).thenReturn(getSystem().getCancelCriterion());

    advisor = DistributionAdvisor.createDistributionAdvisor(advisee);

    when(advisee.getDistributionAdvisor()).thenReturn(advisor);

    Set ids = getSystem().getDistributionManager().getOtherNormalDistributionManagerIds();
    assertEquals(VM.getVMCount(), ids.size());
    List profileList = new ArrayList();

    int i = 0;
    for (Iterator itr = ids.iterator(); itr.hasNext(); i++) {
      InternalDistributedMember id = (InternalDistributedMember) itr.next();
      DistributionAdvisor.Profile profile = new DistributionAdvisor.Profile(id, 0);

      // add profile to advisor
      advisor.putProfile(profile);
      profileList.add(profile);
    }
    this.profiles = (DistributionAdvisor.Profile[]) profileList
        .toArray(new DistributionAdvisor.Profile[profileList.size()]);
  }

  @Override
  public final void preTearDown() throws Exception {
    this.advisor.close();
  }


  @Test
  public void testGenericAdvice() {
    Set expected = new HashSet();
    for (int i = 0; i < profiles.length; i++) {
      expected.add(profiles[i].getDistributedMember());
    }
    assertEquals(expected, advisor.adviseGeneric());
  }

  @Test
  public void advisorIssuesSevereAlertForStateFlush() throws Exception {
    final long membershipVersion = advisor.startOperation();
    advisor.forceNewMembershipVersion();

    final Logger logger = mock(Logger.class);
    final Exception exceptionHolder[] = new Exception[1];
    Thread thread = new Thread(() -> {
      try {
        advisor.waitForCurrentOperations(logger, 2000, 4000);
      } catch (RuntimeException e) {
        synchronized (exceptionHolder) {
          exceptionHolder[0] = e;
        }
      }
    });
    thread.setDaemon(true);
    thread.start();

    try {
      await().untilAsserted(() -> {
        verify(logger, atLeastOnce()).warn(isA(String.class), isA(Long.class));
      });
      await().untilAsserted(() -> {
        verify(logger, atLeastOnce()).fatal(isA(String.class), isA(Long.class));
      });
      advisor.endOperation(membershipVersion);
      await().untilAsserted(() -> {
        verify(logger, atLeastOnce()).info("Wait for current operations completed");
      });
      await().until(() -> !thread.isAlive());
    } finally {
      if (thread.isAlive()) {
        advisor.endOperation(membershipVersion);
        thread.interrupt();
        thread.join(10000);
      } else {
        synchronized (exceptionHolder) {
          if (exceptionHolder[0] != null) {
            throw exceptionHolder[0];
          }
        }
      }
    }
  }
}
