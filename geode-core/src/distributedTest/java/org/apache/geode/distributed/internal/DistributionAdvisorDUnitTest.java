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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category(MembershipTest.class)
@SuppressWarnings("serial")
public class DistributionAdvisorDUnitTest extends DistributedTestCase {

  private transient Profile[] profiles;
  private transient DistributionAdvisor advisor;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() {
    // connect to distributed system in every VM
    invokeInEveryVM(() -> {
      getSystem();
    });

    DistributionAdvisee advisee = mock(DistributionAdvisee.class);
    when(advisee.getName()).thenReturn("DistributionAdvisorDUnitTest");
    when(advisee.getSystem()).thenReturn(getSystem());
    when(advisee.getFullPath()).thenReturn(getName());
    when(advisee.getDistributionManager()).thenReturn(getSystem().getDistributionManager());
    when(advisee.getCancelCriterion()).thenReturn(getSystem().getCancelCriterion());

    advisor = new DistributionAdvisor(advisee);
    advisor.initialize();

    when(advisee.getDistributionAdvisor()).thenReturn(advisor);

    Set<InternalDistributedMember> ids =
        getSystem().getDistributionManager().getOtherNormalDistributionManagerIds();
    assertEquals(VM.getVMCount(), ids.size());
    List<Profile> profileList = new ArrayList<>();

    for (InternalDistributedMember id : ids) {
      Profile profile = new Profile(id, 0);

      // add profile to advisor
      advisor.putProfile(profile);
      profileList.add(profile);
    }

    profiles = profileList.toArray(new Profile[0]);
  }

  @After
  public void tearDown() {
    advisor.close();
  }

  @Test
  public void testGenericAdvice() {
    Set<InternalDistributedMember> expected = new HashSet<>();
    for (Profile profile : profiles) {
      expected.add(profile.getDistributedMember());
    }
    assertEquals(expected, advisor.adviseGeneric());
  }

  @Test
  public void advisorIssuesSevereAlertForStateFlush() throws Exception {
    long membershipVersion = advisor.startOperation();
    advisor.forceNewMembershipVersion();

    Logger logger = mock(Logger.class);

    Future<Void> waitForCurrentOperations = executorServiceRule.submit(() -> {
      try {
        advisor.waitForCurrentOperations(logger, 2000, 4000);
      } catch (Exception e) {
        errorCollector.addError(e);
      }
    });

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

    waitForCurrentOperations.get(getTimeout().toMillis(), MILLISECONDS);
  }
}
