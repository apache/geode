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
package org.apache.geode.distributed.internal.membership.gms.membership;

import static org.junit.Assert.assertNotNull;

import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.WaitCriterion;

public class GMSJoinLeaveTestHelper {

  public static void becomeCoordinatorForTest(GMSJoinLeave gmsJoinLeave) {
    synchronized (gmsJoinLeave.getViewInstallationLock()) {
      gmsJoinLeave.becomeCoordinator();
    }
  }

  public static boolean isViewCreator() {
    GMSJoinLeave gmsJoinLeave = getGmsJoinLeave();
    assertNotNull("There should be a JoinLeave for every DS", gmsJoinLeave);
    return gmsJoinLeave.getView().getCreator().equals(gmsJoinLeave.getMemberID());
  }

  private static void waitCriterion() {
    WaitCriterion waitCriterion = new WaitCriterion() {
      public boolean done() {
        try {
          return getIDS() != null;
        } catch (Exception e) {
          e.printStackTrace();
        }
        return false; // NOTREACHED
      }

      public String description() {
        return "Distributed system is null";
      }
    };
    GeodeAwaitility.await().untilAsserted(waitCriterion);
  }

  private static GMSJoinLeave getGmsJoinLeave() {
    InternalDistributedSystem distributedSystem = getInternalDistributedSystem();
    DistributionManager dm = distributedSystem.getDM();
    GMSMembershipManager membershipManager = (GMSMembershipManager) dm.getMembershipManager();
    Services services = membershipManager.getServices();
    return (GMSJoinLeave) services.getJoinLeave();
  }

  public static InternalDistributedMember getCurrentCoordinator() {
    return getGmsJoinLeave().getView().getCoordinator();
  }

  public static Integer getViewId() {
    return getGmsJoinLeave().getView().getViewId();
  }

  public static InternalDistributedSystem getInternalDistributedSystem() {
    waitCriterion();
    return getIDS();
  }

  private static InternalDistributedSystem getIDS() {
    InternalDistributedSystem distributedSystem = InternalDistributedSystem.getAnyInstance();
    if (distributedSystem == null) {
      Locator locator = Locator.getLocator();
      if (locator == null) {
        return null;
      }
      return (InternalDistributedSystem) locator.getDistributedSystem();
    } else {
      return distributedSystem;
    }
  }
}
