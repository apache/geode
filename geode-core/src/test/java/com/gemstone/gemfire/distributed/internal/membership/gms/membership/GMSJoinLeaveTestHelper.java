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
package com.gemstone.gemfire.distributed.internal.membership.gms.membership;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;

public class GMSJoinLeaveTestHelper {

  public static void becomeCoordinatorForTest(GMSJoinLeave gmsJoinLeave) {
    synchronized (gmsJoinLeave.getViewInstallationLock()) {
      gmsJoinLeave.becomeCoordinator();
    }
  }

  public static boolean isViewCreator() {
    GMSJoinLeave gmsJoinLeave = getGmsJoinLeave();
    if (gmsJoinLeave != null) {
      GMSJoinLeave.ViewCreator viewCreator = gmsJoinLeave.getViewCreator();
      if (viewCreator != null && !viewCreator.isShutdown()) {
        return true;
      } else {
        return false;
      }
    }
    throw new RuntimeException("This should not have happened. There should be a JoinLeave for every DS");
  }

  private static GMSJoinLeave getGmsJoinLeave() {
    InternalDistributedSystem distributedSystem = getInternalDistributedSystem();
    DM dm = distributedSystem.getDM();
    GMSMembershipManager membershipManager = (GMSMembershipManager) dm.getMembershipManager();
    Services services = membershipManager.getServices();
    return (GMSJoinLeave) services.getJoinLeave();
  }

  public static Integer getViewId() {
    return getGmsJoinLeave().getView().getViewId();
  }

  private static InternalDistributedSystem getInternalDistributedSystem() {
    InternalDistributedSystem distributedSystem = InternalDistributedSystem.getAnyInstance();
    if (distributedSystem == null) {
      Locator locator = Locator.getLocator();
      return (InternalDistributedSystem) locator.getDistributedSystem();
    } else {
      return distributedSystem;
    }
  }
}
