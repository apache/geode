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

import java.util.Set;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;

class DifferentialQuorum<ID extends MemberIdentifier> implements Quorum<ID> {

  /**
   * the view where quorum was most recently lost
   */
  GMSMembershipView<ID> quorumLostView;

  @Override
  public boolean isInitialQuorum(final Set<ID> locators) {
    // one locator constitutes an initial quorum in this, the legacy differential algorithm
    return locators.size() > 0;
  }

  @Override
  public boolean isLostQuorum(final GMSMembershipView<ID> currentView,
      final GMSMembershipView<ID> prospectiveView) {
    final int oldWeight = currentView.memberWeight();
    final int failedWeight = prospectiveView.getCrashedMemberWeight(currentView);
    if (failedWeight > 0) {
      int failurePoint = (int) (Math.round(51.0 * oldWeight) / 100.0);
      if (failedWeight > failurePoint && quorumLostView != prospectiveView) {
        quorumLostView = prospectiveView;
        return true;
      }
    }
    return false;
  }

  @Override
  public String quorumLostMessage(final GMSMembershipView<ID> currentView,
      final GMSMembershipView<ID> prospectiveView) {
    return String
        .format("total weight lost in this view change is %s of %s.  Quorum has been lost!",
            prospectiveView.getCrashedMemberWeight(currentView), currentView.memberWeight());
  }
}
