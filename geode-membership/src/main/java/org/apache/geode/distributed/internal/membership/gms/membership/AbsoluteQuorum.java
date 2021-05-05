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

import static org.apache.geode.distributed.internal.membership.api.MemberIdentifier.LOCATOR_DM_TYPE;

import java.util.Collection;
import java.util.Set;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;

class AbsoluteQuorum<ID extends MemberIdentifier> implements Quorum<ID> {

  private int minimumLocatorCount;

  AbsoluteQuorum(final int minimumLocatorCount) {
    this.minimumLocatorCount = minimumLocatorCount;
  }

  @Override
  public boolean isInitialQuorum(final Set<ID> locators) {
    return isQuorum(locators);
  }

  @Override
  public boolean isLostQuorum(final GMSMembershipView<ID> currentView,
      final GMSMembershipView<ID> prospectiveView) {
    return !isQuorum(prospectiveView.getMembers());
  }

  @Override
  public String quorumLostMessage(final GMSMembershipView<ID> currentView,
      final GMSMembershipView<ID> prospectiveView) {
    return String.format("total number of locators %d is less than minimum %d. Quorum is lost!",
        locatorCount(prospectiveView.getMembers()), minimumLocatorCount);
  }

  private boolean isQuorum(final Collection<ID> members) {
    return locatorCount(members) >= minimumLocatorCount;
  }

  private long locatorCount(final Collection<ID> members) {
    return members.stream().filter(
        member -> member.getVmKind() == LOCATOR_DM_TYPE)
        .count();
  }
}
