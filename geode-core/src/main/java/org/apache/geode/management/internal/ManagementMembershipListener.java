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
package org.apache.geode.management.internal;

import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This listener is added to the cache when a node becomes Managing node. It then starts to listen
 * on various membership events to take steps accordingly
 *
 *
 */

public class ManagementMembershipListener implements MembershipListener {

  private static final Logger logger = LogService.getLogger();

  /**
   * Resource Manager
   */
  private final SystemManagementService service;

  public ManagementMembershipListener(SystemManagementService service) {
    this.service = service;
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager, InternalDistributedMember id,
      boolean crashed) {
    if (logger.isDebugEnabled()) {
      logger.debug("ManagementMembershipListener member departed.. {}", id.getId());
    }
    if (service.isManager()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Removing member artifacts for {} from manager ", id.getId());
      }
      service.getFederatingManager().removeMember(id, crashed);
    }

    service.getUniversalListenerContainer().memberDeparted(id, crashed);
  }

  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {

    if (logger.isDebugEnabled()) {
      logger.debug("ManagementMembershipListener member joined .. {}", id.getId());
    }
    if (service.isManager()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Adding member artifacts for {} to manager ", id.getId());
      }
      service.getFederatingManager().addMember(id);
    }
    service.getUniversalListenerContainer().memberJoined(id);
  }

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {

    if (logger.isDebugEnabled()) {
      logger.debug("ManagementMembershipListener member suspected .. {}", id.getId());
    }
    if (service.isManager()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Suspecting member {}", id.getId());
      }
      service.getFederatingManager().suspectMember(id, whoSuspected, reason);
    }
  }

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}
}
