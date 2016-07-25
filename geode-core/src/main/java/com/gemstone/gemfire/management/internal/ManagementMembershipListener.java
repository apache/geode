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
package com.gemstone.gemfire.management.internal;

import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * This listener is added to the cache when a node becomes Managing node. It
 * then starts to listen on various membership events to take steps accordingly
 * 
 * 
 */

public class ManagementMembershipListener implements MembershipListener {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * Resource Manager
   */
  private SystemManagementService service;
  public ManagementMembershipListener(SystemManagementService service) {
    this.service = service;
  }

  @Override
  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    if (logger.isDebugEnabled()) {
      logger.debug("ManagementMembershipListener member departed.. {}", id.getId());
    }
    if (service.isManager()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Removing member artifacts for {} from manager ",  id.getId());
      }
      service.getFederatingManager().removeMember(id, crashed);
    }
    
    service.getUniversalListenerContainer().memberDeparted(id, crashed);
  }

  @Override
  public void memberJoined(InternalDistributedMember id) {

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
  public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected, String reason) {

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

  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
  }
}
