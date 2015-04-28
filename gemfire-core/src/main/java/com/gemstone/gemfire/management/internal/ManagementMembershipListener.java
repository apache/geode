/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
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
 * @author rishim
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
  public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected) {

    if (logger.isDebugEnabled()) {
      logger.debug("ManagementMembershipListener member suspected .. {}", id.getId());
    }
    if (service.isManager()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Suspecting member {}", id.getId());
      }
      service.getFederatingManager().suspectMember(id, whoSuspected);
    }
  }

  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
  }
}
