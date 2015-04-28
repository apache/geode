/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */

package com.gemstone.gemfire.management.internal;

import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.admin.remote.AlertLevelChangeMessage;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;

/**
 * This class will act as a messenger from manager to members for various
 * operations.
 * 
 * To start with its is designed to Manager start stop details, change alert
 * level.
 * 
 * 
 * @author rishim
 * 
 */
public class MemberMessenger {

  private MBeanJMXAdapter jmxAdapter;
  private ManagementResourceRepo repo;
  private InternalDistributedSystem system;

  public MemberMessenger(MBeanJMXAdapter jmxAdapter,
      ManagementResourceRepo repo, InternalDistributedSystem system) {
    this.jmxAdapter = jmxAdapter;
    this.repo = repo;
    this.system = system;

  }

  public void sendManagerInfo(DistributedMember receiver) {

    String levelName = jmxAdapter.getDistributedSystemMXBean().getAlertLevel();
    int alertCode = LogWriterImpl.levelNameToCode(levelName);
    ManagerStartupMessage msg = ManagerStartupMessage.create(alertCode);
    msg.setRecipient((InternalDistributedMember) receiver);
    sendAsync(msg);

  }

  public void broadcastManagerInfo() {
    Set<DistributedMember> otherMemberSet = system.getDistributionManager()
        .getAllOtherMembers();

    String levelName = jmxAdapter.getDistributedSystemMXBean().getAlertLevel();
    int alertCode = LogWriterImpl.levelNameToCode(levelName);
    ManagerStartupMessage msg = ManagerStartupMessage.create(alertCode);
    if (otherMemberSet != null && otherMemberSet.size() > 0) {
      msg.setRecipients(otherMemberSet);
    }

    sendAsync(msg);
    
    DM dm = system.getDistributionManager();
    if(dm instanceof DistributionManager){
      msg.process((DistributionManager)system.getDistributionManager());
    }
    

  }

  /**
   * Sends a message and does not wait for a response
   */
  void sendAsync(DistributionMessage msg) {
    if (system != null) {
      system.getDistributionManager().putOutgoing(msg);
    }
  }

  /**
   * Sets the alert level for this manager agent. Sends a
   * {@link AlertLevelChangeMessage} to each member of the distributed system.
   */
  public void setAlertLevel(String levelName) {
    int alertCode = LogWriterImpl.levelNameToCode(levelName);
    AlertLevelChangeMessage m = AlertLevelChangeMessage.create(alertCode);
    sendAsync(m);
  }
}
