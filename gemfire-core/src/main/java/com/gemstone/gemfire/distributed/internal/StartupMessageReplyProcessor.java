/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.util.Set;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;

public class StartupMessageReplyProcessor extends ReplyProcessor21
{
  /** has a rejection message (license mismatch, etc) been received? */
  private boolean receivedRejectionMessage;

  /** set to true once we receive a reply from someone who accepted us
   * into the group. Note that we receive replies from admin dm but
   * they do not have the authority to accept us into the group.
   */
  private boolean receivedAcceptance;
  private DM dm;

  public StartupMessageReplyProcessor(DM dm, Set recipients) {
    super(dm, recipients);
    this.dm = dm;
  }

  @Override
  protected boolean removeMember(InternalDistributedMember m, boolean departed) {
    boolean result = super.removeMember(m, departed);
    dm.removeUnfinishedStartup(m, true);
    return result;
  }
  
  protected boolean getReceivedRejectionMessage() {
    return this.receivedRejectionMessage;
  }

  protected boolean getReceivedAcceptance() {
    return this.receivedAcceptance;
  }
  
  protected void setReceivedRejectionMessage(boolean v) {
    this.receivedRejectionMessage = v;
  }

  protected void setReceivedAcceptance(boolean v) {
    this.receivedAcceptance = v;
  }

  /**
   * Adds any unresponsive members to s
   */
  void collectUnresponsiveMembers(Set s) {
    if (stillWaiting()) {
      InternalDistributedMember[] memberList = getMembers();
      synchronized (memberList) {
        for (int i=0; i < memberList.length; i++) {
          InternalDistributedMember m = memberList[i];
          if (m != null) {
            s.add(m);
          }
        }
      }
    }
  }
  
  @Override
  public void process(DistributionMessage msg) {
    final LogWriterI18n log = this.system.getLogWriter().convertToLogWriterI18n();
    super.process(msg);
    if (log.fineEnabled()) {
      log.fine(this.toString() + " done processing " + msg + " from " +
               msg.getSender());
    }
  }

  @Override
  protected void preWait() {
    this.waiting = true;
    DM mgr = getDistributionManager();
    this.statStart = mgr.getStats().startReplyWait();
    // Note we do not use addMembershipListenerAndGetDistributionManagerIds
    // because this is the startup message and we do not yet have any
    // members in the dm's list.
    mgr.addMembershipListener(this);
//     Set activeMembers = mgr.addMembershipListenerAndGetDistributionManagerIds(this);
//     synchronized (this.members) {
//       for (int i = 0; i < getMembers().length; i++) {
//         if (!activeMembers.contains(getMembers()[i])) {
//           memberDeparted(getMembers()[i], false);
//         }
//       }
//     }
  }
  
  /** overridden from ReplyProcessor21 to allow early-out. 
   * If an existing member accepted or rejected us then we are done.
   */
  @Override
  protected boolean canStopWaiting() {
    return this.receivedAcceptance || this.receivedRejectionMessage;
  }
  
  
}
