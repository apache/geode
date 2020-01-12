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

import java.util.Set;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class StartupMessageReplyProcessor extends ReplyProcessor21 {
  /** has a rejection message (license mismatch, etc) been received? */
  private boolean receivedRejectionMessage;

  /**
   * set to true once we receive a reply from someone who accepted us into the group. Note that we
   * receive replies from admin dm but they do not have the authority to accept us into the group.
   */
  private boolean receivedAcceptance;
  private DistributionManager dm;

  public StartupMessageReplyProcessor(DistributionManager dm, Set recipients) {
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
  void collectUnresponsiveMembers(Set<InternalDistributedMember> s) {
    if (stillWaiting()) {
      InternalDistributedMember[] memberList = getMembers();
      synchronized (memberList) {
        for (InternalDistributedMember m : memberList) {
          if (m != null) {
            s.add(m);
          }
        }
      }
    }
  }

  @Override
  public void process(DistributionMessage msg) {
    final LogWriter log = this.system.getLogWriter();
    super.process(msg);
    if (log.fineEnabled()) {
      log.fine(this.toString() + " done processing " + msg + " from " + msg.getSender());
    }
  }

  @Override
  protected void preWait() {
    this.waiting = true;
    DistributionManager mgr = getDistributionManager();
    this.statStart = mgr.getStats().startReplyWait();
    // Note we do not use addMembershipListenerAndGetDistributionManagerIds
    // because this is the startup message and we do not yet have any
    // members in the dm's list.
    mgr.addMembershipListener(this);
    // Set activeMembers = mgr.addMembershipListenerAndGetDistributionManagerIds(this);
    // synchronized (this.members) {
    // for (int i = 0; i < getMembers().length; i++) {
    // if (!activeMembers.contains(getMembers()[i])) {
    // memberDeparted(getMembers()[i], false);
    // }
    // }
    // }
  }
}
