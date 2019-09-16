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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.View;

import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.logging.LogService;

public class GMSQuorumChecker {
  private static final Logger logger = LogService.getLogger();
  private boolean isInfoEnabled = false;
  private Map<SocketAddress, GMSMember> addressConversionMap;
  private GMSPingPonger pingPonger;

  private Set<GMSMember> receivedAcks;

  private final GMSMembershipView lastView;

  // guarded by this
  private boolean quorumAchieved = false;
  private final JChannel channel;
  private JGAddress myAddress;
  private final long partitionThreshold;
  private Set<GMSMember> oldMemberIdentifiers;
  private ConcurrentLinkedQueue<Message> messageQueue = new ConcurrentLinkedQueue<>();

  public GMSQuorumChecker(GMSMembershipView jgView, int partitionThreshold, JChannel channel,
      Set<GMSMember> oldMemberIdentifiers) {
    this.lastView = jgView;
    this.partitionThreshold = partitionThreshold;
    this.channel = channel;
    this.oldMemberIdentifiers = oldMemberIdentifiers;
  }

  public void initialize() {
    receivedAcks = new ConcurrentHashSet<>();

    pingPonger = new GMSPingPonger();
    myAddress = (JGAddress) channel.down(new Event(Event.GET_LOCAL_ADDRESS));

    addressConversionMap = new ConcurrentHashMap<>(this.lastView.size());
    List<GMSMember> members = this.lastView.getMembers();
    for (GMSMember addr : members) {
      SocketAddress sockaddr =
          new InetSocketAddress(addr.getInetAddress(), addr.getPort());
      addressConversionMap.put(sockaddr, addr);
    }

    isInfoEnabled = logger.isInfoEnabled();
    resume();
  }


  public synchronized boolean checkForQuorum(long timeout) throws InterruptedException {
    if (quorumAchieved) {
      return true;
    }

    if (isInfoEnabled) {
      logger.info("beginning quorum check with {}", this);
    }
    sendPingMessages();
    quorumAchieved = waitForResponses(lastView.getMembers().size(), timeout);
    // If we did not achieve full quorum, calculate if we achieved quorum
    if (!quorumAchieved) {
      quorumAchieved = calculateQuorum();
    }
    return quorumAchieved;
  }


  public void suspend() {
    // NO-OP for this implementation
  }


  public void close() {
    if (channel != null && !channel.isClosed()) {
      channel.close();
    }
  }


  public void resume() {
    channel.setReceiver(null);
    channel.setReceiver(new QuorumCheckerReceiver());
  }


  public MembershipInformation getMembershipInfo() {
    return new MembershipInformation(channel, oldMemberIdentifiers, messageQueue);
  }

  private boolean calculateQuorum() {
    // quorum check
    int weight = getWeight(this.lastView.getMembers(), this.lastView.getLeadMember());
    int ackedWeight = getWeight(receivedAcks, this.lastView.getLeadMember());
    int lossThreshold = (int) Math.round((weight * this.partitionThreshold) / 100.0);
    if (isInfoEnabled) {
      logger.info(
          "quorum check: contacted {} processes with {} member weight units.  Threshold for a quorum is {}",
          receivedAcks.size(), ackedWeight, lossThreshold);
    }
    return (ackedWeight >= lossThreshold);
  }

  private boolean waitForResponses(int numMembers, long timeout) throws InterruptedException {
    long endTime = System.currentTimeMillis() + timeout;
    for (;;) {
      long time = System.currentTimeMillis();
      long remaining = (endTime - time);
      if (remaining <= 0) {
        if (isInfoEnabled) {
          logger.info("quorum check: timeout waiting for responses.  {} responses received",
              receivedAcks.size());
        }
        break;
      }
      if (isInfoEnabled) {
        logger.info("quorum check: waiting up to {}ms to receive a quorum of responses",
            remaining);
      }
      Thread.sleep(500);
      if (receivedAcks.size() == numMembers) {
        // we've heard from everyone now so we've got a quorum
        if (isInfoEnabled) {
          logger.info(
              "quorum check: received responses from all members that were in the old distributed system");
        }
        return true;
      }
    }
    return false;
  }

  private int getWeight(Collection<GMSMember> idms,
      GMSMember leader) {
    int weight = 0;
    for (GMSMember mbr : idms) {
      int thisWeight = mbr.getMemberWeight();
      if (mbr.getVmKind() == 10 /* NORMAL_DM_KIND */) {
        thisWeight += 10;
        if (leader != null && mbr.equals(leader)) {
          thisWeight += 5;
        }
      } else if (mbr.preferredForCoordinator()) {
        thisWeight += 3;
      }
      weight += thisWeight;
    }
    return weight;
  }

  private void sendPingMessages() {
    // send a ping message to each member in the last view seen
    List<GMSMember> members = this.lastView.getMembers();
    for (GMSMember addr : members) {
      if (!receivedAcks.contains(addr)) {
        JGAddress dest = new JGAddress(addr);
        if (isInfoEnabled) {
          logger.info("quorum check: sending request to {}", addr);
        }
        try {
          pingPonger.sendPingMessage(channel, myAddress, dest);
        } catch (Exception e) {
          logger.info("Failed sending Ping message to " + dest);
        }
      }
    }
  }

  private class QuorumCheckerReceiver implements Receiver {

    @Override
    public void receive(Message msg) {
      byte[] msgBytes = msg.getBuffer();
      if (pingPonger.isPingMessage(msgBytes)) {
        try {
          pingPonger.sendPongMessage(channel, myAddress, msg.getSrc());
        } catch (Exception e) {
          logger.info("Failed sending Pong message to " + msg.getSrc());
        }
      } else if (pingPonger.isPongMessage(msgBytes)) {
        pongReceived(msg.getSrc());
      } else {
        queueMessage(msg);
      }
    }

    private void queueMessage(Message msg) {
      messageQueue.add(msg);
    }

    @Override
    public void getState(OutputStream output) throws Exception {}

    @Override
    public void setState(InputStream input) throws Exception {}

    @Override
    public void viewAccepted(View new_view) {}

    @Override
    public void suspect(Address suspected_mbr) {}

    @Override
    public void block() {}

    @Override
    public void unblock() {}

    public void pongReceived(Address sender) {
      logger.info("received ping-pong response from {}", sender);
      JGAddress jgSender = (JGAddress) sender;
      SocketAddress sockaddr = new InetSocketAddress(jgSender.getInetAddress(), jgSender.getPort());
      GMSMember memberAddr = addressConversionMap.get(sockaddr);

      if (memberAddr != null) {
        logger.info("quorum check: mapped address to member ID {}", memberAddr);
        receivedAcks.add(memberAddr);
      }
    }
  }

  public String toString() {
    return getClass().getSimpleName() + " on view " + this.lastView;
  }

}
