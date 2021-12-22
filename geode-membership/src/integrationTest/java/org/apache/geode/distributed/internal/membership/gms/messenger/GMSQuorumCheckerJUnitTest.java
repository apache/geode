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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.util.MemberIdentifierUtil;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class GMSQuorumCheckerJUnitTest {

  private MemberIdentifier[] mockMembers;
  private JChannel channel;
  private JGAddress address;

  @Before
  public void initMocks() {
    mockMembers = new MemberIdentifier[12];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = MemberIdentifierUtil.createMemberID(8888 + i);
    }
    channel = mock(JChannel.class);
    address = mock(JGAddress.class);
    when(channel.getAddress()).thenReturn(new UUID());
    when(channel.down(any(Event.class))).thenReturn(mock(IpAddress.class));
    Mockito.doCallRealMethod().when(channel).setReceiver(any(Receiver.class));
    when(channel.getReceiver()).thenCallRealMethod();
    Mockito.doReturn(address).when(channel).down(any(Event.class));
  }

  @Test
  public void testQuorumCheckerAllRespond() throws Exception {
    GMSMembershipView view = prepareView();
    Set<Integer> pongResponders = new HashSet<>();
    for (final MemberIdentifier mockMember : mockMembers) {
      pongResponders.add(mockMember.getMembershipPort());
    }
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertTrue(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
    assertTrue(qc.checkForQuorum(500));
    assertSame(MembershipInformationImpl.class, qc.getMembershipInfo().getClass());
    assertSame(((MembershipInformationImpl) qc.getMembershipInfo()).getChannel(), channel);
  }

  @Test
  public void testQuorumCheckerMajorityRespond() throws Exception {
    GMSMembershipView view = prepareView();
    Set<Integer> pongResponders = new HashSet<>();
    for (int i = 0; i < mockMembers.length - 1; i++) {
      pongResponders.add(mockMembers[i].getMembershipPort());
    }
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertTrue(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
  }

  @Test
  public void testQuorumCheckerNotEnoughWeightForQuorum() throws Exception {
    GMSMembershipView view = prepareView();
    Set<Integer> pongResponders = new HashSet<>();
    pongResponders.add(mockMembers[0].getMembershipPort());
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertFalse(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
  }

  @Test
  public void testQuorumCheckerNoQuorumNoResponders() throws Exception {
    GMSMembershipView view = prepareView();
    Set<Integer> pongResponders = new HashSet<>();
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertFalse(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
  }

  @Test
  public void testQuorumChecker10Servers2Locators4ServersLost() throws Exception {
    GMSMembershipView view = prepareView();
    mockMembers[0].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);

    Set<Integer> pongResponders = new HashSet<>();
    for (final MemberIdentifier mockMember : mockMembers) {
      pongResponders.add(mockMember.getMembershipPort());
    }

    // remove 4 servers
    pongResponders.remove(mockMembers[8].getMembershipPort());
    pongResponders.remove(mockMembers[9].getMembershipPort());
    pongResponders.remove(mockMembers[10].getMembershipPort());
    pongResponders.remove(mockMembers[11].getMembershipPort());

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertTrue(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
  }

  @Test
  public void testQuorumChecker10Servers2Locators4ServersAnd1LocatorLost() throws Exception {
    GMSMembershipView view = prepareView();
    mockMembers[0].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);

    Set<Integer> pongResponders = new HashSet<>();
    for (final MemberIdentifier mockMember : mockMembers) {
      pongResponders.add(mockMember.getMembershipPort());
    }

    // remove 4 servers
    pongResponders.remove(mockMembers[8].getMembershipPort());
    pongResponders.remove(mockMembers[9].getMembershipPort());
    pongResponders.remove(mockMembers[10].getMembershipPort());
    pongResponders.remove(mockMembers[11].getMembershipPort());

    // remove 1 locator
    pongResponders.remove(mockMembers[1].getMembershipPort());

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertTrue(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
  }

  @Test
  public void testQuorumChecker10Servers2Locators5ServersAnd2LocatorsButNotLeadMemberLost()
      throws Exception {
    GMSMembershipView view = prepareView();
    mockMembers[0].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);

    Set<Integer> pongResponders = new HashSet<>();
    for (final MemberIdentifier mockMember : mockMembers) {
      pongResponders.add(mockMember.getMembershipPort());
    }

    // remove 5 servers
    pongResponders.remove(mockMembers[7].getMembershipPort());
    pongResponders.remove(mockMembers[8].getMembershipPort());
    pongResponders.remove(mockMembers[9].getMembershipPort());
    pongResponders.remove(mockMembers[10].getMembershipPort());
    pongResponders.remove(mockMembers[11].getMembershipPort());

    // remove locators
    pongResponders.remove(mockMembers[0].getMembershipPort());
    pongResponders.remove(mockMembers[1].getMembershipPort());

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertFalse(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
  }

  @Test
  public void testQuorumChecker10Servers2Locators5ServerAnd1LocatorWithLeadMemberLost()
      throws Exception {
    GMSMembershipView view = prepareView();
    mockMembers[0].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);

    Set<Integer> pongResponders = new HashSet<>();
    for (final MemberIdentifier mockMember : mockMembers) {
      pongResponders.add(mockMember.getMembershipPort());
    }
    // remove 5 servers
    pongResponders.remove(mockMembers[2].getMembershipPort()); // lead member
    pongResponders.remove(mockMembers[8].getMembershipPort());
    pongResponders.remove(mockMembers[9].getMembershipPort());
    pongResponders.remove(mockMembers[10].getMembershipPort());
    pongResponders.remove(mockMembers[11].getMembershipPort());

    // remove locator
    pongResponders.remove(mockMembers[0].getMembershipPort());

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertFalse(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
  }

  @Test
  public void testQuorumChecker2Servers2LocatorsLeadMemberLost() throws Exception {
    int numMembers = 4;
    GMSMembershipView view = prepareView(numMembers);
    mockMembers[0].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);

    Set<Integer> pongResponders = new HashSet<>();
    for (int i = 0; i < numMembers; i++) {
      pongResponders.add(mockMembers[i].getMembershipPort());
    }
    // remove lead member
    pongResponders.remove(mockMembers[2].getMembershipPort()); // lead member

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertTrue(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
  }

  @Test
  public void testQuorumChecker2Servers2LocatorsLeadMemberAnd1LocatorLost() throws Exception {
    int numMembers = 4;
    GMSMembershipView view = prepareView(numMembers);
    mockMembers[0].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(MemberIdentifier.LOCATOR_DM_TYPE);

    Set<Integer> pongResponders = new HashSet<>();
    for (int i = 0; i < numMembers; i++) {
      pongResponders.add(mockMembers[i].getMembershipPort());
    }
    // remove members
    pongResponders.remove(mockMembers[2].getMembershipPort()); // lead member
    pongResponders.remove(mockMembers[0].getMembershipPort()); // locator

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel, null);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    assertFalse(quorum);
    assertSame(view.getMembers().size(), answerer.getPingCount());
  }

  private GMSMembershipView prepareView() {
    return prepareView(mockMembers.length);
  }

  private GMSMembershipView prepareView(int numMembers) {
    int viewId = 1;
    List<MemberIdentifier> mbrs = new LinkedList<>();
    for (int i = 0; i < numMembers; i++) {
      mbrs.add(mockMembers[i]);
    }

    // prepare the view
    GMSMembershipView netView = new GMSMembershipView(mockMembers[0], viewId, mbrs);
    return netView;
  }

  private static class PingMessageAnswer implements Answer {

    private int pingCount = 0;
    private final JChannel channel;
    private final GMSPingPonger pingPonger = new GMSPingPonger();
    private final Set<Integer> simulatedPongRespondersByPort;

    public PingMessageAnswer(JChannel channel, Set<Integer> simulatedPongRespondersByPort) {
      this.channel = channel;
      this.simulatedPongRespondersByPort = simulatedPongRespondersByPort;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      Object[] args = invocation.getArguments();
      for (final Object arg : args) {
        if (arg instanceof Message) {
          Message msg = (Message) arg;
          Object content = null;
          content = msg.getBuffer();
          if (content instanceof byte[]) {
            if (pingPonger.isPingMessage((byte[]) content)) {
              pingCount++;
              if (simulatedPongRespondersByPort.contains(((JGAddress) msg.getDest()).getPort())) {
                channel.getReceiver()
                    .receive(pingPonger.createPongMessage(msg.getDest(), msg.getSrc()));
              }
            }
          }
        }
      }
      return null;
    }

    public int getPingCount() {
      return pingCount;
    }
  }
}
