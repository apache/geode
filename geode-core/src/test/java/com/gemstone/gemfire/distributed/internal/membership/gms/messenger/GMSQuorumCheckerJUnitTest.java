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
package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.UnknownHostException;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GMSQuorumCheckerJUnitTest {

  private InternalDistributedMember[] mockMembers;

  private Services services;

  private JChannel channel;
  
  private JGAddress address;

  private class PingMessageAnswer implements Answer {
    private int pingCount = 0;
    private JChannel channel;
    private GMSPingPonger pingPonger = new GMSPingPonger();
    private Set<Integer> simulatedPongRespondersByPort;
    
    public PingMessageAnswer(JChannel channel, Set<Integer> simulatedPongRespondersByPort) {
      this.channel = channel;
      this.simulatedPongRespondersByPort = simulatedPongRespondersByPort;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      Object[] args = invocation.getArguments();
      for (int i = 0; i < args.length; i++) {
        if (args[i] instanceof Message) {
          Message msg = (Message) args[i];
          Object content = null;
          content = msg.getBuffer();
          if (content instanceof byte[]) {
            if (pingPonger.isPingMessage((byte[]) content)) {
              pingCount++;              
              if (simulatedPongRespondersByPort.contains(((JGAddress)msg.getDest()).getPort())) {
                channel.getReceiver().receive(pingPonger.createPongMessage(msg.getDest(), msg.getSrc()));
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

  @Before
  public void initMocks() throws UnknownHostException, Exception {
    mockMembers = new InternalDistributedMember[12];
    for (int i = 0; i < mockMembers.length; i++) {
      mockMembers[i] = new InternalDistributedMember("localhost", 8888 + i);
    }
    channel = mock(JChannel.class);
    address = mock(JGAddress.class);
    when(channel.getAddress()).thenReturn(new UUID());
    when(channel.down(any(Event.class))).thenReturn(mock(IpAddress.class));
    Mockito.doCallRealMethod().when(channel).setReceiver(any(Receiver.class));
    when(channel.getReceiver()).thenCallRealMethod();
    Mockito.doReturn(address).when(channel).down(any(Event.class));
  }
  
  private NetView prepareView() throws IOException {
    return prepareView(mockMembers.length);
  }

  private NetView prepareView(int numMembers) throws IOException {
    int viewId = 1;
    List<InternalDistributedMember> mbrs = new LinkedList<InternalDistributedMember>();
    for (int i = 0; i < numMembers; i++) {
      mbrs.add(mockMembers[i]);
    }

    // prepare the view
    NetView netView = new NetView(mockMembers[0], viewId, mbrs);
    return netView;
  }

  @Test
  public void testQuorumCheckerAllRespond() throws Exception {
    NetView view = prepareView();
    Set<Integer> pongResponders = new HashSet<Integer>();
    for (int i = 0; i < mockMembers.length; i++) {
      pongResponders.add(mockMembers[i].getPort());
    }
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertTrue(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
    Assert.assertTrue(qc.checkForQuorum(500));
    Assert.assertSame(qc.getMembershipInfo(), channel);
  }
  
  @Test
  public void testQuorumCheckerMajorityRespond() throws Exception {
    NetView view = prepareView();
    Set<Integer> pongResponders = new HashSet<Integer>();
    for (int i = 0; i < mockMembers.length - 1; i++) {
      pongResponders.add(mockMembers[i].getPort());
    }
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertTrue(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
  }
  
  @Test
  public void testQuorumCheckerNotEnoughWeightForQuorum() throws Exception {
    NetView view = prepareView();
    Set<Integer> pongResponders = new HashSet<Integer>();
    pongResponders.add(mockMembers[0].getPort());
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertFalse(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
  }
  
  @Test
  public void testQuorumCheckerNoQuorumNoResponders() throws Exception {
    NetView view = prepareView();
    Set<Integer> pongResponders = new HashSet<Integer>();
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);    
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertFalse(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
  }
  
  @Test
  public void testQuorumChecker10Servers2Locators4ServersLost() throws Exception {
    NetView view = prepareView();
    mockMembers[0].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    
    Set<Integer> pongResponders = new HashSet<Integer>();
    for (int i = 0; i < mockMembers.length; i++) {
      pongResponders.add(mockMembers[i].getPort());
    }
    //remove 4 servers
    pongResponders.remove(mockMembers[8].getPort());
    pongResponders.remove(mockMembers[9].getPort());
    pongResponders.remove(mockMembers[10].getPort());
    pongResponders.remove(mockMembers[11].getPort());
    
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertTrue(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
  }
  
  @Test
  public void testQuorumChecker10Servers2Locators4ServersAnd1LocatorLost() throws Exception {
    NetView view = prepareView();
    mockMembers[0].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    
    Set<Integer> pongResponders = new HashSet<Integer>();
    for (int i = 0; i < mockMembers.length; i++) {
      pongResponders.add(mockMembers[i].getPort());
    }
    //remove 4 servers
    pongResponders.remove(mockMembers[8].getPort());
    pongResponders.remove(mockMembers[9].getPort());
    pongResponders.remove(mockMembers[10].getPort());
    pongResponders.remove(mockMembers[11].getPort());
    //remove 1 locator
    pongResponders.remove(mockMembers[1].getPort());

    
    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertTrue(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
  }
  
  @Test
  public void testQuorumChecker10Servers2Locators5ServersAnd2LocatorsButNotLeadMemberLost() throws Exception {
    NetView view = prepareView();
    mockMembers[0].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    
    Set<Integer> pongResponders = new HashSet<Integer>();
    for (int i = 0; i < mockMembers.length; i++) {
      pongResponders.add(mockMembers[i].getPort());
    }
    //remove 5 servers
    pongResponders.remove(mockMembers[7].getPort());
    pongResponders.remove(mockMembers[8].getPort());
    pongResponders.remove(mockMembers[9].getPort());
    pongResponders.remove(mockMembers[10].getPort());
    pongResponders.remove(mockMembers[11].getPort());
    //remove locators
    pongResponders.remove(mockMembers[0].getPort());
    pongResponders.remove(mockMembers[1].getPort());

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertFalse(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
  }
  
  @Test
  public void testQuorumChecker10Servers2Locators5ServerAnd1LocatorWithLeadMemberLost() throws Exception {
    NetView view = prepareView();
    mockMembers[0].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    
    Set<Integer> pongResponders = new HashSet<Integer>();
    for (int i = 0; i < mockMembers.length; i++) {
      pongResponders.add(mockMembers[i].getPort());
    }
    //remove 5 servers
    pongResponders.remove(mockMembers[2].getPort()); //lead member
    pongResponders.remove(mockMembers[8].getPort());
    pongResponders.remove(mockMembers[9].getPort());
    pongResponders.remove(mockMembers[10].getPort());
    pongResponders.remove(mockMembers[11].getPort());
    
    //remove locator
    pongResponders.remove(mockMembers[0].getPort());

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertFalse(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
  }
  
  @Test
  public void testQuorumChecker2Servers2LocatorsLeadMemberLost() throws Exception {
    int numMembers = 4;
    NetView view = prepareView(numMembers);
    mockMembers[0].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    
    Set<Integer> pongResponders = new HashSet<Integer>();
    for (int i = 0; i < numMembers; i++) {
      pongResponders.add(mockMembers[i].getPort());
    }
    //remove lead member
    pongResponders.remove(mockMembers[2].getPort()); //lead member

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertTrue(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
  }
  
  @Test
  public void testQuorumChecker2Servers2LocatorsLeadMemberAnd1LocatorLost() throws Exception {
    int numMembers = 4;
    NetView view = prepareView(numMembers);
    mockMembers[0].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    mockMembers[1].setVmKind(DistributionManager.LOCATOR_DM_TYPE);
    
    Set<Integer> pongResponders = new HashSet<Integer>();
    for (int i = 0; i < numMembers; i++) {
      pongResponders.add(mockMembers[i].getPort());
    }
    //remove members
    pongResponders.remove(mockMembers[2].getPort()); //lead member
    pongResponders.remove(mockMembers[0].getPort()); //locator

    PingMessageAnswer answerer = new PingMessageAnswer(channel, pongResponders);
    Mockito.doAnswer(answerer).when(channel).send(any(Message.class));

    GMSQuorumChecker qc = new GMSQuorumChecker(view, 51, channel);
    qc.initialize();
    boolean quorum = qc.checkForQuorum(500);
    Assert.assertFalse(quorum);
    Assert.assertSame(view.getMembers().size(), answerer.getPingCount());
  }
  
}
