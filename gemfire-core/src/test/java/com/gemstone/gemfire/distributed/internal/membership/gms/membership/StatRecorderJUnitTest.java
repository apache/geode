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
package com.gemstone.gemfire.distributed.internal.membership.gms.membership;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.UNICAST3.Header;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.LonerDistributionManager.DummyDMStats;
import com.gemstone.gemfire.distributed.internal.membership.gms.ServiceConfig;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import com.gemstone.gemfire.distributed.internal.membership.gms.messenger.StatRecorder;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * This class tests the GMS StatRecorder class, which records JGroups
 * messaging statistics
 */
@Category(UnitTest.class)
public class StatRecorderJUnitTest {
  Protocol mockDownProtocol, mockUpProtocol;
  StatRecorder recorder;
  MyStats stats = new MyStats();
  
  @Before
  public void initMocks() throws Exception {
    // create a StatRecorder that has mock up/down protocols and stats
    mockDownProtocol = mock(Protocol.class);
    mockUpProtocol = mock(Protocol.class);
    recorder = new StatRecorder();
    recorder.setDMStats(stats);
    recorder.setUpProtocol(mockUpProtocol);
    recorder.setDownProtocol(mockDownProtocol);
  }
  
  /**
   * Ensure that unicast events are recorded in DMStats
   */
  @Test
  public void testUnicastStats() throws Exception {
    Message msg = mock(Message.class);
    when(msg.getHeader(any(Short.class))).thenReturn(Header.createDataHeader(1L, (short)1, true));
    when(msg.size()).thenReturn(150L);
    
    Event evt = new Event(Event.MSG, msg);
    recorder.up(evt);
    assertTrue("stats.ucastMessagesReceived =" + stats.ucastMessagesReceived,
        stats.ucastMessagesReceived == 1);
    assertEquals(stats.ucastMessageBytesReceived, 150);
    
    recorder.down(evt);
    assertTrue("stats.ucastMessagesSent =" + stats.ucastMessagesSent,
        stats.ucastMessagesSent == 1);
    assertEquals(stats.ucastMessageBytesSent, 150);
    
    msg = mock(Message.class);
    when(msg.getHeader(any(Short.class))).thenReturn(Header.createXmitReqHeader());
    when(msg.size()).thenReturn(150L);
    evt = new Event(Event.MSG, msg);
    recorder.down(evt);
    assertTrue("stats.ucastRetransmits =" + stats.ucastRetransmits,
        stats.ucastRetransmits == 1);
  }

  /**
   * ensure that multicast events are recorded in DMStats
   */
  @Test
  public void testMulticastStats() throws Exception {
    Message msg = mock(Message.class);
    when(msg.getHeader(any(Short.class))).thenReturn(NakAckHeader2.createMessageHeader(1L));
    when(msg.size()).thenReturn(150L);
    
    Event evt = new Event(Event.MSG, msg);
    recorder.up(evt);
    assertTrue("mcastMessagesReceived = " + stats.mcastMessagesReceived,
        stats.mcastMessagesReceived == 1);
    assertEquals(stats.mcastMessageBytesReceived, 150);
    
    recorder.down(evt);
    assertTrue("mcastMessagesSent = " + stats.mcastMessagesSent,
        stats.mcastMessagesSent == 1);
    assertEquals(stats.mcastMessageBytesSent, 150);
    
    msg = mock(Message.class);
    when(msg.size()).thenReturn(150L);
    when(msg.getHeader(any(Short.class))).thenReturn(NakAckHeader2.createXmitRequestHeader(null));
    evt = new Event(Event.MSG, msg);
    recorder.down(evt);
    assertTrue("mcastRetransmitRequests = " + stats.mcastRetransmitRequests,
        stats.mcastRetransmitRequests == 1);

    msg = mock(Message.class);
    when(msg.size()).thenReturn(150L);
    when(msg.getHeader(any(Short.class))).thenReturn(NakAckHeader2.createXmitResponseHeader());
    evt = new Event(Event.MSG, msg);
    recorder.down(evt);
    assertTrue("mcastRetransmits = " + stats.mcastRetransmits,
        stats.mcastRetransmits == 1);
  }
  
  
  /**
   * Ensure that the messenger JGroups configuration XML strings contain
   * the statistics recorder protocol
   */
  @Test
  public void messengerStackHoldsStatRecorder() throws Exception {
    Services mockServices = mock(Services.class);
    ServiceConfig mockConfig = mock(ServiceConfig.class);
    when(mockServices.getConfig()).thenReturn(mockConfig);
    
    // first test to see if the non-multicast stack has the recorder installed
    Properties nonDefault = new Properties();
    nonDefault.put(DistributionConfig.MCAST_PORT_NAME, "0");
    nonDefault.put(DistributionConfig.LOCATORS_NAME, "localhost[12345]");
    DistributionConfigImpl config = new DistributionConfigImpl(nonDefault);
    when(mockConfig.getDistributionConfig()).thenReturn(config);

    RemoteTransportConfig transport = new RemoteTransportConfig(config,
        DistributionManager.NORMAL_DM_TYPE);
    when(mockConfig.getTransport()).thenReturn(transport);

    JGroupsMessenger messenger = new JGroupsMessenger();
    messenger.init(mockServices);
    String jgroupsConfig = messenger.getJGroupsStackConfig();
    System.out.println(jgroupsConfig);
    assertTrue(jgroupsConfig.contains("gms.messenger.StatRecorder"));
    
    // now test to see if the multicast stack has the recorder installed
    nonDefault.put(DistributionConfig.MCAST_PORT_NAME, "12345");
    config = new DistributionConfigImpl(nonDefault);
    transport = new RemoteTransportConfig(config, DistributionManager.NORMAL_DM_TYPE);
    when(mockConfig.getDistributionConfig()).thenReturn(config);
    when(mockConfig.getTransport()).thenReturn(transport);
    messenger = new JGroupsMessenger();
    messenger.init(mockServices);
    assertTrue(jgroupsConfig.contains("gms.messenger.StatRecorder"));
  }

  static class MyStats extends DummyDMStats {
    public int ucastMessagesReceived;
    public int ucastMessageBytesReceived;
    public int ucastMessagesSent;
    public int ucastMessageBytesSent;
    public int ucastRetransmits;
    
    public int mcastMessagesReceived;
    public int mcastMessageBytesReceived;
    public int mcastMessagesSent;
    public int mcastMessageBytesSent;
    public int mcastRetransmits;
    public int mcastRetransmitRequests;
    
    @Override
    public void incUcastReadBytes(int i) {
      ucastMessagesReceived++;
      ucastMessageBytesReceived += i;
    }
    
    @Override
    public void incUcastWriteBytes(int i) {
      ucastMessagesSent++;
      ucastMessageBytesSent += i;
    }
    
    @Override
    public void incUcastRetransmits() {
      ucastRetransmits++;
    }

    @Override
    public void incMcastReadBytes(int i) {
      mcastMessagesReceived++;
      mcastMessageBytesReceived += i;
    }
    
    @Override
    public void incMcastWriteBytes(int i) {
      mcastMessagesSent++;
      mcastMessageBytesSent += i;
    }
    
    @Override
    public void incMcastRetransmits() {
      mcastRetransmits++;
    }
  
    @Override
    public void incMcastRetransmitRequests() {
      mcastRetransmitRequests++;
    }
  }

}
