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
package org.apache.geode.distributed.internal.membership.gms.membership;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.LonerDistributionManager.DummyDMStats;
import org.apache.geode.distributed.internal.membership.gms.ServiceConfig;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.messenger.JGroupsMessenger;
import org.apache.geode.distributed.internal.membership.gms.messenger.StatRecorder;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.test.junit.categories.UnitTest;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.UNICAST3.Header;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

/**
 * This class tests the GMS StatRecorder class, which records JGroups
 * messaging statistics
 */
@Category(UnitTest.class)
public class StatRecorderJUnitTest {

  private Protocol mockDownProtocol;
  private Protocol mockUpProtocol;
  private StatRecorder recorder;
  private MyStats stats;
  private Services services;
  
  @Before
  public void setUp() throws Exception {
    stats = new MyStats();

    // create a StatRecorder that has mock up/down protocols and stats
    mockDownProtocol = mock(Protocol.class);
    mockUpProtocol = mock(Protocol.class);

    services = mock(Services.class);
    when(services.getStatistics()).thenReturn(stats);

    recorder = new StatRecorder();
    recorder.setServices(services);
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


  @Test
  public void recorderHandlesRejectedExecution() throws Exception {
    Message msg = mock(Message.class);
    when(msg.getHeader(any(Short.class))).thenReturn(Header.createDataHeader(1L, (short)1, true));
    when(msg.size()).thenReturn(150L);

    // GEODE-1178, the TP protocol may throw a RejectedExecutionException & StatRecorder should retry
    when(mockDownProtocol.down(any(Event.class))).thenThrow(new RejectedExecutionException());

    // after the first down() throws an exception we want StatRecorder to retry, so
    // we set the Manager to say no shutdown is in progress the first time and then say
    // one IS in progress so we can break out of the StatRecorder exception handling loop
    when(services.getCancelCriterion()).thenReturn(new Services().getCancelCriterion());
    Manager manager = mock(Manager.class);
    when(services.getManager()).thenReturn(manager);
    when(manager.shutdownInProgress()).thenReturn(Boolean.FALSE, Boolean.TRUE);

    verify(mockDownProtocol, never()).down(isA(Event.class));

    Event evt = new Event(Event.MSG, msg);
    recorder.down(evt);

    verify(mockDownProtocol, times(2)).down(isA(Event.class));
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
    nonDefault.put(MCAST_PORT, "0");
    nonDefault.put(LOCATORS, "localhost[12345]");
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
    nonDefault.put(MCAST_PORT, "12345");
    config = new DistributionConfigImpl(nonDefault);
    transport = new RemoteTransportConfig(config, DistributionManager.NORMAL_DM_TYPE);
    when(mockConfig.getDistributionConfig()).thenReturn(config);
    when(mockConfig.getTransport()).thenReturn(transport);
    messenger = new JGroupsMessenger();
    messenger.init(mockServices);
    assertTrue(jgroupsConfig.contains("gms.messenger.StatRecorder"));
  }

  private static class MyStats extends DummyDMStats {

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
