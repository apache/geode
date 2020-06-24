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

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.RejectedExecutionException;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.protocols.UNICAST3.Header;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.MembershipStatistics;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * This class tests the GMS StatRecorder class, which records JGroups messaging statistics
 */
@Category({MembershipTest.class})
public class StatRecorderJUnitTest {

  private Protocol mockDownProtocol;
  private Protocol mockUpProtocol;
  private StatRecorder recorder;
  private MembershipStatistics stats;
  private Services services;

  @Before
  public void setUp() throws Exception {
    stats = mock(MembershipStatistics.class);

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
    when(msg.getHeader(any(Short.class))).thenReturn(Header.createDataHeader(1L, (short) 1, true));
    when(msg.size()).thenReturn(150L);

    Event evt = new Event(Event.MSG, msg);
    recorder.up(evt);
    verify(stats).incUcastReadBytes(150);

    recorder.down(evt);
    verify(stats).incUcastWriteBytes(150);

    msg = mock(Message.class);
    when(msg.getHeader(any(Short.class))).thenReturn(Header.createXmitReqHeader());
    when(msg.size()).thenReturn(150L);
    evt = new Event(Event.MSG, msg);
    recorder.down(evt);
    verify(stats).incUcastRetransmits();
  }


  @Test
  public void recorderHandlesRejectedExecution() throws Exception {
    Message msg = mock(Message.class);
    when(msg.getHeader(any(Short.class))).thenReturn(Header.createDataHeader(1L, (short) 1, true));
    when(msg.size()).thenReturn(150L);

    // GEODE-1178, the TP protocol may throw a RejectedExecutionException & StatRecorder should
    // retry
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
    verify(stats).incMcastReadBytes(150);

    recorder.down(evt);
    verify(stats).incMcastWriteBytes(150);

    msg = mock(Message.class);
    when(msg.size()).thenReturn(150L);
    when(msg.getHeader(any(Short.class))).thenReturn(NakAckHeader2.createXmitRequestHeader(null));
    evt = new Event(Event.MSG, msg);
    recorder.down(evt);
    verify(stats).incMcastRetransmitRequests();

    msg = mock(Message.class);
    when(msg.size()).thenReturn(150L);
    when(msg.getHeader(any(Short.class))).thenReturn(NakAckHeader2.createXmitResponseHeader());
    evt = new Event(Event.MSG, msg);
    recorder.down(evt);
    verify(stats).incMcastRetransmits();
  }

  /**
   * Ensure that the messenger JGroups configuration XML strings contain the statistics recorder
   * protocol
   */
  @Test
  public void messengerStackHoldsStatRecorder() throws Exception {
    Services mockServices = mock(Services.class);

    // first test to see if the non-multicast stack has the recorder installed
    MembershipConfig mockConfig = mock(MembershipConfig.class);
    when(mockConfig.getMembershipPortRange()).thenReturn(new int[] {0, 10});
    when(mockConfig.getSecurityUDPDHAlgo()).thenReturn("");
    when(mockServices.getConfig()).thenReturn(mockConfig);


    JGroupsMessenger messenger = new JGroupsMessenger();
    messenger.init(mockServices, new ServiceLoaderModuleService(LogService.getLogger()));
    String jgroupsConfig = messenger.jgStackConfig;
    System.out.println(jgroupsConfig);
    assertTrue(jgroupsConfig.contains("gms.messenger.StatRecorder"));

    // now test to see if the multicast stack has the recorder installed
    when(mockConfig.getMcastPort()).thenReturn(12345);
    when(mockServices.getConfig()).thenReturn(mockConfig);


    messenger = new JGroupsMessenger();
    messenger.init(mockServices, new ServiceLoaderModuleService(LogService.getLogger()));
    assertTrue(jgroupsConfig.contains("gms.messenger.StatRecorder"));
  }
}
