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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;

/**
 * InterceptUDP replaces the regular UDP JGroups messaging protocol
 * for unit testing.  It does not create a datagram socket
 * and is only set up to record message counts and respond
 * to Unicast to keep it from retransmitting
 */
public class InterceptUDP extends Protocol {
  
  static final int MEMBERSHIP_PORT = 12345;
  
  private final short nakackHeaderId = ClassConfigurator.getProtocolId(NAKACK2.class);
  private final short unicastHeaderId = ClassConfigurator.getProtocolId(UNICAST3.class);
  
  UUID uuid;
//  IpAddress addr;
//  Map<UUID, IpAddress> addressMap;
  int unicastSentDataMessages;
  int mcastSentDataMessages;
  
  boolean collectMessages = false;
  List<Message> collectedMessages = new LinkedList<>();
  
  public InterceptUDP() {
//    uuid = new UUID();
//    try {
//      addr = new IpAddress("localhost", MEMBERSHIP_PORT);
//    } catch (UnknownHostException e) {
//      throw new RuntimeException("unexpected exception", e);
//    }
//    addressMap = new HashMap<>();
//    addressMap.put(uuid, addr);
  }
  
  @Override
  public Object up(Event evt) {
    return up_prot.up(evt);
  }

  @Override
  public Object down(Event evt) {
    switch (evt.getType()) {
    case Event.MSG:
      handleMessage((Message)evt.getArg());
      return null;
    case Event.SET_LOCAL_ADDRESS:
      uuid=(UUID)evt.getArg();
      break;
    }
    return down_prot.down(evt);
  }
  
  private void handleMessage(Message msg) {
    if (collectMessages) {
      collectedMessages.add(msg);
    }
    Object o = msg.getHeader(nakackHeaderId);
    if (o != null) {
      mcastSentDataMessages++;
    } else {
      o = msg.getHeader(unicastHeaderId);
      if (o != null) {
        UNICAST3.Header hdr = (UNICAST3.Header)o;
        switch (hdr.type()) {
        case UNICAST3.Header.DATA:
          unicastSentDataMessages++;
          Message response = new Message(uuid, msg.getDest(), null);
          response.putHeader(unicastHeaderId, UNICAST3.Header.createAckHeader(hdr.seqno(), hdr.connId(), System.currentTimeMillis()));
          up_prot.up(new Event(Event.MSG, response));
          break;
        }
      }
    }
  }
}
