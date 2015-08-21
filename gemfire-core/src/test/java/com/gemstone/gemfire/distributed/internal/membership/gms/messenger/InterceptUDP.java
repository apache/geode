package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.UUID;

/**
 * FakeUDP replaces the regular UDP JGroups messaging protocol
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
    Object o = msg.getHeader(nakackHeaderId);
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