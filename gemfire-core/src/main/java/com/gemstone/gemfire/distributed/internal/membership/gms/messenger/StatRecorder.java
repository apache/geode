package com.gemstone.gemfire.distributed.internal.membership.gms.messenger;

import org.apache.logging.log4j.Logger;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;

/**
 * JGroups doesn't capture quite the stats we want so this protocol is
 * inserted into the stack to gather the missing ones.
 * 
 * @author bschuchardt
 *
 */
public class StatRecorder extends Protocol {
  
  private static final Logger logger = Services.getLogger();
  
  private static final int OUTGOING = 0;
  private static final int INCOMING = 1;
  
  DMStats stats;
  
  private final short nakackHeaderId = ClassConfigurator.getProtocolId(NAKACK2.class);
  private final short unicastHeaderId = ClassConfigurator.getProtocolId(UNICAST3.class);
  
  /**
   * set the statistics object to modify when events are detected
   * @param stats
   */
  public void setDMStats(DMStats stats) {
    this.stats = stats;
  }
  
  @Override
  public Object up(Event evt) {
    switch (evt.getType()) {
    case Event.MSG:
      Message msg = (Message)evt.getArg();
      processForMulticast(msg, INCOMING);
      processForUnicast(msg, INCOMING);
    }
    return up_prot.up(evt);
  }
  
  @Override
  public Object down(Event evt) {
    switch (evt.getType()) {
    case Event.MSG:
      Message msg = (Message)evt.getArg();
      processForMulticast(msg, INCOMING);
      processForUnicast(msg, INCOMING);
      break;
    }
    return down_prot.down(evt);
  }
  

  private void processForMulticast(Message msg, int direction) {
    Object o = msg.getHeader(nakackHeaderId);
    if (o instanceof NakAckHeader2  &&  stats != null) {
      NakAckHeader2 hdr = (NakAckHeader2)o;
      switch (direction) {
      case INCOMING:
        stats.incMcastReadBytes((int)msg.size());
        break;
      case OUTGOING:
        stats.incMcastWriteBytes((int)msg.size());
        switch (hdr.getType()) {
        case NakAckHeader2.XMIT_RSP:
          stats.incMcastRetransmits();
          break;
        case NakAckHeader2.XMIT_REQ:
          stats.incMcastRetransmitRequests();
          break;
        }
        break;
      }
    }
  }

  private void processForUnicast(Message msg, int direction) {
    Object o = msg.getHeader(unicastHeaderId);
    if (o instanceof UNICAST3.Header  &&  stats != null) {
      UNICAST3.Header hdr = (UNICAST3.Header)o;
      switch (direction) {
      case INCOMING:
        stats.incUcastReadBytes((int)msg.size());
        break;
      case OUTGOING:
        stats.incUcastWriteBytes((int)msg.size());
        switch (hdr.type()) {
        case UNICAST3.Header.XMIT_REQ:
          stats.incUcastRetransmits();
          break;
        }
        break;
      }
    }
  }
}
