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

import org.apache.logging.log4j.Logger;
import org.jgroups.Event;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.FragHeader;
import org.jgroups.protocols.UNICAST3;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSUtil;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;

/**
 * JGroups doesn't capture quite the stats we want so this protocol is
 * inserted into the stack to gather the missing ones.
 * 
 *
 */
public class StatRecorder extends Protocol {
  
  private static final Logger logger = Services.getLogger();
  
  private static final int OUTGOING = 0;
  private static final int INCOMING = 1;
  
  DMStats stats;
  
  private final short nakackHeaderId = ClassConfigurator.getProtocolId(NAKACK2.class);
  private final short unicastHeaderId = ClassConfigurator.getProtocolId(UNICAST3.class);
  private final short frag2HeaderId = ClassConfigurator.getProtocolId(FRAG2.class);
  
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
      filter(msg, INCOMING);
    }
    return up_prot.up(evt);
  }
  
  @Override
  public Object down(Event evt) {
    switch (evt.getType()) {
    case Event.MSG:
      Message msg = (Message)evt.getArg();
      processForMulticast(msg, OUTGOING);
      processForUnicast(msg, OUTGOING);
      filter(msg, OUTGOING);
      break;
    }
    return down_prot.down(evt);
  }
  

  private void processForMulticast(Message msg, int direction) {
    Object o = msg.getHeader(nakackHeaderId);
//    logger.debug("sending message with NakAck header {}: {}", o, msg);
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
  
  private void filter(Message msg, int direction) {
    if (direction == INCOMING) {
      Header h = msg.getHeader(frag2HeaderId);
      boolean copyBuffer = false;
      if (h != null && h instanceof FragHeader) {
        copyBuffer = true;
//      String str = direction == OUTGOING? "sending" : "receiving";
//      logger.debug("{} fragment {} msg buffer hash {}  offset {} msg size {} first bytes=\n{}", str, hdr, 
//          msg.getRawBuffer().hashCode(), msg.getOffset(), msg.getLength(),
//          GMSUtil.formatBytes(msg.getRawBuffer(), msg.getOffset(),
//              Math.min(200, msg.getLength())));
      } else {
        h = msg.getHeader(unicastHeaderId);
        if (h instanceof UNICAST3.Header) {
          copyBuffer = true;
        } else {
          h = msg.getHeader(nakackHeaderId);
          if (h instanceof NakAckHeader2) {
            copyBuffer = true;
          }
        }
      }
      if (copyBuffer) {
        // JGroups doesn't copy its message buffer when thread pools are
        // disabled.  This causes Frag2 fragments to become corrupted
        msg.setBuffer(msg.getBuffer(), 0, msg.getLength());
      }
    }
  }
}
