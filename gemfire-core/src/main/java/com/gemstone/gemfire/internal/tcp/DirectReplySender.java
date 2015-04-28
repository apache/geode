/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.ToDataException;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.LonerDistributionManager.DummyDMStats;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * A reply sender which replies back directly to a dedicated socket
 * socket.
 * @author dsmith
 *
 */
class DirectReplySender implements ReplySender {
  private static final Logger logger = LogService.getLogger();
  
  private static final DMStats DUMMY_STATS = new DummyDMStats();

  private final Connection conn;
  private boolean sentReply = false;

  public DirectReplySender(Connection connection) {
    this.conn = connection;
  }
  
  public Set putOutgoing(DistributionMessage msg) {
    Assert.assertTrue(!this.sentReply, "Trying to reply twice to a message");
    //Using an ArrayList, rather than Collections.singletonList here, because the MsgStreamer
    //mutates the list when it has exceptions.
    
    // fix for bug #42199 - cancellation check
    this.conn.owner.getDM().getCancelCriterion().checkCancelInProgress(null);
    
    if(logger.isTraceEnabled(LogMarker.DM)) {
      logger.trace(LogMarker.DM, "Sending a direct reply {} to {}", msg, conn.getRemoteAddress());
    }
    ArrayList<Connection> conns = new ArrayList<Connection>(1);
    conns.add(conn);
    MsgStreamer ms = (MsgStreamer)MsgStreamer.create(conns, msg, false,
        DUMMY_STATS);
    try {
      ms.writeMessage();
      ConnectExceptions ce = ms.getConnectExceptions();
      if(ce != null && !ce.getMembers().isEmpty()) {
        Assert.assertTrue(ce.getMembers().size() == 1);
        logger.warn(LocalizedMessage.create(
            LocalizedStrings.DirectChannel_FAILURE_SENDING_DIRECT_REPLY, ce.getMembers().iterator().next()));
        return Collections.singleton(ce.getMembers().iterator().next());
      }
      sentReply = true;
      return Collections.emptySet();
    } 
    catch (NotSerializableException e) {
      throw new InternalGemFireException(e);
    } 
    catch (ToDataException e) {
      // exception from user code
      throw e;
    } 
    catch (IOException ex) {
      throw new InternalGemFireException(LocalizedStrings.DirectChannel_UNKNOWN_ERROR_SERIALIZING_MESSAGE.toLocalizedString(), ex);
    }
    finally {
      try {
        ms.close();
      }
      catch (IOException e) {
        throw new InternalGemFireException("Unknown error serializing message", e);
      }
    }

  }

}
