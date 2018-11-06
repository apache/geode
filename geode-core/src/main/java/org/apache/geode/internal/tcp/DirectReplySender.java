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
package org.apache.geode.internal.tcp;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.LonerDistributionManager.DummyDMStats;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * A reply sender which replies back directly to a dedicated socket socket.
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

  public Set<InternalDistributedMember> putOutgoing(DistributionMessage msg) {
    Assert.assertTrue(!this.sentReply, "Trying to reply twice to a message");
    // Using an ArrayList, rather than Collections.singletonList here, because the MsgStreamer
    // mutates the list when it has exceptions.

    // fix for bug #42199 - cancellation check
    this.conn.getConduit().getDM().getCancelCriterion().checkCancelInProgress(null);

    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "Sending a direct reply {} to {}", msg,
          conn.getRemoteAddress());
    }
    ArrayList<Connection> conns = new ArrayList<Connection>(1);
    conns.add(conn);
    MsgStreamer ms = (MsgStreamer) MsgStreamer.create(conns, msg, false, DUMMY_STATS);
    try {
      ms.writeMessage();
      ConnectExceptions ce = ms.getConnectExceptions();
      if (ce != null && !ce.getMembers().isEmpty()) {
        Assert.assertTrue(ce.getMembers().size() == 1);
        logger.warn("Failed sending a direct reply to {}",
            ce.getMembers().iterator().next());
        return Collections.singleton(ce.getMembers().iterator().next());
      }
      sentReply = true;
      return Collections.emptySet();
    } catch (NotSerializableException e) {
      throw new InternalGemFireException(e);
    } catch (IOException ex) {
      throw new InternalGemFireException(
          "Unknown error serializing message", ex);
    } finally {
      try {
        ms.close();
      } catch (IOException e) {
        throw new InternalGemFireException("Unknown error serializing message", e);
      }
    }

  }

}
