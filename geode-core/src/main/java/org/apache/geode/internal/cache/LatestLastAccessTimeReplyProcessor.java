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
package org.apache.geode.internal.cache;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Processes the replies from a LatestLastAccessTimeMessage. This reply processor only keeps the
 * largest value returned. It waits for every recipient to respond.
 *
 * @since Geode 1.4
 */
public class LatestLastAccessTimeReplyProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  private long latestLastAccessTime = 0;

  public LatestLastAccessTimeReplyProcessor(DistributionManager dm,
      Set<InternalDistributedMember> recipients) {
    super(dm, recipients);
  }

  @Override
  public void process(DistributionMessage msg) {
    try {
      ReplyMessage reply = (ReplyMessage) msg;
      long replyTime = (long) reply.getReturnValue();
      updateLatestLastAccessTime(replyTime);
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "LatestLastAccessTimeReplyMessage return value is {}",
            replyTime);
      }
    } finally {
      super.process(msg);
    }
  }

  private synchronized void updateLatestLastAccessTime(long newTime) {
    if (newTime > latestLastAccessTime) {
      latestLastAccessTime = newTime;
    }
  }

  public synchronized long getLatestLastAccessTime() {
    return latestLastAccessTime;
  }

}
