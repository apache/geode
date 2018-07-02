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
package org.apache.geode.internal.cache.backup;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;

/**
 * A Operation to from an admin VM to all non admin members to start a backup. In the prepare phase
 * of the backup, the members will suspend bucket destroys to make sure buckets aren't missed during
 * the backup.
 */
public class FlushToDiskStep {
  private static final Logger logger = LogService.getLogger();

  private final DistributionManager dm;
  private final InternalDistributedMember member;
  private final InternalCache cache;
  private final Set<InternalDistributedMember> recipients;
  private final FlushToDiskFactory flushToDiskFactory;

  FlushToDiskStep(DistributionManager dm, InternalDistributedMember member,
      InternalCache cache, Set<InternalDistributedMember> recipients,
      FlushToDiskFactory flushToDiskFactory) {
    this.flushToDiskFactory = flushToDiskFactory;
    this.dm = dm;
    this.member = member;
    this.recipients = recipients;
    this.cache = cache;
  }

  void send() {
    ReplyProcessor21 replyProcessor = createReplyProcessor();

    dm.putOutgoing(createDistributionMessage(replyProcessor));

    processLocally();

    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if (!(e.getCause() instanceof CancelException)) {
        throw e;
      }
    } catch (InterruptedException e) {
      logger.warn(e.getMessage(), e);
    }
  }

  private ReplyProcessor21 createReplyProcessor() {
    return this.flushToDiskFactory.createReplyProcessor(dm, recipients);
  }

  private DistributionMessage createDistributionMessage(ReplyProcessor21 replyProcessor) {
    return this.flushToDiskFactory.createRequest(member, recipients,
        replyProcessor.getProcessorId());
  }

  private void processLocally() {
    flushToDiskFactory.createFlushToDisk(cache).run();
  }

}
