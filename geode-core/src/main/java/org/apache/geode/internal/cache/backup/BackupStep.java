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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;

abstract class BackupStep implements BackupResultCollector {
  private static final Logger logger = LogService.getLogger();

  private final DistributionManager dm;
  private final Map<DistributedMember, Set<PersistentID>> results =
      Collections.synchronizedMap(new HashMap<>());

  BackupStep(DistributionManager dm) {
    this.dm = dm;
  }

  abstract ReplyProcessor21 createReplyProcessor();

  abstract DistributionMessage createDistributionMessage(ReplyProcessor21 replyProcessor);

  abstract void processLocally();

  @Override
  public void addToResults(InternalDistributedMember member, Set<PersistentID> persistentIds) {
    if (persistentIds != null && !persistentIds.isEmpty()) {
      results.put(member, persistentIds);
    }
  }

  Map<DistributedMember, Set<PersistentID>> send() {
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

    return getResults();
  }

  Map<DistributedMember, Set<PersistentID>> getResults() {
    return results;
  }

  DistributionManager getDistributionManager() {
    return dm;
  }
}
