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
package org.apache.geode.admin.internal;

import java.util.Collection;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.admin.remote.AdminMultipleReplyProcessor;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.admin.remote.CliLegacyMessage;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;

/**
 * A request to from an admin VM to all non admin members to start a backup. In the prepare phase of
 * the backup, the members will suspend bucket destroys to make sure buckets aren't missed during
 * the backup.
 */
public class FlushToDiskRequest extends CliLegacyMessage {
  private static final Logger logger = LogService.getLogger();

  public FlushToDiskRequest() {
    // nothing
  }

  public static void send(DM dm, Set recipients) {
    FlushToDiskRequest request = new FlushToDiskRequest();
    request.setRecipients(recipients);

    FlushToDiskProcessor replyProcessor = new FlushToDiskProcessor(dm, recipients);
    request.msgId = replyProcessor.getProcessorId();
    dm.putOutgoing(request);
    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if (!(e.getCause() instanceof CancelException)) {
        throw e;
      }
    } catch (InterruptedException e) {
      logger.debug(e);
    }
    AdminResponse response = request.createResponse((DistributionManager) dm);
    response.setSender(dm.getDistributionManagerId());
    replyProcessor.process(response);
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      cache.listDiskStoresIncludingRegionOwned().forEach(DiskStore::flush);
    }

    return new FlushToDiskResponse(this.getSender());
  }

  @Override
  public int getDSFID() {
    return FLUSH_TO_DISK_REQUEST;
  }

  private static class FlushToDiskProcessor extends AdminMultipleReplyProcessor {
    public FlushToDiskProcessor(DM dm, Collection initMembers) {
      super(dm, initMembers);
    }

    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }
  }
}
