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
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.ArrayUtils;

/**
 * An instruction to all members with cache that they should compact their disk stores.
 */
public class CompactRequest extends CliLegacyMessage {
  private static final Logger logger = LogService.getLogger();

  public CompactRequest() {
    // do nothing
  }

  public static Map<DistributedMember, Set<PersistentID>> send(DM dm) {
    Set recipients = dm.getOtherDistributionManagerIds();
    CompactRequest request = new CompactRequest();
    request.setRecipients(recipients);

    CompactReplyProcessor replyProcessor = new CompactReplyProcessor(dm, recipients);
    request.msgId = replyProcessor.getProcessorId();
    dm.putOutgoing(request);

    request.setSender(dm.getDistributionManagerId());
    request.process((DistributionManager) dm);

    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if (!(e.getCause() instanceof CancelException)) {
        throw e;
      }
    } catch (InterruptedException e) {
      logger.warn(e);
    }

    return replyProcessor.results;
  }

  @Override
  protected void process(DistributionManager dm) {
    super.process(dm);
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    InternalCache cache = dm.getCache();
    HashSet<PersistentID> compactedStores = new HashSet<>();
    if (cache != null && !cache.isClosed()) {
      for (DiskStore store : cache.listDiskStoresIncludingRegionOwned()) {
        if (store.forceCompaction()) {
          compactedStores.add(((DiskStoreImpl) store).getPersistentID());
        }
      }
    }

    return new CompactResponse(this.getSender(), compactedStores);
  }

  @Override
  public int getDSFID() {
    return COMPACT_REQUEST;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public String toString() {
    return "Compact request sent to " + ArrayUtils.toString((Object[]) this.getRecipients())
        + " from " + this.getSender();
  }

  private static class CompactReplyProcessor extends AdminMultipleReplyProcessor {
    Map<DistributedMember, Set<PersistentID>> results =
        Collections.synchronizedMap(new HashMap<DistributedMember, Set<PersistentID>>());

    CompactReplyProcessor(DM dm, Collection initMembers) {
      super(dm, initMembers);
    }

    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    @Override
    protected boolean allowReplyFromSender() {
      return true;
    }

    @Override
    protected void process(DistributionMessage msg, boolean warn) {
      if (msg instanceof CompactResponse) {
        final Set<PersistentID> persistentIds = ((CompactResponse) msg).getPersistentIds();
        if (persistentIds != null && !persistentIds.isEmpty()) {
          this.results.put(msg.getSender(), persistentIds);
        }
      }
      super.process(msg, warn);
    }
  }
}
