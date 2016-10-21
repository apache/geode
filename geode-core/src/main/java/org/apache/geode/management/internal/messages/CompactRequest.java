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
package org.apache.geode.management.internal.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.admin.remote.AdminMultipleReplyProcessor;
import org.apache.geode.internal.admin.remote.AdminRequest;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;

/**
 * An instruction to all members with cache that they should compact their disk stores.
 * 
 * 
 * @since GemFire 7.0
 */
// NOTE: This is copied from org/apache/geode/internal/admin/remote/CompactRequest.java
// and modified as per requirements. (original-author Dan Smith)
public class CompactRequest extends AdminRequest {
  private static final Logger logger = LogService.getLogger();

  private String diskStoreName;
  private static String notExecutedMembers;

  public static Map<DistributedMember, PersistentID> send(DM dm, String diskStoreName,
      Set<?> recipients) {
    Map<DistributedMember, PersistentID> results = Collections.emptyMap();

    if (recipients != null && !recipients.isEmpty()) {
      CompactRequest request = new CompactRequest();
      request.setRecipients(recipients);

      CompactReplyProcessor replyProcessor = new CompactReplyProcessor(dm, recipients);
      request.msgId = replyProcessor.getProcessorId();
      request.diskStoreName = diskStoreName;
      request.setSender(dm.getDistributionManagerId());
      Set<?> putOutgoing = dm.putOutgoing(request);
      if (putOutgoing != null && !putOutgoing.isEmpty()) {
        notExecutedMembers = putOutgoing.toString();
      }

      try {
        replyProcessor.waitForReplies();
      } catch (ReplyException e) {
        if (!(e.getCause() instanceof CancelException)) {
          throw e;
        }
      } catch (InterruptedException e) {
        logger.debug(e.getMessage(), e);
      }

      results = replyProcessor.results;
    }

    return results;
  }

  @Override
  protected void process(DistributionManager dm) {
    super.process(dm);
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    PersistentID compactedDiskStore = compactDiskStore(this.diskStoreName);

    return new CompactResponse(this.getSender(), compactedDiskStore);
  }

  public static PersistentID compactDiskStore(String diskStoreName) {
    PersistentID persistentID = null;
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && !cache.isClosed()) {
      DiskStoreImpl diskStore = (DiskStoreImpl) cache.findDiskStore(diskStoreName);
      if (diskStore != null && diskStore.forceCompaction()) {
        persistentID = diskStore.getPersistentID();
      }
    }

    return persistentID;
  }

  public static String getNotExecutedMembers() {
    return notExecutedMembers;
  }

  public int getDSFID() {
    return MGMT_COMPACT_REQUEST;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.diskStoreName = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.diskStoreName, out);
  }

  @Override
  public String toString() {
    return "Compact request sent to " + Arrays.toString(this.getRecipients()) + " from "
        + this.getSender() + " for " + this.diskStoreName;
  }

  private static class CompactReplyProcessor extends AdminMultipleReplyProcessor {
    Map<DistributedMember, PersistentID> results =
        Collections.synchronizedMap(new HashMap<DistributedMember, PersistentID>());

    public CompactReplyProcessor(DM dm, Collection<?> initMembers) {
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
        final PersistentID persistentId = ((CompactResponse) msg).getPersistentId();
        if (persistentId != null) {
          results.put(msg.getSender(), persistentId);
        }
      }
      super.process(msg, warn);
    }
  }
}
