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
package org.apache.geode.internal.cache.persistence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.logging.LogService;

/**
 *
 */
public class PrepareNewPersistentMemberMessage extends HighPriorityDistributionMessage
    implements MessageWithReply {

  private static final Logger logger = LogService.getLogger();

  private String regionPath;
  private PersistentMemberID oldId;
  private PersistentMemberID newId;
  private int processorId;

  public PrepareNewPersistentMemberMessage() {

  }

  public PrepareNewPersistentMemberMessage(String regionPath, PersistentMemberID oldId,
      PersistentMemberID newId, int processorId) {
    this.regionPath = regionPath;
    this.newId = newId;
    this.oldId = oldId;
    this.processorId = processorId;
  }

  public static void send(Set<InternalDistributedMember> members, DM dm, String regionPath,
      PersistentMemberID oldId, PersistentMemberID newId) throws ReplyException {
    ReplyProcessor21 processor = new ReplyProcessor21(dm, members);
    PrepareNewPersistentMemberMessage msg =
        new PrepareNewPersistentMemberMessage(regionPath, oldId, newId, processor.getProcessorId());
    msg.setRecipients(members);
    dm.putOutgoing(msg);
    processor.waitForRepliesUninterruptibly();
  }

  @Override
  protected void process(DistributionManager dm) {
    int oldLevel = // Set thread local flag to allow entrance through initialization Latch
        LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);

    PersistentMemberState state = null;
    PersistentMemberID myId = null;
    ReplyException exception = null;
    try {
      // get the region from the path, but do NOT wait on initialization,
      // otherwise we could have a distributed deadlock

      Cache cache = CacheFactory.getInstance(dm.getSystem());
      Region region = cache.getRegion(this.regionPath);
      PersistenceAdvisor persistenceAdvisor = null;
      if (region instanceof DistributedRegion) {
        persistenceAdvisor = ((DistributedRegion) region).getPersistenceAdvisor();
      } else if (region == null) {
        Bucket proxy = PartitionedRegionHelper.getProxyBucketRegion(GemFireCacheImpl.getInstance(),
            this.regionPath, false);
        if (proxy != null) {
          persistenceAdvisor = proxy.getPersistenceAdvisor();
        }
      }

      if (persistenceAdvisor != null) {
        persistenceAdvisor.prepareNewMember(getSender(), oldId, newId);
      }

    } catch (RegionDestroyedException e) {
      logger.debug("<RegionDestroyed> {}", this);
    } catch (CancelException e) {
      logger.debug("<CancelException> {}", this);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      exception = new ReplyException(t);
    } finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
      ReplyMessage replyMsg = new ReplyMessage();
      replyMsg.setRecipient(getSender());
      replyMsg.setProcessorId(processorId);
      if (exception != null) {
        replyMsg.setException(exception);
      }
      dm.putOutgoing(replyMsg);
    }
  }

  public int getDSFID() {
    return PREPARE_NEW_PERSISTENT_MEMBER_REQUEST;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    regionPath = DataSerializer.readString(in);
    processorId = in.readInt();
    boolean hasOldId = in.readBoolean();
    if (hasOldId) {
      oldId = new PersistentMemberID();
      InternalDataSerializer.invokeFromData(oldId, in);
    }
    newId = new PersistentMemberID();
    InternalDataSerializer.invokeFromData(newId, in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(regionPath, out);
    out.writeInt(processorId);
    out.writeBoolean(oldId != null);
    if (oldId != null) {
      InternalDataSerializer.invokeToData(oldId, out);
    }
    InternalDataSerializer.invokeToData(newId, out);
  }
}
