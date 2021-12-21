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

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class RemovePersistentMemberMessage extends HighPriorityDistributionMessage
    implements MessageWithReply {

  private static final Logger logger = LogService.getLogger();

  private String regionPath;
  private PersistentMemberID id;
  private int processorId;
  private PersistentMemberID initializingId;

  public RemovePersistentMemberMessage() {

  }

  public RemovePersistentMemberMessage(String regionPath, PersistentMemberID id,
      PersistentMemberID initializingId, int processorId) {
    this.regionPath = regionPath;
    this.id = id;
    this.initializingId = initializingId;
    this.processorId = processorId;
  }

  public static void send(Set<InternalDistributedMember> members, DistributionManager dm,
      String regionPath, PersistentMemberID id, PersistentMemberID initializingId)
      throws ReplyException {
    if (id == null && initializingId == null) {
      // no need to do anything
      return;
    }
    ReplyProcessor21 processor = new ReplyProcessor21(dm, members);
    RemovePersistentMemberMessage msg = new RemovePersistentMemberMessage(regionPath, id,
        initializingId, processor.getProcessorId());
    msg.setRecipients(members);
    dm.putOutgoing(msg);
    processor.waitForRepliesUninterruptibly();
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);

    PersistentMemberState state = null;
    PersistentMemberID myId = null;
    ReplyException exception = null;
    try {
      // get the region from the path, but do NOT wait on initialization,
      // otherwise we could have a distributed deadlock

      Cache cache = dm.getExistingCache();
      Region region = cache.getRegion(regionPath);
      PersistenceAdvisor persistenceAdvisor = null;
      if (region instanceof DistributedRegion) {
        persistenceAdvisor = ((DistributedRegion) region).getPersistenceAdvisor();
      } else if (region == null) {
        Bucket proxy =
            PartitionedRegionHelper.getProxyBucketRegion(dm.getCache(), regionPath, false);
        if (proxy != null) {
          persistenceAdvisor = proxy.getPersistenceAdvisor();
        }
      }

      if (persistenceAdvisor != null) {
        if (id != null) {
          persistenceAdvisor.removeMember(id);
        }
        if (initializingId != null) {
          persistenceAdvisor.removeMember(initializingId);
        }
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

  @Override
  public int getDSFID() {
    return REMOVE_PERSISTENT_MEMBER_REQUEST;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    regionPath = DataSerializer.readString(in);
    processorId = in.readInt();
    boolean hasId = in.readBoolean();
    if (hasId) {
      id = new PersistentMemberID();
      InternalDataSerializer.invokeFromData(id, in);
    }
    boolean hasInitializingId = in.readBoolean();
    if (hasInitializingId) {
      initializingId = new PersistentMemberID();
      InternalDataSerializer.invokeFromData(initializingId, in);
    }
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeString(regionPath, out);
    out.writeInt(processorId);
    out.writeBoolean(id != null);
    if (id != null) {
      InternalDataSerializer.invokeToData(id, out);
    }
    out.writeBoolean(initializingId != null);
    if (initializingId != null) {
      InternalDataSerializer.invokeToData(initializingId, out);
    }
  }
}
