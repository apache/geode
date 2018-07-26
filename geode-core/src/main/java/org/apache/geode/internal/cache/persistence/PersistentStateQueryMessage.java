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
import java.util.Collection;
import java.util.HashSet;
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
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.logging.LogService;

public class PersistentStateQueryMessage extends HighPriorityDistributionMessage
    implements MessageWithReply {

  private static final Logger logger = LogService.getLogger();

  private String regionPath;
  private PersistentMemberID id;
  private int processorId;
  private PersistentMemberID initializingId;

  public PersistentStateQueryMessage() {

  }

  public PersistentStateQueryMessage(String regionPath, PersistentMemberID id,
      PersistentMemberID initializingId, int processorId) {
    this.regionPath = regionPath;
    this.id = id;
    this.initializingId = initializingId;
    this.processorId = processorId;
  }

  PersistentStateQueryResults send(Set<InternalDistributedMember> members, DistributionManager dm,
      PersistentStateQueryReplyProcessor processor) {
    setRecipients(members);

    dm.putOutgoing(this);

    processor.waitForRepliesUninterruptibly();
    return processor.results;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    int oldLevel = // Set thread local flag to allow entrance through initialization Latch
        LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);

    PersistentMemberState state = null;
    PersistentMemberID myId = null;
    PersistentMemberID myInitializingId = null;
    DiskStoreID diskStoreId = null;
    HashSet<PersistentMemberID> onlineMembers = null;
    ReplyException exception = null;
    boolean successfulReply = false;
    try {
      // get the region from the path, but do NOT wait on initialization,
      // otherwise we could have a distributed deadlock

      Cache cache = dm.getExistingCache();
      Region region = cache.getRegion(this.regionPath);
      PersistenceAdvisor persistenceAdvisor = null;
      if (region instanceof DistributedRegion) {
        persistenceAdvisor = ((DistributedRegion) region).getPersistenceAdvisor();
      } else if (region == null) {
        Bucket proxy =
            PartitionedRegionHelper.getProxyBucketRegion(dm.getCache(), this.regionPath, false);
        if (proxy != null) {
          persistenceAdvisor = proxy.getPersistenceAdvisor();
        }
      }
      if (persistenceAdvisor != null) {
        if (id != null) {
          state = persistenceAdvisor.getPersistedStateOfMember(id);
        }
        if (initializingId != null && state == null) {
          state = persistenceAdvisor.getPersistedStateOfMember(initializingId);
        }
        myId = persistenceAdvisor.getPersistentID();
        myInitializingId = persistenceAdvisor.getInitializingID();
        onlineMembers = persistenceAdvisor.getPersistedOnlineOrEqualMembers();
        diskStoreId = persistenceAdvisor.getDiskStoreID();
        successfulReply = true;
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
      ReplyMessage replyMsg;
      if (successfulReply) {
        PersistentStateQueryReplyMessage persistentReplyMessage =
            new PersistentStateQueryReplyMessage();
        persistentReplyMessage.myId = myId;
        persistentReplyMessage.persistedStateOfPeer = state;
        persistentReplyMessage.myInitializingId = myInitializingId;
        persistentReplyMessage.diskStoreId = diskStoreId;
        persistentReplyMessage.onlineMembers = onlineMembers;
        replyMsg = persistentReplyMessage;
      } else {
        replyMsg = new ReplyMessage();
      }
      replyMsg.setProcessorId(processorId);
      replyMsg.setRecipient(getSender());
      if (exception != null) {
        replyMsg.setException(exception);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Received {},replying with {}", this, replyMsg);
      }
      dm.putOutgoing(replyMsg);
    }
  }

  public int getDSFID() {
    return PERSISTENT_STATE_QUERY_REQUEST;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    regionPath = DataSerializer.readString(in);
    processorId = in.readInt();
    boolean hasId = in.readBoolean();
    if (hasId) {
      id = new PersistentMemberID();
      id.fromData(in);
    }
    boolean hasInitializingId = in.readBoolean();
    if (hasInitializingId) {
      initializingId = new PersistentMemberID();
      initializingId.fromData(in);
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(regionPath, out);
    out.writeInt(processorId);
    out.writeBoolean(id != null);
    if (id != null) {
      id.toData(out);
    }
    out.writeBoolean(initializingId != null);
    if (initializingId != null) {
      initializingId.toData(out);
    }
  }

  @Override
  public String toString() {
    return super.toString() + ",id=" + id + ",regionPath=" + regionPath + ",initializingId="
        + initializingId;
  }

  static class PersistentStateQueryReplyProcessor extends ReplyProcessor21 {
    PersistentStateQueryResults results = new PersistentStateQueryResults();

    public PersistentStateQueryReplyProcessor(DistributionManager dm, Collection initMembers) {
      super(dm, initMembers);
    }

    @Override
    public void process(DistributionMessage msg) {
      if (msg instanceof PersistentStateQueryReplyMessage) {
        PersistentStateQueryReplyMessage reply = (PersistentStateQueryReplyMessage) msg;
        results.addResult(reply.persistedStateOfPeer, reply.getSender(), reply.myId,
            reply.myInitializingId, reply.diskStoreId, reply.onlineMembers);
      }
      super.process(msg);
    }
  }

  public static class PersistentStateQueryReplyMessage extends ReplyMessage {
    public HashSet<PersistentMemberID> onlineMembers;
    public DiskStoreID diskStoreId;
    private PersistentMemberState persistedStateOfPeer;
    private PersistentMemberID myId;
    private PersistentMemberID myInitializingId;

    @Override
    public int getDSFID() {
      return PERSISTENT_STATE_QUERY_REPLY;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      boolean hasId = in.readBoolean();
      if (hasId) {
        myId = new PersistentMemberID();
        myId.fromData(in);
      }
      boolean hasState = in.readBoolean();
      if (hasState) {
        persistedStateOfPeer = PersistentMemberState.fromData(in);
      }
      boolean hasInitializingId = in.readBoolean();
      if (hasInitializingId) {
        myInitializingId = new PersistentMemberID();
        myInitializingId.fromData(in);
      }
      boolean hasDiskStoreID = in.readBoolean();
      if (hasDiskStoreID) {
        diskStoreId = new DiskStoreID(in.readLong(), in.readLong());
      }
      boolean hasOnlineMembers = in.readBoolean();
      if (hasOnlineMembers) {
        onlineMembers = DataSerializer.readHashSet(in);
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      if (myId == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        myId.toData(out);
      }
      if (persistedStateOfPeer == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        persistedStateOfPeer.toData(out);
      }
      if (myInitializingId == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        myInitializingId.toData(out);
      }
      if (diskStoreId == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        out.writeLong(diskStoreId.getMostSignificantBits());
        out.writeLong(diskStoreId.getLeastSignificantBits());
      }
      if (onlineMembers == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        DataSerializer.writeHashSet(onlineMembers, out);
      }
    }

    @Override
    public String toString() {
      return super.toString() + ",myId=" + myId + ",myInitializingId=" + myInitializingId
          + ",persistedStateOfPeer=" + persistedStateOfPeer;
    }
  }
}
