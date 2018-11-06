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

package org.apache.geode.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.DataLocationException;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * This message is generated based on event received on GatewayReceiver for updating the time-stamp
 * in a version tag for a RegionEntry.
 *
 *
 */
public class PRUpdateEntryVersionMessage extends PartitionMessageWithDirectReply {

  private static final Logger logger = LogService.getLogger();

  /** The key associated with the value that must be sent */
  private Object key;

  /** The operation performed on the sender */
  private Operation op;

  /** event identifier */
  private EventID eventId;

  protected VersionTag versionTag;

  /** for deserialization */
  public PRUpdateEntryVersionMessage() {}

  public PRUpdateEntryVersionMessage(Collection<InternalDistributedMember> recipients, int regionId,
      DirectReplyProcessor processor) {
    super(recipients, regionId, processor);
  }

  public PRUpdateEntryVersionMessage(Set recipients, int regionId, DirectReplyProcessor processor,
      EntryEventImpl event) {
    super(recipients, regionId, processor, event);
    this.key = event.getKey();
    this.op = event.getOperation();
    this.eventId = event.getEventId();
    this.versionTag = event.getVersionTag();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.DataSerializableFixedID#getDSFID()
   */
  @Override
  public int getDSFID() {
    return PR_UPDATE_ENTRY_VERSION_MESSAGE;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.partitioned.PartitionMessage# operateOnPartitionedRegion
   * (org.apache.geode.distributed.internal.DistributionManager,
   * org.apache.geode.internal.cache.PartitionedRegion, long)
   */
  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion pr,
      long startTime) throws CacheException, QueryException, DataLocationException,
      InterruptedException, IOException {
    // release not needed because disallowOffHeapValues called
    final EntryEventImpl event =
        EntryEventImpl.create(pr, getOperation(), getKey(), null, /* newValue */
            null, /* callbackargs */
            false /* originRemote - false to force distribution in buckets */,
            getSender() /* eventSender */, false /* generateCallbacks */, false /* initializeId */);
    event.disallowOffHeapValues();

    Assert.assertTrue(eventId != null);
    if (this.versionTag != null) {
      event.setVersionTag(this.versionTag);
    }
    event.setEventId(eventId);
    event.setPossibleDuplicate(this.posDup);
    event.setInvokePRCallbacks(false);
    event.setCausedByMessage(this);

    boolean sendReply = true;

    if (!notificationOnly) {

      PartitionedRegionDataStore ds = pr.getDataStore();
      Assert.assertTrue(ds != null,
          "This process should have storage for an item in " + this.toString());
      try {
        Integer bucket = Integer.valueOf(PartitionedRegionHelper.getHashKey(event));

        pr.getDataView().updateEntryVersion(event);

        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace("{}: updateEntryVersionLocally in bucket: {}, key: {}", getClass().getName(),
              bucket, key);
        }
      } catch (EntryNotFoundException eee) {
        // failed = true;
        if (logger.isDebugEnabled()) {
          logger.debug("{}: operateOnRegion caught EntryNotFoundException", getClass().getName());
        }
        sendReply(getSender(), getProcessorId(), dm, null /*
                                                           * No need to send exception back
                                                           */, pr, startTime);
        sendReply = false; // this prevents us from acknowledging later
      } catch (PrimaryBucketException pbe) {
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(pbe), pr, startTime);
        return false;
      }

    }
    return sendReply;
  }

  private Operation getOperation() {
    return this.op;
  }

  private Object getKey() {
    return this.key;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
    this.op = Operation.fromOrdinal(in.readByte());
    this.eventId = (EventID) DataSerializer.readObject(in);
    this.versionTag = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(getKey(), out);
    out.writeByte(this.op.ordinal);
    DataSerializer.writeObject(this.eventId, out);
    DataSerializer.writeObject(this.versionTag, out);
  }

  /**
   * Assists the toString method in reporting the contents of this message
   *
   * @see PartitionMessage#toString()
   */

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; key=").append(getKey());
    buff.append("; op=").append(this.op);

    if (eventId != null) {
      buff.append("; eventId=").append(eventId);
    }
    if (this.versionTag != null) {
      buff.append("; version=").append(this.versionTag);
    }
  }

  /**
   * Response for PartitionMessage {@link PRUpdateEntryVersionMessage}.
   *
   */
  public static class UpdateEntryVersionResponse extends PartitionResponse {

    private volatile boolean versionUpdated;
    private final Object key;

    public UpdateEntryVersionResponse(InternalDistributedSystem dm,
        InternalDistributedMember member, Object k) {
      super(dm, member);
      this.key = k;
    }

    public UpdateEntryVersionResponse(InternalDistributedSystem dm, Set recipients, Object k) {
      super(dm, recipients, false);
      this.key = k;
    }

    public void setResponse(ReplyMessage msg) {
      this.versionUpdated = true;
    }

    /**
     * @throws ForceReattemptException if the peer is no longer available
     * @throws CacheException if the peer generates an error
     */
    public void waitForResult() throws CacheException, ForceReattemptException {
      try {
        waitForCacheException();
      } catch (ForceReattemptException e) {
        e.checkKey(key);
        throw e;
      }

      if (!this.versionUpdated) {
        if (logger.isDebugEnabled()) {
          logger.debug("UpdateEntryVersionResponse: Update entry version failed for key: {}", key);
        }
      }
    }
  }

  public static UpdateEntryVersionResponse send(InternalDistributedMember recipient,
      PartitionedRegion r, EntryEventImpl event) throws ForceReattemptException {
    Set recipients = Collections.singleton(recipient);
    UpdateEntryVersionResponse p =
        new UpdateEntryVersionResponse(r.getSystem(), recipient, event.getKey());
    PRUpdateEntryVersionMessage m =
        new PRUpdateEntryVersionMessage(recipients, r.getPRId(), p, event);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(
          String.format("Failed sending < %s >.", m));
    }
    return p;
  }
}
