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
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEventImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientTombstoneMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.SerializationVersions;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This message class sends tombstone GC information to other PR holders
 *
 * @since GemFire 7.0
 */
public class PRTombstoneMessage extends PartitionMessageWithDirectReply
    implements SerializationVersions {

  private static final Logger logger = LogService.getLogger();

  @Immutable
  private static final KnownVersion[] serializationVersions = null;

  private Set<Object> keys;
  private EventID eventID;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public PRTombstoneMessage() {}

  public static void send(BucketRegion r, final Set<Object> keys, EventID eventID) {
    Set<InternalDistributedMember> recipients =
        r.getPartitionedRegion().getRegionAdvisor().adviseAllServersWithInterest();
    recipients.removeAll(r.getDistributionAdvisor().adviseReplicates());
    if (recipients.size() == 0) {
      return;
    }
    PartitionResponse p = new Response(r.getSystem(), recipients);
    PRTombstoneMessage m =
        new PRTombstoneMessage(recipients, r.getPartitionedRegion().getPRId(), p, keys, eventID);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    r.getDistributionManager().putOutgoing(m);

    try {
      p.waitForCacheException();
    } catch (ForceReattemptException e) {
      // ignore - the member is going away or has destroyed the PR so
      // it won't be forwarding anything to clients for the PR
    }
  }

  private PRTombstoneMessage(Set<InternalDistributedMember> recipients, int regionId,
      PartitionResponse p, Set<Object> reapedKeys, EventID eventID) {
    super(recipients, regionId, p);
    keys = reapedKeys;
    this.eventID = eventID;
  }

  @Override
  protected boolean operateOnPartitionedRegion(final ClusterDistributionManager dm,
      PartitionedRegion r, long startTime) throws ForceReattemptException {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace("PRTombstoneMessage operateOnRegion: {}", r.getFullPath());
    }
    FilterProfile fp = r.getFilterProfile();
    if (keys != null && keys.size() > 0) { // sanity check
      if (fp != null && CacheClientNotifier.singletonHasClientProxies() && eventID != null) {
        RegionEventImpl regionEvent = new RegionEventImpl(r, Operation.REGION_DESTROY, null, true,
            r.getGemFireCache().getMyId());
        regionEvent.setLocalFilterInfo(fp.getLocalFilterRouting(regionEvent));
        ClientUpdateMessage clientMessage = ClientTombstoneMessage.gc(r, keys, eventID);
        CacheClientNotifier.notifyClients(regionEvent, clientMessage);
      }
    }
    return true;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; keys=").append(keys.size());
    buff.append("; eventID=").append(eventID);
  }

  @Override
  public int getDSFID() {
    return PR_TOMBSTONE_MESSAGE;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return serializationVersions;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int numKeys = in.readInt();
    keys = new HashSet<>(numKeys);
    for (int i = 0; i < numKeys; i++) {
      keys.add(DataSerializer.readObject(in));
    }
    eventID = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(keys.size());
    for (Object key : keys) {
      DataSerializer.writeObject(key, out);
    }
    DataSerializer.writeObject(eventID, out);
  }

  private static class Response extends PartitionResponse {
    // Set<InternalDistributedMember> forceReattemptSenders = new
    // HashSet<InternalDistributedMember>();

    public Response(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients, false);
    }

    @Override
    public void process(DistributionMessage msg) {
      ReplyMessage reply = (ReplyMessage) msg;
      if (reply.getException() != null) {
        Throwable cause = reply.getException().getCause();
        if (cause instanceof ForceReattemptException || cause instanceof CancelException) {
          // TODO do we need to resend to these recipients? Might they have clients that won't
          // otherwise get
          // the GC message?
          // this.forceReattemptSenders.add(reply.getSender());
          reply.setException(null);
        }
      }
      super.process(reply);
    }
  }
}
