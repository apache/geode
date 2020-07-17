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

package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.InvalidVersionException;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Common code for both UpdateOperation and DistributedPutAllOperation.
 *
 */
public abstract class AbstractUpdateOperation extends DistributedCacheOperation {
  private static final Logger logger = LogService.getLogger();

  @MutableForTesting
  public static volatile boolean test_InvalidVersion;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD",
      justification = "test hook that is unset normally")

  private final long lastModifiedTime;

  public AbstractUpdateOperation(CacheEvent event, long lastModifiedTime) {
    super(event);
    this.lastModifiedTime = lastModifiedTime;
  }

  @Override
  protected Set getRecipients() {
    CacheDistributionAdvisor advisor = getRegion().getCacheDistributionAdvisor();
    return advisor.adviseUpdate(getEvent());
  }

  @Override
  protected void initMessage(CacheOperationMessage msg, DirectReplyProcessor pr) {
    super.initMessage(msg, pr);
    AbstractUpdateMessage m = (AbstractUpdateMessage) msg;
    DistributedRegion region = getRegion();
    DistributionManager mgr = region.getDistributionManager();
    // [bruce] We might have to stop using cacheTimeMillis because it causes a skew between
    // lastModified and the version tag's timestamp
    m.lastModified = this.lastModifiedTime;
  }

  private static final boolean ALWAYS_REPLICATE_UPDATES =
      Boolean.getBoolean("GemFire.ALWAYS_REPLICATE_UPDATES");

  /** @return whether we should do a local create for a remote one */
  private static boolean shouldDoRemoteCreate(LocalRegion rgn, EntryEventImpl ev) {
    DataPolicy dp = rgn.getAttributes().getDataPolicy();
    if (!rgn.isAllEvents() || (dp.withReplication() && rgn.isInitialized()
        && ev.getOperation().isUpdate() && !rgn.getConcurrencyChecksEnabled()
        // misordered CREATE and
        // UPDATE messages can
        // cause inconsistencies
        && !ALWAYS_REPLICATE_UPDATES)) {
      // we are not accepting all events
      // or we are a replicate and initialized and it was an update
      // (we exclude that latter to avoid resurrecting a key deleted in a replicate
      return false;
    } else {
      return true;
    }
  }

  private static boolean checkIfToUpdateAfterCreateFailed(LocalRegion rgn, EntryEventImpl ev) {
    // Try to create is failed due to found the entry exist, double check if should update
    boolean doUpdate = true;
    if (ev.oldValueIsDestroyedToken()) {
      if (rgn.getVersionVector() != null && ev.getVersionTag() != null) {
        rgn.getVersionVector().recordVersion(
            (InternalDistributedMember) ev.getDistributedMember(), ev.getVersionTag());
      }
      doUpdate = false;
    }
    if (ev.isConcurrencyConflict()) {
      if (logger.isDebugEnabled()) {
        logger.debug("basicUpdate failed with CME, not to retry:" + ev);
      }
      doUpdate = false;
    }
    return doUpdate;
  }

  /**
   * Does a remote update (could be create or put). This code was factored into a static for
   * QueuedOperation.
   *
   * @param rgn the region to do the update on
   * @param ev the event describing the operation
   * @param lastMod the modification timestamp for this op
   * @return true if the update was done.
   * @since GemFire 5.0
   */
  public static boolean doPutOrCreate(LocalRegion rgn, EntryEventImpl ev, long lastMod) {
    try {
      boolean updated = false;
      boolean doUpdate = true; // start with assumption we have key and need value
      boolean invokeCallbacks = true;

      if (shouldDoRemoteCreate(rgn, ev)) {
        if (logger.isDebugEnabled()) {
          logger.debug("doPutOrCreate: attempting to update or create entry");
        }
        final long startPut = rgn.getCachePerfStats().getTime();
        final boolean isBucket = rgn.isUsedForPartitionedRegionBucket();
        if (isBucket) {
          BucketRegion br = (BucketRegion) rgn;
          br.getPartitionedRegion().getPrStats().startApplyReplication();
        }
        try {
          // if the oldValue is the DESTROYED token and overwrite is disallowed,
          // then basicPut will set the blockedDestroyed flag in the event
          boolean overwriteDestroyed = ev.getOperation().isCreate();
          try {
            boolean firstBasicUpdateSuccess =
                rgn.basicUpdate(ev, true, false, lastMod, overwriteDestroyed, true, true);
            if (firstBasicUpdateSuccess) {
              rgn.getCachePerfStats().endPut(startPut, ev.isOriginRemote());
              // we did a create, or replayed a create event
              doUpdate = false;
              updated = true;
            } else {
              // already exists. If it was blocked by the DESTROYED token, then
              // do no update.
              doUpdate = checkIfToUpdateAfterCreateFailed(rgn, ev);
            }
          } catch (ConcurrentCacheModificationException ex) {
            invokeCallbacks = false;
            doUpdate = checkIfToUpdateAfterCreateFailed(rgn, ev);
          }
        } finally {
          if (isBucket) {
            BucketRegion br = (BucketRegion) rgn;
            br.getPartitionedRegion().getPrStats().endApplyReplication(startPut);
          }
        }
      }

      // If we care about the region entry being updated, get its value
      // from this message.
      if (doUpdate) {
        if (!ev.isLocalInvalid()) {
          final long startPut = rgn.getCachePerfStats().getTime();
          boolean overwriteDestroyed = ev.getOperation().isCreate();
          final boolean isBucket = rgn.isUsedForPartitionedRegionBucket();
          if (isBucket) {
            BucketRegion br = (BucketRegion) rgn;
            br.getPartitionedRegion().getPrStats().startApplyReplication();
          }
          try {
            boolean secondBasicUpdateSuccess;
            try {
              secondBasicUpdateSuccess =
                  rgn.basicUpdate(ev, false, true, lastMod, overwriteDestroyed,
                      invokeCallbacks, true);
            } catch (ConcurrentCacheModificationException ex) {
              secondBasicUpdateSuccess = false;
              invokeCallbacks = false;
            }
            if (secondBasicUpdateSuccess) {
              rgn.getCachePerfStats().endPut(startPut, ev.isOriginRemote());
              if (logger.isTraceEnabled()) {
                logger.trace("Processing put key {} in region {}", ev.getKey(), rgn.getFullPath());
              }
              updated = true;
            } else {
              // key not here, blocked by DESTROYED token or ConcurrentCacheModificationException
              // thrown during second update attempt
              if (rgn.isUsedForPartitionedRegionBucket()
                  || (rgn.getDataPolicy().withReplication() && rgn.getConcurrencyChecksEnabled())) {
                ev.makeCreate();
                boolean thirdBasicUpdateSuccess =
                    rgn.basicUpdate(ev, false, false, lastMod, true, invokeCallbacks, false);
                if (thirdBasicUpdateSuccess) {
                  rgn.getCachePerfStats().endPut(startPut, ev.isOriginRemote());
                  updated = true;
                }
              } else {
                if (rgn.getVersionVector() != null && ev.getVersionTag() != null) {
                  rgn.getVersionVector().recordVersion(
                      (InternalDistributedMember) ev.getDistributedMember(), ev.getVersionTag());
                }
                if (logger.isDebugEnabled()) {
                  logger.debug(
                      "While processing Update message, update not performed because this key is {}",
                      (ev.oldValueIsDestroyedToken() ? "blocked by DESTROYED/TOMBSTONE token"
                          : "not defined"));
                }
              }
            }
          } finally {
            if (isBucket) {
              BucketRegion br = (BucketRegion) rgn;
              br.getPartitionedRegion().getPrStats().endApplyReplication(startPut);
            }
          }
        } else { // supplied null, must be a create operation
          if (logger.isTraceEnabled()) {
            logger.trace("Processing create with null value provided, value not put");
          }
        }
      } else {
        if (rgn.getVersionVector() != null && ev.getVersionTag() != null
            && !ev.getVersionTag().isRecorded()) {
          rgn.getVersionVector().recordVersion(
              (InternalDistributedMember) ev.getDistributedMember(), ev.getVersionTag());
        }
        if (!updated) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "While processing Update message, update not performed because key was created but mirroring keys only and value not in update message, OR key was not new for sender and has been destroyed here");
          }
        }
      }
      return true;
    } catch (CacheWriterException e) {
      throw new Error("CacheWriter should not be called", e);
    } catch (TimeoutException e) {
      throw new Error(
          "DistributedLock should not be acquired",
          e);
    }
  }

  public abstract static class AbstractUpdateMessage extends CacheOperationMessage {
    protected long lastModified;

    @Override
    protected boolean operateOnRegion(CacheEvent event, ClusterDistributionManager dm)
        throws EntryNotFoundException {
      EntryEventImpl ev = (EntryEventImpl) event;
      DistributedRegion rgn = (DistributedRegion) ev.getRegion();
      DistributionManager mgr = dm;
      boolean sendReply = true; // by default tell caller to send ack

      // if (!rgn.hasSeenEvent((InternalCacheEvent)event)) {
      if (!rgn.isCacheContentProxy()) {
        basicOperateOnRegion(ev, rgn);
      }
      // }
      else {
        if (logger.isDebugEnabled()) {
          logger.debug("UpdateMessage: this cache has already seen this event {}", event);
        }
      }

      return sendReply;
    }

    // @todo darrel: make this method static?
    /**
     * Do the actual update after operationOnRegion has confirmed work needs to be done Note this is
     * the default implementation used by UpdateOperation. DistributedPutAllOperation overrides and
     * then calls back using super to this implementation. NOTE: be careful to not use methods like
     * getEvent(); defer to the ev passed as a parameter instead.
     */
    protected void basicOperateOnRegion(EntryEventImpl ev, DistributedRegion rgn) {
      if (logger.isDebugEnabled()) {
        logger.debug("Processing  {}", this);
      }
      try {
        long time = this.lastModified;
        if (ev.getVersionTag() != null) {
          checkVersionTag(rgn, ev.getVersionTag());
          time = ev.getVersionTag().getVersionTimeStamp();
        }
        this.appliedOperation = doPutOrCreate(rgn, ev, time);
      } catch (ConcurrentCacheModificationException e) {
        dispatchElidedEvent(rgn, ev);
        this.appliedOperation = false;
      }
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; lastModified=");
      buff.append(this.lastModified);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.lastModified = in.readLong();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeLong(this.lastModified);
    }

    protected void checkVersionTag(DistributedRegion rgn, VersionTag tag) {
      RegionAttributes attr = rgn.getAttributes();
      if (attr.getConcurrencyChecksEnabled() && attr.getDataPolicy().withPersistence()
          && attr.getScope() != Scope.GLOBAL
          && (tag.getMemberID() == null || test_InvalidVersion)) {

        if (logger.isDebugEnabled()) {
          logger.debug("Version tag is missing the memberID: {}", tag);
        }

        String msg =
            String.format("memberID cannot be null for persistent regions: %s", tag);
        RuntimeException ex = (sender.getVersion().isOlderThan(KnownVersion.GFE_80))
            ? new InternalGemFireException(msg) : new InvalidVersionException(msg);
        throw ex;
      }
    }

    @Override
    protected boolean mayNotifySerialGatewaySender(ClusterDistributionManager dm) {
      return notifiesSerialGatewaySender(dm);
    }
  }
}
