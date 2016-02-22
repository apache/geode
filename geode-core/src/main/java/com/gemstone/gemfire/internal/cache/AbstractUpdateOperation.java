/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.InvalidVersionException;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.versions.ConcurrentCacheModificationException;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.util.DelayedAction;

/**
 * Common code for both UpdateOperation and DistributedPutAllOperation.
 *
 */
public abstract class AbstractUpdateOperation extends DistributedCacheOperation  {
  private static final Logger logger = LogService.getLogger();
  
  public static volatile boolean test_InvalidVersion;
  
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD",
      justification="test hook that is unset normally")
  public static volatile DelayedAction test_InvalidVersionAction;
  
  private final long lastModifiedTime;

  public AbstractUpdateOperation(CacheEvent event, long lastModifiedTime) {
    super(event);
    this.lastModifiedTime = lastModifiedTime;
  }
  
  @Override
  public void distribute() {
    try {
      super.distribute();
    } catch (InvalidVersionException e) {
      if (logger.isDebugEnabled()) {
        logger.trace(LogMarker.DM, "PutAll failed since versions were missing; retrying again", e);
      }
      
      if (test_InvalidVersionAction != null) {
        test_InvalidVersionAction.run();
      }
      super.distribute();
    }
  }

  @Override
  protected Set getRecipients() {
    CacheDistributionAdvisor advisor = getRegion().getCacheDistributionAdvisor();
    return advisor.adviseUpdate(getEvent());
  }

  @Override
  protected void initMessage(CacheOperationMessage msg, DirectReplyProcessor pr) {
    super.initMessage(msg, pr);
    AbstractUpdateMessage m = (AbstractUpdateMessage)msg;
    DistributedRegion region = getRegion();
    DM mgr = region.getDistributionManager();
    // [bruce] We might have to stop using cacheTimeMillis because it causes a skew between
    // lastModified and the version tag's timestamp
    m.lastModified = this.lastModifiedTime;
  }

  private static final boolean ALWAYS_REPLICATE_UPDATES = Boolean.getBoolean("GemFire.ALWAYS_REPLICATE_UPDATES");
  
  /** @return whether we should do a local create for a remote one */
  private static final boolean shouldDoRemoteCreate(LocalRegion rgn, EntryEventImpl ev) {
    DataPolicy dp = rgn.getAttributes().getDataPolicy();
    if (!rgn.isAllEvents()
        || (dp.withReplication()
            && rgn.isInitialized()
            && ev.getOperation().isUpdate()
            && !rgn.concurrencyChecksEnabled // misordered CREATE and UPDATE messages can cause inconsistencies
            && !ALWAYS_REPLICATE_UPDATES))
    {
      // we are not accepting all events
      // or we are a replicate and initialized and it was an update
      // (we exclude that latter to avoid resurrecting a key deleted in a replicate
      return false;
    } else {
      return true;
    }
  }
  /**
   * Does a remote update (could be create or put).
   * This code was factored into a static for QueuedOperation.
   * @param rgn the region to do the update on
   * @param ev the event describing the operation
   * @param lastMod the modification timestamp for this op
   * @return true if the update was done.
   * @since 5.0
   */
  public static boolean doPutOrCreate(LocalRegion rgn, EntryEventImpl ev, long lastMod) {
    try {
      boolean updated = false;
      boolean doUpdate = true; // start with assumption we have key and need value
      if (shouldDoRemoteCreate(rgn, ev)) {
        if (logger.isDebugEnabled()) {
          logger.debug("doPutOrCreate: attempting to create entry");
        }
        final long startPut = CachePerfStats.getStatTime();
        final boolean isBucket = rgn.isUsedForPartitionedRegionBucket();
        if (isBucket) {
          BucketRegion br = (BucketRegion)rgn;
          br.getPartitionedRegion().getPrStats().startApplyReplication();
        }
        try {
        // if the oldValue is the DESTROYED token and overwrite is disallowed,
        // then basicPut will set the blockedDestroyed flag in the event
        boolean overwriteDestroyed = ev.getOperation().isCreate();
        if (rgn.basicUpdate(ev, true /*ifNew*/, false/*ifOld*/, lastMod, overwriteDestroyed)) {
          rgn.getCachePerfStats().endPut(startPut, ev.isOriginRemote());
          // we did a create, or replayed a create event
          doUpdate = false;
          updated = true;
        }
        else { // already exists. If it was blocked by the DESTROYED token, then
          // do no update.
          if (ev.oldValueIsDestroyedToken()) {
            if (rgn.getVersionVector() != null && ev.getVersionTag() != null) {
                rgn.getVersionVector().recordVersion(
                    (InternalDistributedMember) ev.getDistributedMember(),
                    ev.getVersionTag());
            }
            doUpdate = false;
          }
        }
        } finally {
          if (isBucket) {
            BucketRegion br = (BucketRegion)rgn;
            br.getPartitionedRegion().getPrStats().endApplyReplication(startPut);
          }
        }
      }

      // If we care about the region entry being updated, get its value
      // from this message.
      if (doUpdate) {
        if (!ev.isLocalInvalid()) {
          final long startPut = CachePerfStats.getStatTime();
          boolean overwriteDestroyed = ev.getOperation().isCreate();
          final boolean isBucket = rgn.isUsedForPartitionedRegionBucket();
          if (isBucket) {
            BucketRegion br = (BucketRegion)rgn;
            br.getPartitionedRegion().getPrStats().startApplyReplication();
          }
          try {
          if (rgn.basicUpdate(ev, false/*ifNew*/, true/*ifOld*/, lastMod, overwriteDestroyed)) {
            rgn.getCachePerfStats().endPut(startPut, ev.isOriginRemote());
            if (logger.isTraceEnabled()) {
              logger.trace("Processing put key {} in region {}", ev.getKey(), rgn.getFullPath());
            }
            updated = true;
          }
          else { // key not here or blocked by DESTROYED token
            if (rgn.isUsedForPartitionedRegionBucket()
                || (rgn.dataPolicy.withReplication() && rgn.getConcurrencyChecksEnabled())) {
              overwriteDestroyed = true;
              ev.makeCreate();
              rgn.basicUpdate(ev, true /*ifNew*/, false/*ifOld*/, lastMod, overwriteDestroyed);
              rgn.getCachePerfStats().endPut(startPut, ev.isOriginRemote());
              updated = true;
            } else {
              if (rgn.getVersionVector() != null && ev.getVersionTag() != null) {
                rgn.getVersionVector().recordVersion((InternalDistributedMember) ev.getDistributedMember(), ev.getVersionTag());
              }
              if (logger.isDebugEnabled()) {
                logger.debug("While processing Update message, update not performed because this key is {}",
                  (ev.oldValueIsDestroyedToken() ? "blocked by DESTROYED/TOMBSTONE token" : "not defined"));
              }
            }
          }
          } finally {
            if (isBucket) {
              BucketRegion br = (BucketRegion)rgn;
              br.getPartitionedRegion().getPrStats().endApplyReplication(startPut);
            }
          }
        }
        else { // supplied null, must be a create operation
          if (logger.isTraceEnabled()) {
            logger.trace("Processing create with null value provided, value not put");
          }
        }
      }
      else {
        if (rgn.getVersionVector() != null && ev.getVersionTag() != null && !ev.getVersionTag().isRecorded()) {
          rgn.getVersionVector().recordVersion((InternalDistributedMember) ev.getDistributedMember(), ev.getVersionTag());
        }
        if (!updated) {
          if (logger.isDebugEnabled()) {
            logger.debug("While processing Update message, update not performed because key was created but mirroring keys only and value not in update message, OR key was not new for sender and has been destroyed here");
          }
        }
      }
      return true;
    }
    catch (CacheWriterException e) {
      throw new Error(LocalizedStrings.AbstractUpdateOperation_CACHEWRITER_SHOULD_NOT_BE_CALLED.toLocalizedString(), e);
    }
    catch (TimeoutException e) {
      throw new Error(LocalizedStrings.AbstractUpdateOperation_DISTRIBUTEDLOCK_SHOULD_NOT_BE_ACQUIRED.toLocalizedString(), e);
    }
  }

  public static abstract class AbstractUpdateMessage extends CacheOperationMessage  {
    protected long lastModified;

    @Override
    protected boolean operateOnRegion(CacheEvent event, DistributionManager dm) throws EntryNotFoundException {
      EntryEventImpl ev = (EntryEventImpl)event;
      DistributedRegion rgn = (DistributedRegion)ev.region;
      DM mgr = dm;
      boolean sendReply = true; // by default tell caller to send ack
      
//      if (!rgn.hasSeenEvent((InternalCacheEvent)event)) {
        if (!rgn.isCacheContentProxy()) {
          basicOperateOnRegion(ev, rgn);
        }
//      }
      else {
        if (logger.isDebugEnabled()) {
          logger.debug("UpdateMessage: this cache has already seen this event {}", event);
        }
      }
      
      return sendReply;
    }

    // @todo darrel: make this method static?
    /**
     * Do the actual update after operationOnRegion has confirmed work needs to be done
     * Note this is the default implementation used by UpdateOperation.
     *   DistributedPutAllOperation overrides and then calls back using super
     *   to this implementation.
     * NOTE: be careful to not use methods like getEvent(); defer to
     *   the ev passed as a parameter instead.
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
      }
      catch (ConcurrentCacheModificationException e) {
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
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.lastModified = in.readLong();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeLong(this.lastModified);
    }

    protected void checkVersionTag(DistributedRegion rgn, VersionTag tag) {
      RegionAttributes attr = rgn.getAttributes();
      if (attr.getConcurrencyChecksEnabled() 
          && attr.getDataPolicy().withPersistence() 
          && attr.getScope() != Scope.GLOBAL
          && (tag.getMemberID() == null || test_InvalidVersion)) {

        if (logger.isDebugEnabled()) {
          logger.debug("Version tag is missing the memberID: {}", tag);
        }
        
        String msg = LocalizedStrings.DistributedPutAllOperation_MISSING_VERSION
            .toLocalizedString(tag);
        RuntimeException ex = (sender.getVersionObject().compareTo(Version.GFE_80) < 0) 
            ? new InternalGemFireException(msg) 
            : new InvalidVersionException(msg);
        throw ex;
      }
    }
    
    @Override
    protected boolean mayAddToMultipleSerialGateways(DistributionManager dm) {
      return _mayAddToMultipleSerialGateways(dm);
    }
  }
}
