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

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.annotations.Retained;

/**
 * This operation updates Version stamp of an entry if entry is available and entry version stamp
 * has same DSID as in event's version tag.
 *
 *
 */
public class UpdateEntryVersionOperation extends DistributedCacheOperation {
  private static final Logger logger = LogService.getLogger();

  public UpdateEntryVersionOperation(CacheEvent event) {
    super(event);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.DistributedCacheOperation#createMessage()
   */
  @Override
  protected CacheOperationMessage createMessage() {
    return new UpdateEntryVersionMessage(event);
  }

  @Override
  protected void initMessage(CacheOperationMessage msg, DirectReplyProcessor p) {
    super.initMessage(msg, p);
    UpdateEntryVersionMessage imsg = (UpdateEntryVersionMessage) msg;
    EntryEventImpl eei = getEvent();
    imsg.key = eei.getKey();
    imsg.eventId = eei.getEventId();
    imsg.versionTag = eei.getVersionTag();
  }

  public static class UpdateEntryVersionMessage extends CacheOperationMessage {

    protected Object key;
    protected EventID eventId = null;
    protected EntryEventImpl event = null;
    private Long tailKey = 0L; // Used for Parallel Gateway Senders

    public UpdateEntryVersionMessage() {}

    public UpdateEntryVersionMessage(InternalCacheEvent ev) {
      this.event = (EntryEventImpl) ev;
    }

    @Override
    public int getDSFID() {
      return UPDATE_ENTRY_VERSION_MESSAGE;
    }

    @Override
    @Retained
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      @Retained
      EntryEventImpl ev = EntryEventImpl.create(rgn, getOperation(), this.key, null /* newValue */,
          this.callbackArg /* callbackArg */, true /* originRemote */ , getSender(),
          false /* generateCallbacks */);
      ev.setEventId(this.eventId);
      ev.setVersionTag(this.versionTag);
      ev.setTailKey(this.tailKey);

      return ev;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; key=");
      buff.append(this.key);
      if (this.eventId != null) {
        buff.append("; eventId=").append(this.eventId);
      }
    }

    @Override
    protected boolean operateOnRegion(CacheEvent event, ClusterDistributionManager dm)
        throws EntryNotFoundException {
      EntryEventImpl ev = (EntryEventImpl) event;
      DistributedRegion rgn = (DistributedRegion) ev.getRegion();

      try {
        if (!rgn.isCacheContentProxy()) {
          if (logger.isTraceEnabled()) {
            logger.trace("UpdateEntryVersionMessage.operationOnRegion; key={}", ev.getKey());
          }

          if (rgn.getConcurrencyChecksEnabled()) {
            rgn.basicUpdateEntryVersion(ev);
          }
        }

        this.appliedOperation = true;
        return true;
      } catch (ConcurrentCacheModificationException e) {
        if (logger.isTraceEnabled()) {
          logger.trace(
              "UpdateEntryVersionMessage.operationOnRegion; ConcurrentCacheModificationException occurred for key={}",
              ev.getKey());
        }
        return true; // concurrent modification problems are not reported to senders
      } catch (CacheWriterException e) {
        throw new Error("CacheWriter should not be called", e);
      } catch (TimeoutException e) {
        throw new Error(
            "DistributedLock should not be acquired",
            e);
      }
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.eventId = (EventID) DataSerializer.readObject(in);
      this.key = DataSerializer.readObject(in);
      Boolean hasTailKey = DataSerializer.readBoolean(in);
      if (hasTailKey.booleanValue()) {
        this.tailKey = DataSerializer.readLong(in);
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.eventId, out);
      DataSerializer.writeObject(this.key, out);

      DistributedRegion region = (DistributedRegion) this.event.getRegion();
      if (region instanceof BucketRegion) {
        PartitionedRegion pr = region.getPartitionedRegion();
        if (pr.isParallelWanEnabled()) {
          DataSerializer.writeBoolean(Boolean.TRUE, out);
          DataSerializer.writeLong(this.event.getTailKey(), out);
        } else {
          DataSerializer.writeBoolean(Boolean.FALSE, out);
        }
      } else if (((LocalRegion) region).isUsedForSerialGatewaySenderQueue()) {
        DataSerializer.writeBoolean(Boolean.TRUE, out);
        DataSerializer.writeLong(this.event.getTailKey(), out);
      } else {
        DataSerializer.writeBoolean(Boolean.FALSE, out);
      }
    }
  }
}
