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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.persistence.PersistentReplicatesOfflineException;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.partitioned.RemoveAllPRMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.RemoteRemoveAllMessage;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Handles distribution of a Region.removeAll operation.
 *
 * TODO: extend DistributedCacheOperation instead of AbstractUpdateOperation
 *
 * @since GemFire 8.1
 */
public class DistributedRemoveAllOperation extends AbstractUpdateOperation {
  private static final Logger logger = LogService.getLogger();

  /**
   * Release is called by freeOffHeapResources.
   */
  @Retained
  protected final RemoveAllEntryData[] removeAllData;

  public int removeAllDataSize;

  protected boolean isBridgeOp = false;

  static final byte USED_FAKE_EVENT_ID = 0x01;
  static final byte NOTIFY_ONLY = 0x02;
  static final byte FILTER_ROUTING = 0x04;
  static final byte VERSION_TAG = 0x08;
  static final byte POSDUP = 0x10;
  static final byte PERSISTENT_TAG = 0x20;
  static final byte HAS_CALLBACKARG = 0x40;
  static final byte HAS_TAILKEY = (byte) 0x80;

  public DistributedRemoveAllOperation(CacheEvent event, int size, boolean isBridgeOp) {
    super(event, ((EntryEventImpl) event).getEventTime(0L));
    this.removeAllData = new RemoveAllEntryData[size];
    this.removeAllDataSize = 0;
    this.isBridgeOp = isBridgeOp;
  }

  /**
   * return if the operation is bridge operation
   */
  public boolean isBridgeOperation() {
    return this.isBridgeOp;
  }

  public RemoveAllEntryData[] getRemoveAllEntryData() {
    return removeAllData;
  }

  public void setRemoveAllEntryData(RemoveAllEntryData[] removeAllEntryData) {
    for (int i = 0; i < removeAllEntryData.length; i++) {
      removeAllData[i] = removeAllEntryData[i];
    }
    this.removeAllDataSize = removeAllEntryData.length;
  }

  /**
   * Add an entry that this removeAll operation should distribute.
   */
  public void addEntry(RemoveAllEntryData removeAllEntry) {
    this.removeAllData[this.removeAllDataSize] = removeAllEntry;
    this.removeAllDataSize += 1;
  }

  /**
   * Add an entry that this removeAll operation should distribute.
   */
  public void addEntry(EntryEventImpl ev) {
    this.removeAllData[this.removeAllDataSize] = new RemoveAllEntryData(ev);
    this.removeAllDataSize += 1;
  }

  /**
   * Add an entry that this removeAll operation should distribute. This method is for a special
   * case: the callback will be called after this in hasSeenEvent() case, so we should change the
   * status beforehand
   */
  public void addEntry(EntryEventImpl ev, boolean newCallbackInvoked) {
    this.removeAllData[this.removeAllDataSize] = new RemoveAllEntryData(ev);
    this.removeAllData[this.removeAllDataSize].setCallbacksInvoked(newCallbackInvoked);
    this.removeAllDataSize += 1;
  }

  /**
   * Add an entry for PR bucket's msg.
   *
   * @param ev event to be added
   * @param bucketId message is for this bucket
   */
  public void addEntry(EntryEventImpl ev, Integer bucketId) {
    this.removeAllData[this.removeAllDataSize] = new RemoveAllEntryData(ev);
    this.removeAllData[this.removeAllDataSize].setBucketId(bucketId);
    this.removeAllDataSize += 1;
  }

  /**
   * set using fake thread id
   *
   * @param status whether the entry is using fake event id
   */
  public void setUseFakeEventId(boolean status) {
    for (int i = 0; i < removeAllDataSize; i++) {
      removeAllData[i].setUsedFakeEventId(status);
    }
  }

  /**
   * In the originating cache, this returns an iterator on the list of events caused by the
   * removeAll operation. This is cached for listener notification purposes. The iterator is
   * guaranteed to return events in the order they are present in putAllData[]
   */
  public Iterator eventIterator() {
    return new Iterator() {
      int position = 0;

      @Override
      public boolean hasNext() {
        return DistributedRemoveAllOperation.this.removeAllDataSize > position;
      };

      @Override
      @Unretained
      public Object next() {
        @Unretained
        EntryEventImpl ev = getEventForPosition(position);
        position++;
        return ev;
      };

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      };
    };
  }

  public void freeOffHeapResources() {
    // I do not use eventIterator here because it forces the lazy creation of EntryEventImpl by
    // calling getEventForPosition.
    for (int i = 0; i < this.removeAllDataSize; i++) {
      RemoveAllEntryData entry = this.removeAllData[i];
      if (entry != null && entry.event != null) {
        entry.event.release();
      }
    }
  }

  @Unretained
  public EntryEventImpl getEventForPosition(int position) {
    RemoveAllEntryData entry = this.removeAllData[position];
    if (entry == null) {
      return null;
    }
    if (entry.event != null) {
      return entry.event;
    }
    LocalRegion region = (LocalRegion) this.event.getRegion();
    // owned by this.removeAllData once entry.event = ev is done
    @Retained
    EntryEventImpl ev = EntryEventImpl.create(region, entry.getOp(), entry.getKey(),
        null/* value */, this.event.getCallbackArgument(), false /* originRemote */,
        this.event.getDistributedMember(), this.event.isGenerateCallbacks(), entry.getEventID());
    boolean returnedEv = false;
    try {
      ev.setPossibleDuplicate(entry.isPossibleDuplicate());
      ev.setIsRedestroyedEntry(entry.getRedestroyedEntry());
      if (entry.versionTag != null && region.getConcurrencyChecksEnabled()) {
        VersionSource id = entry.versionTag.getMemberID();
        if (id != null) {
          entry.versionTag.setMemberID(ev.getRegion().getVersionVector().getCanonicalId(id));
        }
        ev.setVersionTag(entry.versionTag);
      }

      entry.event = ev;
      returnedEv = true;
      ev.setOldValue(entry.getOldValue());
      CqService cqService = region.getCache().getCqService();
      if (cqService.isRunning() && !entry.getOp().isCreate() && !ev.hasOldValue()) {
        ev.setOldValueForQueryProcessing();
      }
      ev.setInvokePRCallbacks(!entry.isNotifyOnly());
      if (getBaseEvent().getContext() != null) {
        ev.setContext(getBaseEvent().getContext());
      }
      ev.callbacksInvoked(entry.isCallbacksInvoked());
      ev.setTailKey(entry.getTailKey());
      return ev;
    } finally {
      if (!returnedEv) {
        ev.release();
      }
    }
  }

  public EntryEventImpl getBaseEvent() {
    return getEvent();
  }

  /**
   * Data that represents a single entry being RemoveAll'd.
   */
  public static class RemoveAllEntryData {

    final Object key;

    private final Object oldValue;

    private final Operation op;

    private EventID eventID;

    transient EntryEventImpl event;

    private Integer bucketId = Integer.valueOf(-1);

    protected transient boolean callbacksInvoked = false;

    public FilterRoutingInfo filterRouting;

    // One flags byte for all booleans
    protected byte flags = 0x00;

    // TODO: Yogesh, this should be intialized and sent on wire only when
    // parallel wan is enabled
    private Long tailKey = 0L;

    public VersionTag versionTag;

    transient boolean inhibitDistribution;

    transient boolean redestroyedEntry;

    /**
     * Constructor to use when preparing to send putall data out
     */
    public RemoveAllEntryData(EntryEventImpl event) {

      this.key = event.getKey();
      Object oldValue = event.getRawOldValue();

      if (oldValue == Token.NOT_AVAILABLE || Token.isRemoved(oldValue)) {
        this.oldValue = null;
      } else {
        this.oldValue = oldValue;
      }

      this.op = event.getOperation();
      this.eventID = event.getEventId();
      this.tailKey = event.getTailKey();
      this.versionTag = event.getVersionTag();

      setNotifyOnly(!event.getInvokePRCallbacks());
      setCallbacksInvoked(event.callbacksInvoked());
      setPossibleDuplicate(event.isPossibleDuplicate());
      setInhibitDistribution(event.getInhibitDistribution());
      setRedestroyedEntry(event.getIsRedestroyedEntry());
    }

    /**
     * Constructor to use when receiving a putall from someone else
     */
    public RemoveAllEntryData(DataInput in, EventID baseEventID, int idx, Version version,
        ByteArrayDataInput bytesIn,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      this.key = context.getDeserializer().readObject(in);
      this.oldValue = null;
      this.op = Operation.fromOrdinal(in.readByte());
      this.flags = in.readByte();
      if ((this.flags & FILTER_ROUTING) != 0) {
        this.filterRouting = (FilterRoutingInfo) context.getDeserializer().readObject(in);
      }
      if ((this.flags & VERSION_TAG) != 0) {
        boolean persistentTag = (this.flags & PERSISTENT_TAG) != 0;
        this.versionTag = VersionTag.create(persistentTag, in);
      }
      if (isUsedFakeEventId()) {
        this.eventID = new EventID();
        InternalDataSerializer.invokeFromData(this.eventID, in);
      } else {
        this.eventID = new EventID(baseEventID, idx);
      }
      if ((this.flags & HAS_TAILKEY) != 0) {
        this.tailKey = DataSerializer.readLong(in);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(50);
      sb.append("(").append(getKey()).append(",").append(getOldValue());
      if (this.bucketId > 0) {
        sb.append(", b").append(this.bucketId);
      }
      if (versionTag != null) {
        sb.append(",v").append(versionTag.getEntryVersion())
            .append(",rv=" + versionTag.getRegionVersion());
      }
      if (filterRouting != null) {
        sb.append(", ").append(filterRouting);
      }
      sb.append(")");
      return sb.toString();
    }

    void setSender(InternalDistributedMember sender) {
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(sender);
      }
    }

    /**
     * Used to serialize this instances data to <code>out</code>. If changes are made to this method
     * make sure that it is backwards compatible by creating toDataPreXX methods. Also make sure
     * that the callers to this method are backwards compatible by creating toDataPreXX methods for
     * them even if they are not changed. <br>
     * Callers for this method are: <br>
     * {@link DataSerializableFixedID#toData(DataOutput, SerializationContext)} <br>
     * {@link DataSerializableFixedID#toData(DataOutput, SerializationContext)} <br>
     * {@link DataSerializableFixedID#toData(DataOutput, SerializationContext)} <br>
     */
    public void serializeTo(final DataOutput out,
        SerializationContext context) throws IOException {
      Object key = this.key;
      context.getSerializer().writeObject(key, out);

      out.writeByte(this.op.ordinal);
      byte bits = this.flags;
      if (this.filterRouting != null)
        bits |= FILTER_ROUTING;
      if (this.versionTag != null) {
        bits |= VERSION_TAG;
        if (this.versionTag instanceof DiskVersionTag) {
          bits |= PERSISTENT_TAG;
        }
      }
      // TODO: Yogesh, this should be conditional,
      // make sure that we sent it on wire only
      // when parallel wan is enabled
      bits |= HAS_TAILKEY;
      out.writeByte(bits);

      if (this.filterRouting != null) {
        context.getSerializer().writeObject(this.filterRouting, out);
      }
      if (this.versionTag != null) {
        InternalDataSerializer.invokeToData(this.versionTag, out);
      }
      if (isUsedFakeEventId()) {
        // fake event id should be serialized
        InternalDataSerializer.invokeToData(this.eventID, out);
      }
      // TODO: Yogesh, this should be conditional,
      // make sure that we sent it on wire only
      // when parallel wan is enabled
      DataSerializer.writeLong(this.tailKey, out);
    }

    /**
     * Returns the key
     */
    public Object getKey() {
      return this.key;
    }

    /**
     * Returns the old value
     */
    public Object getOldValue() {
      return this.oldValue;
    }

    public Long getTailKey() {
      return this.tailKey;
    }

    public void setTailKey(Long key) {
      this.tailKey = key;
    }

    /**
     * Returns the operation
     */
    public Operation getOp() {
      return this.op;
    }

    public EventID getEventID() {
      return this.eventID;
    }

    /**
     * change event id for the entry
     *
     * @param eventId new event id
     */
    public void setEventId(EventID eventId) {
      this.eventID = eventId;
    }

    /**
     * change bucket id for the entry
     *
     * @param bucketId new bucket id
     */
    public void setBucketId(Integer bucketId) {
      this.bucketId = bucketId;
    }

    /**
     * get bucket id for the entry
     *
     * @return bucket id
     */
    public Integer getBucketId() {
      return this.bucketId;
    }

    /**
     * change event id into fake event id The algorithm is to change the threadid into
     * bucketid*MAX_THREAD_PER_CLIENT+oldthreadid. So from the log, we can derive the original
     * thread id.
     *
     * @return wether current event id is fake or not new bucket id
     */
    public boolean setFakeEventID() {
      if (bucketId.intValue() < 0) {
        return false;
      }

      if (!isUsedFakeEventId()) {
        // assign a fake big thread id. bucket id starts from 0. In order to distinguish
        // with other read thread, let bucket id starts from 1 in fake thread id
        long threadId = ThreadIdentifier.createFakeThreadIDForBulkOp(bucketId.intValue(),
            eventID.getThreadID());
        this.eventID = new EventID(eventID.getMembershipID(), threadId, eventID.getSequenceID());
        this.setUsedFakeEventId(true);
      }
      return true;
    }

    public boolean isUsedFakeEventId() {
      return (flags & USED_FAKE_EVENT_ID) != 0;
    }

    public void setUsedFakeEventId(boolean usedFakeEventId) {
      if (usedFakeEventId) {
        flags |= USED_FAKE_EVENT_ID;
      } else {
        flags &= ~(USED_FAKE_EVENT_ID);
      }
    }

    public boolean isNotifyOnly() {
      return (flags & NOTIFY_ONLY) != 0;
    }

    public void setNotifyOnly(boolean notifyOnly) {
      if (notifyOnly) {
        flags |= NOTIFY_ONLY;
      } else {
        flags &= ~(NOTIFY_ONLY);
      }
    }

    boolean isPossibleDuplicate() {
      return (this.flags & POSDUP) != 0;
    }

    public void setPossibleDuplicate(boolean possibleDuplicate) {
      if (possibleDuplicate) {
        flags |= POSDUP;
      } else {
        flags &= ~(POSDUP);
      }
    }

    public boolean isInhibitDistribution() {
      return this.inhibitDistribution;
    }

    public void setInhibitDistribution(boolean inhibitDistribution) {
      this.inhibitDistribution = inhibitDistribution;
    }

    public boolean getRedestroyedEntry() {
      return redestroyedEntry;
    }

    public void setRedestroyedEntry(boolean redestroyedEntry) {
      this.redestroyedEntry = redestroyedEntry;
    }

    public boolean isCallbacksInvoked() {
      return this.callbacksInvoked;
    }

    public void setCallbacksInvoked(boolean callbacksInvoked) {
      this.callbacksInvoked = callbacksInvoked;
    }
  }

  @Override
  protected FilterRoutingInfo getRecipientFilterRouting(Set cacheOpRecipients) {
    // for removeAll, we need to determine the routing information for each event and
    // create a consolidated routing object representing all events that can be
    // used for distribution
    CacheDistributionAdvisor advisor;
    LocalRegion region = (LocalRegion) this.event.getRegion();
    if (region instanceof PartitionedRegion) {
      advisor = ((PartitionedRegion) region).getCacheDistributionAdvisor();
    } else if (region.isUsedForPartitionedRegionBucket()) {
      advisor = ((BucketRegion) region).getPartitionedRegion().getCacheDistributionAdvisor();
    } else {
      advisor = ((DistributedRegion) region).getCacheDistributionAdvisor();
    }
    FilterRoutingInfo consolidated = new FilterRoutingInfo();
    for (int i = 0; i < this.removeAllData.length; i++) {
      @Unretained
      EntryEventImpl ev = getEventForPosition(i);
      if (ev != null) {
        FilterRoutingInfo eventRouting = advisor.adviseFilterRouting(ev, cacheOpRecipients);
        if (eventRouting != null) {
          consolidated.addFilterInfo(eventRouting);
        }
        removeAllData[i].filterRouting = eventRouting;
      }
    }
    // we need to create routing information for each PUT event
    return consolidated;
  }


  @Override
  protected FilterInfo getLocalFilterRouting(FilterRoutingInfo frInfo) {
    FilterProfile fp = getRegion().getFilterProfile();
    if (fp == null) {
      return null;
    }

    // this will set the local FilterInfo in the events
    if (this.removeAllData != null && this.removeAllData.length > 0) {
      fp.getLocalFilterRoutingForRemoveAllOp(this, this.removeAllData);
    }

    return null;
  }

  @Override
  protected CacheOperationMessage createMessage() {
    EntryEventImpl event = getBaseEvent();
    RemoveAllMessage msg = new RemoveAllMessage();
    msg.eventId = event.getEventId();
    msg.context = event.getContext();
    return msg;
  }

  /**
   * Create RemoveAllPRMessage for notify only (to adjunct nodes)
   *
   * @param bucketId create message to send to this bucket
   */
  public RemoveAllPRMessage createPRMessagesNotifyOnly(int bucketId) {
    final EntryEventImpl event = getBaseEvent();
    RemoveAllPRMessage prMsg = new RemoveAllPRMessage(bucketId, removeAllDataSize, true,
        event.isPossibleDuplicate(), !event.isGenerateCallbacks(), event.getCallbackArgument());
    if (event.getContext() != null) {
      prMsg.setBridgeContext(event.getContext());
    }

    // will not recover event id here
    for (int i = 0; i < removeAllDataSize; i++) {
      prMsg.addEntry(removeAllData[i]);
    }

    return prMsg;
  }

  /**
   * Create RemoveAllPRMessages for primary buckets out of this op
   *
   * @return a HashMap contain RemoveAllPRMessages, key is bucket id
   */
  public HashMap<Integer, RemoveAllPRMessage> createPRMessages() {
    // getFilterRecipients(Collections.EMPTY_SET); // establish filter recipient routing information
    HashMap<Integer, RemoveAllPRMessage> prMsgMap = new HashMap<Integer, RemoveAllPRMessage>();
    final EntryEventImpl event = getBaseEvent();

    for (int i = 0; i < removeAllDataSize; i++) {
      Integer bucketId = removeAllData[i].getBucketId();
      RemoveAllPRMessage prMsg = prMsgMap.get(bucketId);
      if (prMsg == null) {
        prMsg = new RemoveAllPRMessage(bucketId.intValue(), removeAllDataSize, false,
            event.isPossibleDuplicate(), !event.isGenerateCallbacks(), event.getCallbackArgument());
        prMsg
            .setTransactionDistributed(event.getRegion().getCache().getTxManager().isDistributed());

        // set dpao's context(original sender) into each PutAllMsg
        // dpao's event's context could be null if it's P2P putAll in PR
        if (event.getContext() != null) {
          prMsg.setBridgeContext(event.getContext());
        }
      }

      // Modify the event id, assign new thread id and new sequence id
      // We have to set fake event id here, because we cannot derive old event id from baseId+idx as
      // we
      // did in DR's PutAllMessage.
      removeAllData[i].setFakeEventID();
      // we only save the reference in prMsg. No duplicate copy
      prMsg.addEntry(removeAllData[i]);
      prMsgMap.put(bucketId, prMsg);
    }
    return prMsgMap;
  }

  @Override
  protected void initMessage(CacheOperationMessage msg, DirectReplyProcessor proc) {
    super.initMessage(msg, proc);
    RemoveAllMessage m = (RemoveAllMessage) msg;

    // if concurrency checks are enabled and this is not a replicated
    // region we need to see if any of the entries have no versions and,
    // if so, cull them out and send a 1-hop message to a replicate that
    // can generate a version for the operation

    RegionAttributes attr = this.event.getRegion().getAttributes();
    if (attr.getConcurrencyChecksEnabled() && !attr.getDataPolicy().withReplication()
        && attr.getScope() != Scope.GLOBAL) {
      if (attr.getDataPolicy() == DataPolicy.EMPTY) {
        // all entries are without version tags
        boolean success = RemoteRemoveAllMessage.distribute((EntryEventImpl) this.event,
            this.removeAllData, this.removeAllDataSize);
        if (success) {
          m.callbackArg = this.event.getCallbackArgument();
          m.removeAllData = new RemoveAllEntryData[0];
          m.removeAllDataSize = 0;
          m.skipCallbacks = !event.isGenerateCallbacks();

          return;

        } else if (!getRegion().getGenerateVersionTag()) {
          // Fix for #45934. We can't continue if we need versions and we failed
          // to distribute versionless entries.
          throw new PersistentReplicatesOfflineException();
        }
      } else {
        // some entries may have Create ops - these will not have version tags
        RemoveAllEntryData[] versionless = selectVersionlessEntries();
        if (logger.isTraceEnabled()) {
          logger.trace("Found these versionless entries: {}", Arrays.toString(versionless));
        }
        if (versionless.length > 0) {
          boolean success = RemoteRemoveAllMessage.distribute((EntryEventImpl) this.event,
              versionless, versionless.length);
          if (success) {
            versionless = null;
            RemoveAllEntryData[] versioned = selectVersionedEntries();
            if (logger.isTraceEnabled()) {
              logger.trace("Found these remaining versioned entries: {}",
                  Arrays.toString(versioned));
            }
            m.callbackArg = this.event.getCallbackArgument();
            m.removeAllData = versioned;
            m.removeAllDataSize = versioned.length;
            m.skipCallbacks = !event.isGenerateCallbacks();
            return;

          } else if (!getRegion().getGenerateVersionTag()) {
            // Fix for #45934. We can't continue if we need versions and we failed
            // to distribute versionless entries.
            throw new PersistentReplicatesOfflineException();
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("All entries have versions, so using normal DPAO message");
          }
        }
      }
    }
    m.callbackArg = this.event.getCallbackArgument();
    m.removeAllData = this.removeAllData;
    m.removeAllDataSize = this.removeAllDataSize;
    m.skipCallbacks = !event.isGenerateCallbacks();
  }


  @Override
  protected boolean shouldAck() {
    // bug #45704 - RemotePutAllOp's DPAO in another server conflicts with lingering DPAO from same
    // thread, so
    // we require an ACK if concurrency checks are enabled to make sure that the previous op has
    // finished first.
    return super.shouldAck() || getRegion().getConcurrencyChecksEnabled();
  }

  private RemoveAllEntryData[] selectVersionlessEntries() {
    int resultSize = this.removeAllData.length;
    for (int i = 0; i < this.removeAllData.length; i++) {
      RemoveAllEntryData p = this.removeAllData[i];
      if (p == null || p.isInhibitDistribution()) {
        resultSize--;
      } else if (p.versionTag != null && p.versionTag.hasValidVersion()) {
        resultSize--;
      }
    }
    RemoveAllEntryData[] result = new RemoveAllEntryData[resultSize];
    int ri = 0;
    for (int i = 0; i < this.removeAllData.length; i++) {
      RemoveAllEntryData p = this.removeAllData[i];
      if (p == null || p.isInhibitDistribution()) {
        continue; // skip blanks
      } else if (p.versionTag != null && p.versionTag.hasValidVersion()) {
        continue; // skip versioned
      }
      // what remains is versionless
      result[ri++] = p;
    }
    return result;
  }

  private RemoveAllEntryData[] selectVersionedEntries() {
    int resultSize = 0;
    for (int i = 0; i < this.removeAllData.length; i++) {
      RemoveAllEntryData p = this.removeAllData[i];
      if (p == null || p.isInhibitDistribution()) {
        continue; // skip blanks
      } else if (p.versionTag != null && p.versionTag.hasValidVersion()) {
        resultSize++;
      }
    }
    RemoveAllEntryData[] result = new RemoveAllEntryData[resultSize];
    int ri = 0;
    for (int i = 0; i < this.removeAllData.length; i++) {
      RemoveAllEntryData p = this.removeAllData[i];
      if (p == null || p.isInhibitDistribution()) {
        continue; // skip blanks
      } else if (p.versionTag != null && p.versionTag.hasValidVersion()) {
        result[ri++] = p;
      }
    }
    return result;
  }

  /**
   * version tags are gathered from local operations and remote operation responses. This method
   * gathers all of them and stores them in the given list.
   *
   */
  protected void fillVersionedObjectList(VersionedObjectList list) {
    for (RemoveAllEntryData entry : this.removeAllData) {
      if (entry.versionTag != null) {
        list.addKeyAndVersion(entry.key, entry.versionTag);
      }
    }
  }


  public static class RemoveAllMessage extends AbstractUpdateMessage // TODO extend
                                                                     // CacheOperationMessage
                                                                     // instead
  {

    protected RemoveAllEntryData[] removeAllData;

    protected int removeAllDataSize;

    protected transient ClientProxyMembershipID context;

    protected boolean skipCallbacks;

    protected EventID eventId = null;

    protected static final short HAS_BRIDGE_CONTEXT = UNRESERVED_FLAGS_START;
    protected static final short SKIP_CALLBACKS = (short) (HAS_BRIDGE_CONTEXT << 1);

    /** test to see if this message holds any data */
    public boolean isEmpty() {
      return this.removeAllData.length == 0;
    }

    /**
     * Note this this is a "dummy" event since this message contains a list of entries each one of
     * which has its own event. The key thing needed in this event is the region. This is the event
     * that gets passed to basicOperateOnRegion
     */
    @Override
    @Retained
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      // Gester: We have to specify eventId for the message of MAP
      @Retained
      EntryEventImpl event = EntryEventImpl.create(rgn, Operation.REMOVEALL_DESTROY, null /* key */,
          null/* value */, this.callbackArg, true /* originRemote */, getSender());
      if (this.context != null) {
        event.context = this.context;
      }
      event.setPossibleDuplicate(this.possibleDuplicate);
      event.setEventId(this.eventId);
      return event;
    }

    @Override
    public void appendFields(StringBuilder sb) {
      super.appendFields(sb);
      if (eventId != null) {
        sb.append("; eventId=").append(this.eventId);
      }
      sb.append("; entries=").append(this.removeAllDataSize);
      if (removeAllDataSize <= 20) {
        // 20 is a size for test
        sb.append("; entry values=").append(Arrays.toString(this.removeAllData));
      }
    }

    /**
     * Does the "remove" of one entry for a "removeAll" operation. Note it calls back to
     * AbstractUpdateOperation.UpdateMessage#basicOperationOnRegion
     *
     * @param entry the entry being removed
     * @param rgn the region the entry is removed from
     */
    public void doEntryRemove(RemoveAllEntryData entry, DistributedRegion rgn) {
      @Released
      EntryEventImpl ev = RemoveAllMessage.createEntryEvent(entry, getSender(), this.context, rgn,
          this.possibleDuplicate, this.needsRouting, this.callbackArg, true, skipCallbacks);
      // rgn.getLogWriterI18n().info(String.format("%s", "RemoveAllMessage.doEntryRemove
      // sender=" + getSender() +
      // " event="+ev));
      // we don't need to set old value here, because the msg is from remote. local old value will
      // get from next step
      try {
        if (ev.getVersionTag() != null) {
          checkVersionTag(rgn, ev.getVersionTag());
        }
        // TODO check all removeAll basicDestroy calls done on the farside and make sure
        // "cacheWrite" is false
        rgn.basicDestroy(ev, false, null);
      } catch (EntryNotFoundException ignore) {
        this.appliedOperation = true;
      } catch (ConcurrentCacheModificationException e) {
        dispatchElidedEvent(rgn, ev);
        this.appliedOperation = false;
      } finally {
        if (ev.hasValidVersionTag() && !ev.getVersionTag().isRecorded()) {
          if (rgn.getVersionVector() != null) {
            rgn.getVersionVector().recordVersion(getSender(), ev.getVersionTag());
          }
        }
        ev.release();
      }
    }

    /**
     * create an event for a RemoveAllEntryData element
     *
     * @return the event to be used in applying the element
     */
    @Retained
    public static EntryEventImpl createEntryEvent(RemoveAllEntryData entry,
        InternalDistributedMember sender, ClientProxyMembershipID context, DistributedRegion rgn,
        boolean possibleDuplicate, boolean needsRouting, Object callbackArg, boolean originRemote,
        boolean skipCallbacks) {
      final Object key = entry.getKey();
      EventID evId = entry.getEventID();
      @Retained
      EntryEventImpl ev = EntryEventImpl.create(rgn, entry.getOp(), key, null/* value */,
          callbackArg, originRemote, sender, !skipCallbacks, evId);
      boolean returnedEv = false;
      try {
        if (context != null) {
          ev.context = context;
        }
        ev.setPossibleDuplicate(possibleDuplicate);
        ev.setVersionTag(entry.versionTag);
        // if (needsRouting) {
        // FilterProfile fp = rgn.getFilterProfile();
        // if (fp != null) {
        // FilterInfo fi = fp.getLocalFilterRouting(ev);
        // ev.setLocalFilterInfo(fi);
        // }
        // }
        if (entry.filterRouting != null) {
          InternalDistributedMember id = rgn.getMyId();
          ev.setLocalFilterInfo(entry.filterRouting.getFilterInfo(id));
        }
        /*
         * Setting tailKey for the secondary bucket here. Tail key was update by the primary.
         */
        ev.setTailKey(entry.getTailKey());
        returnedEv = true;
        return ev;
      } finally {
        if (!returnedEv) {
          ev.release();
        }
      }
    }

    @Override
    protected void basicOperateOnRegion(EntryEventImpl ev, final DistributedRegion rgn) {
      for (int i = 0; i < removeAllDataSize; ++i) {
        if (removeAllData[i].versionTag != null) {
          checkVersionTag(rgn, removeAllData[i].versionTag);
        }
      }

      rgn.syncBulkOp(new Runnable() {
        @Override
        public void run() {
          for (int i = 0; i < removeAllDataSize; ++i) {
            if (logger.isTraceEnabled()) {
              logger.trace("removeAll processing {} with {}", removeAllData[i],
                  removeAllData[i].versionTag);
            }
            removeAllData[i].setSender(sender);
            doEntryRemove(removeAllData[i], rgn);
          }
        }
      }, ev.getEventId());
    }

    @Override
    public int getDSFID() {
      return REMOVE_ALL_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {

      super.fromData(in, context);
      this.eventId = (EventID) context.getDeserializer().readObject(in);
      this.removeAllDataSize = (int) InternalDataSerializer.readUnsignedVL(in);
      this.removeAllData = new RemoveAllEntryData[this.removeAllDataSize];
      if (this.removeAllDataSize > 0) {
        final Version version = StaticSerialization.getVersionForDataStreamOrNull(in);
        final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
        for (int i = 0; i < this.removeAllDataSize; i++) {
          this.removeAllData[i] = new RemoveAllEntryData(in, eventId, i, version, bytesIn, context);
        }

        boolean hasTags = in.readBoolean();
        if (hasTags) {
          EntryVersionsList versionTags = EntryVersionsList.create(in);
          for (int i = 0; i < this.removeAllDataSize; i++) {
            this.removeAllData[i].versionTag = versionTags.get(i);
          }
        }
      }

      if ((flags & HAS_BRIDGE_CONTEXT) != 0) {
        this.context = context.getDeserializer().readObject(in);
      }
      this.skipCallbacks = (flags & SKIP_CALLBACKS) != 0;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {

      super.toData(out, context);
      context.getSerializer().writeObject(this.eventId, out);
      InternalDataSerializer.writeUnsignedVL(this.removeAllDataSize, out);
      if (this.removeAllDataSize > 0) {
        EntryVersionsList versionTags = new EntryVersionsList(removeAllDataSize);

        boolean hasTags = false;
        for (int i = 0; i < this.removeAllDataSize; i++) {
          if (!hasTags && removeAllData[i].versionTag != null) {
            hasTags = true;
          }
          VersionTag<?> tag = removeAllData[i].versionTag;
          versionTags.add(tag);
          removeAllData[i].versionTag = null;
          this.removeAllData[i].serializeTo(out, context);
          this.removeAllData[i].versionTag = tag;
        }

        out.writeBoolean(hasTags);
        if (hasTags) {
          InternalDataSerializer.invokeToData(versionTags, out);
        }
      }
      if (this.context != null) {
        context.getSerializer().writeObject(this.context, out);
      }
    }

    @Override
    protected short computeCompressedShort(short s) {
      s = super.computeCompressedShort(s);
      if (this.context != null)
        s |= HAS_BRIDGE_CONTEXT;
      if (this.skipCallbacks)
        s |= SKIP_CALLBACKS;
      return s;
    }

    public ClientProxyMembershipID getContext() {
      return this.context;
    }

    public RemoveAllEntryData[] getRemoveAllEntryData() {
      return this.removeAllData;
    }

  }
}
