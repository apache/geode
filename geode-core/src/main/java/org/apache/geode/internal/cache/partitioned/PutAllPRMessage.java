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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.DataLocationException;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import org.apache.geode.internal.cache.DistributedPutAllOperation.PutAllEntryData;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PutAllPartialResultException;
import org.apache.geode.internal.cache.PutAllPartialResultException.PutAllPartialResult;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;

/**
 * A Partitioned Region update message. Meant to be sent only to a bucket's primary owner. In
 * addition to updating an entry it is also used to send Partitioned Region event information.
 *
 * @since GemFire 6.0
 */
public class PutAllPRMessage extends PartitionMessageWithDirectReply {
  private static final Logger logger = LogService.getLogger();

  private PutAllEntryData[] putAllPRData;

  private int putAllPRDataSize = 0;

  private Integer bucketId;

  /**
   * An additional object providing context for the operation, e.g., for BridgeServer notification
   */
  ClientProxyMembershipID bridgeContext;

  /** true if no callbacks should be invoked */
  private boolean skipCallbacks;
  private Object callbackArg;

  protected static final short HAS_BRIDGE_CONTEXT = UNRESERVED_FLAGS_START;
  protected static final short SKIP_CALLBACKS = (HAS_BRIDGE_CONTEXT << 1);

  private transient InternalDistributedSystem internalDs;

  /** whether direct-acknowledgement is desired */
  private transient boolean directAck = false;

  /**
   * state from operateOnRegion that must be preserved for transmission from the waiting pool
   */
  transient boolean result = false;

  transient VersionedObjectList versions = null;

  /**
   * Empty constructor to satisfy {@link DataSerializer}requirements
   */
  public PutAllPRMessage() {}

  public PutAllPRMessage(int bucketId, int size, boolean notificationOnly, boolean posDup,
      boolean skipCallbacks, Object callbackArg) {
    this.bucketId = bucketId;
    putAllPRData = new PutAllEntryData[size];
    this.notificationOnly = notificationOnly;
    this.posDup = posDup;
    this.skipCallbacks = skipCallbacks;
    this.callbackArg = callbackArg;
    initTxMemberId();
  }

  public void addEntry(PutAllEntryData entry) {
    this.putAllPRData[this.putAllPRDataSize++] = entry;
  }

  public void initMessage(PartitionedRegion r, Set recipients, boolean notifyOnly,
      DirectReplyProcessor p) {
    setInternalDs(r.getSystem());
    setDirectAck(false);
    this.resetRecipients();
    if (recipients != null) {
      setRecipients(recipients);
    }
    this.regionId = r.getPRId();
    this.processor = p;
    this.processorId = p == null ? 0 : p.getProcessorId();
    if (p != null && this.isSevereAlertCompatible()) {
      p.enableSevereAlertProcessing();
    }
    this.notificationOnly = notifyOnly;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  public void setPossibleDuplicate(boolean posDup) {
    this.posDup = posDup;
  }

  // this method made unnecessary by entry versioning in 7.0 but kept here for merging
  // public void saveKeySet(PutAllPartialResult partialKeys) {
  // partialKeys.addKeysAndVersions(this.versions);
  // }

  public int getSize() {
    return putAllPRDataSize;
  }

  public Set getKeys() {
    Set keys = new HashSet(getSize());
    for (int i = 0; i < putAllPRData.length; i++) {
      if (putAllPRData[i] != null) {
        keys.add(putAllPRData[i].getKey());
      }
    }
    return keys;
  }

  /**
   * Sends a PartitionedRegion PutAllPRMessage to the recipient
   *
   * @param recipient the member to which the put message is sent
   * @param r the PartitionedRegion for which the put was performed
   * @return the processor used to await acknowledgement that the update was sent, or null to
   *         indicate that no acknowledgement will be sent
   * @throws ForceReattemptException if the peer is no longer available
   */
  public PartitionResponse send(DistributedMember recipient, PartitionedRegion r)
      throws ForceReattemptException {
    // Assert.assertTrue(recipient != null, "PutAllPRMessage NULL recipient"); recipient can be null
    // for event notifications
    Set recipients = Collections.singleton(recipient);
    PutAllResponse p = new PutAllResponse(r.getSystem(), recipients);
    initMessage(r, recipients, false, p);
    setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    if (logger.isDebugEnabled()) {
      logger.debug("PutAllPRMessage.send: recipient is {}, msg is {}", recipient, this);
    }

    Set failures = r.getDistributionManager().putOutgoing(this);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException("Failed sending <" + this + ">");
    }
    return p;
  }

  public void setBridgeContext(ClientProxyMembershipID contx) {
    Assert.assertTrue(contx != null);
    this.bridgeContext = contx;
  }

  public int getDSFID() {
    return PR_PUTALL_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketId = (int) InternalDataSerializer.readSignedVL(in);
    if ((flags & HAS_BRIDGE_CONTEXT) != 0) {
      this.bridgeContext = DataSerializer.readObject(in);
    }
    this.callbackArg = DataSerializer.readObject(in);
    this.putAllPRDataSize = (int) InternalDataSerializer.readUnsignedVL(in);
    this.putAllPRData = new PutAllEntryData[putAllPRDataSize];
    if (this.putAllPRDataSize > 0) {
      final Version version = InternalDataSerializer.getVersionForDataStreamOrNull(in);
      final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
      for (int i = 0; i < this.putAllPRDataSize; i++) {
        this.putAllPRData[i] = new PutAllEntryData(in, null, i, version, bytesIn);
      }

      boolean hasTags = in.readBoolean();
      if (hasTags) {
        EntryVersionsList versionTags = EntryVersionsList.create(in);
        for (int i = 0; i < this.putAllPRDataSize; i++) {
          this.putAllPRData[i].versionTag = versionTags.get(i);
        }
      }
    }

  }

  @Override
  public void toData(DataOutput out) throws IOException {

    super.toData(out);
    if (bucketId == null) {
      InternalDataSerializer.writeSignedVL(-1, out);
    } else {
      InternalDataSerializer.writeSignedVL(bucketId, out);
    }
    if (this.bridgeContext != null) {
      DataSerializer.writeObject(this.bridgeContext, out);
    }
    DataSerializer.writeObject(this.callbackArg, out);
    InternalDataSerializer.writeUnsignedVL(this.putAllPRDataSize, out);
    if (this.putAllPRDataSize > 0) {
      EntryVersionsList versionTags = new EntryVersionsList(putAllPRDataSize);

      boolean hasTags = false;
      for (int i = 0; i < this.putAllPRDataSize; i++) {
        // If sender's version is >= 7.0.1 then we can send versions list.
        if (!hasTags && putAllPRData[i].versionTag != null) {
          hasTags = true;
        }

        VersionTag<?> tag = putAllPRData[i].versionTag;
        versionTags.add(tag);
        putAllPRData[i].versionTag = null;
        putAllPRData[i].toData(out);
        putAllPRData[i].versionTag = tag;
        // PutAllEntryData's toData did not serialize eventID to save
        // performance for DR, but in PR,
        // we pack it for each entry since we used fake eventID
      }

      out.writeBoolean(hasTags);
      if (hasTags) {
        InternalDataSerializer.invokeToData(versionTags, out);
      }
    }
  }

  @Override
  protected short computeCompressedShort(short s) {
    s = super.computeCompressedShort(s);
    if (this.bridgeContext != null)
      s |= HAS_BRIDGE_CONTEXT;
    if (this.skipCallbacks)
      s |= SKIP_CALLBACKS;
    return s;
  }

  @Override
  protected void setBooleans(short s, DataInput in) throws IOException, ClassNotFoundException {
    super.setBooleans(s, in);
    this.skipCallbacks = ((s & SKIP_CALLBACKS) != 0);
  }

  @Override
  public EventID getEventID() {
    if (this.putAllPRData.length > 0) {
      return this.putAllPRData[0].getEventID();
    }
    return null;
  }

  /**
   * This method is called upon receipt and make the desired changes to the PartitionedRegion Note:
   * It is very important that this message does NOT cause any deadlocks as the sender will wait
   * indefinitely for the acknowledgement
   */
  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion pr,
      long startTime) throws EntryExistsException, ForceReattemptException, DataLocationException {
    boolean sendReply = true;

    InternalDistributedMember eventSender = getSender();

    long lastModified = 0L;
    try {
      result = doLocalPutAll(pr, eventSender, lastModified);
    } catch (ForceReattemptException fre) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(fre), pr, startTime);
      return false;
    }

    if (sendReply) {
      sendReply(getSender(), getProcessorId(), dm, null, pr, startTime);
    }
    return false;
  }

  /* we need a event with content for waitForNodeOrCreateBucket() */
  @Retained
  public EntryEventImpl getFirstEvent(PartitionedRegion r) {
    if (putAllPRDataSize == 0) {
      return null;
    }

    @Retained
    EntryEventImpl ev = EntryEventImpl.create(r, putAllPRData[0].getOp(), putAllPRData[0].getKey(),
        putAllPRData[0].getValue(r.getCache()), this.callbackArg, false /* originRemote */,
        getSender(), true/* generate Callbacks */, putAllPRData[0].getEventID());
    return ev;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    // TODO Auto-generated method stub
    return super.clone();
  }

  /**
   * This method is called by both operateOnPartitionedRegion() when processing a remote msg or by
   * sendMsgByBucket() when processing a msg targeted to local Jvm. PartitionedRegion Note: It is
   * very important that this message does NOT cause any deadlocks as the sender will wait
   * indefinitely for the acknowledgment
   *
   * @param r partitioned region eventSender the endpoint server who received request from client
   *        lastModified timestamp for last modification
   * @return If succeeds, return true, otherwise, throw exception
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("IMSE_DONT_CATCH_IMSE")
  public boolean doLocalPutAll(PartitionedRegion r, InternalDistributedMember eventSender,
      long lastModified)
      throws EntryExistsException, ForceReattemptException, DataLocationException {
    boolean didPut = false;
    long clientReadTimeOut = PoolFactory.DEFAULT_READ_TIMEOUT;
    if (r.hasServerProxy()) {
      clientReadTimeOut = r.getServerProxy().getPool().getReadTimeout();
      if (logger.isDebugEnabled()) {
        logger.debug("PutAllPRMessage: doLocalPutAll: clientReadTimeOut is {}", clientReadTimeOut);
      }
    }

    DistributedPutAllOperation dpao = null;
    @Released
    EntryEventImpl baseEvent = null;
    BucketRegion bucketRegion = null;
    PartitionedRegionDataStore ds = r.getDataStore();
    InternalDistributedMember myId = r.getDistributionManager().getDistributionManagerId();
    try {

      if (!notificationOnly) {
        // bucketRegion is not null only when !notificationOnly
        bucketRegion = ds.getInitializedBucketForId(null, bucketId);

        this.versions = new VersionedObjectList(this.putAllPRDataSize, true,
            bucketRegion.getAttributes().getConcurrencyChecksEnabled());

        // create a base event and a DPAO for PutAllMessage distributed btw redundant buckets
        baseEvent = EntryEventImpl.create(bucketRegion, Operation.PUTALL_CREATE, null, null,
            this.callbackArg, true, eventSender, !skipCallbacks, true);
        // set baseEventId to the first entry's event id. We need the thread id for DACE
        baseEvent.setEventId(putAllPRData[0].getEventID());
        if (this.bridgeContext != null) {
          baseEvent.setContext(this.bridgeContext);
        }
        baseEvent.setPossibleDuplicate(this.posDup);
        if (logger.isDebugEnabled()) {
          logger.debug(
              "PutAllPRMessage.doLocalPutAll: eventSender is {}, baseEvent is {}, msg is {}",
              eventSender, baseEvent, this);
        }
        dpao = new DistributedPutAllOperation(baseEvent, putAllPRDataSize, false);
      }

      Object[] keys = getKeysToBeLocked();
      if (!notificationOnly) {
        boolean locked = false;
        try {
          if (putAllPRData.length > 0) {
            if (this.posDup && bucketRegion.getConcurrencyChecksEnabled()) {
              if (logger.isDebugEnabled()) {
                logger.debug("attempting to locate version tags for retried event");
              }
              // bug #48205 - versions may have already been generated for a posdup event
              // so try to recover them before wiping out the eventTracker's record
              // of the previous attempt
              for (int i = 0; i < putAllPRDataSize; i++) {
                if (putAllPRData[i].versionTag == null) {
                  putAllPRData[i].versionTag =
                      bucketRegion.findVersionTagForClientBulkOp(putAllPRData[i].getEventID());
                  if (putAllPRData[i].versionTag != null) {
                    putAllPRData[i].versionTag.replaceNullIDs(bucketRegion.getVersionMember());
                  }
                }
              }
            }
            EventID eventID = putAllPRData[0].getEventID();
            ThreadIdentifier membershipID =
                new ThreadIdentifier(eventID.getMembershipID(), eventID.getThreadID());
            bucketRegion.recordBulkOpStart(membershipID, eventID);
          }
          locked = bucketRegion.waitUntilLocked(keys);
          boolean lockedForPrimary = false;
          final HashMap succeeded = new HashMap();
          PutAllPartialResult partialKeys = new PutAllPartialResult(putAllPRDataSize);
          Object key = keys[0];
          try {
            bucketRegion.doLockForPrimary(false);
            lockedForPrimary = true;

            /*
             * The real work to be synchronized, it will take long time. We don't worry about
             * another thread to send any msg which has the same key in this request, because these
             * request will be blocked by foundKey
             */
            for (int i = 0; i < putAllPRDataSize; i++) {
              @Released
              EntryEventImpl ev = getEventFromEntry(r, myId, eventSender, i, putAllPRData,
                  notificationOnly, bridgeContext, posDup, skipCallbacks);
              try {
                key = ev.getKey();

                ev.setPutAllOperation(dpao);

                // make sure a local update inserts a cache de-serializable
                ev.makeSerializedNewValue();

                // ev.setLocalFilterInfo(r.getFilterProfile().getLocalFilterRouting(ev));

                // ev will be added into dpao in putLocally()
                // oldValue and real operation will be modified into ev in putLocally()
                // then in basicPutPart3(), the ev is added into dpao
                try {
                  didPut = r.getDataView().putEntryOnRemote(ev, false, false, null, false,
                      lastModified, true);
                  if (didPut && logger.isDebugEnabled()) {
                    logger.debug("PutAllPRMessage.doLocalPutAll:putLocally success for {}", ev);
                  }
                } catch (ConcurrentCacheModificationException e) {
                  didPut = true;
                  if (logger.isDebugEnabled()) {
                    logger.debug(
                        "PutAllPRMessage.doLocalPutAll:putLocally encountered concurrent cache modification for {}",
                        ev, e);
                  }
                }
                putAllPRData[i].setTailKey(ev.getTailKey());
                if (!didPut) { // make sure the region hasn't gone away
                  r.checkReadiness();
                  ForceReattemptException fre = new ForceReattemptException(
                      "unable to perform put in PutAllPR, but operation should not fail");
                  fre.setHash(ev.getKey().hashCode());
                  throw fre;
                } else {
                  succeeded.put(putAllPRData[i].getKey(), putAllPRData[i].getValue(r.getCache()));
                  this.versions.addKeyAndVersion(putAllPRData[i].getKey(), ev.getVersionTag());
                }
              } finally {
                ev.release();
              }
            } // for

          } catch (IllegalMonitorStateException ignore) {
            throw new ForceReattemptException("unable to get lock for primary, retrying... ");
          } catch (CacheWriterException cwe) {
            // encounter cacheWriter exception
            partialKeys.saveFailedKey(key, cwe);
          } finally {
            doPostPutAll(r, dpao, bucketRegion, lockedForPrimary);
          }
          if (partialKeys.hasFailure()) {
            partialKeys.addKeysAndVersions(this.versions);
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "PutAllPRMessage: partial keys applied, map to bucket {}'s keys: {}. Applied {}",
                  bucketId, Arrays.toString(keys), succeeded);
            }
            throw new PutAllPartialResultException(partialKeys);
          }
        } catch (RegionDestroyedException e) {
          ds.checkRegionDestroyedOnBucket(bucketRegion, true, e);
        } finally {
          if (locked) {
            bucketRegion.removeAndNotifyKeys(keys);
          }
        }
      } else {
        for (int i = 0; i < putAllPRDataSize; i++) {
          EntryEventImpl ev = getEventFromEntry(r, myId, eventSender, i, putAllPRData,
              notificationOnly, bridgeContext, posDup, skipCallbacks);
          try {
            ev.setOriginRemote(true);
            if (this.callbackArg != null) {
              ev.setCallbackArgument(this.callbackArg);
            }
            r.invokePutCallbacks(ev.getOperation().isCreate() ? EnumListenerEvent.AFTER_CREATE
                : EnumListenerEvent.AFTER_UPDATE, ev, r.isInitialized(), true);
          } finally {
            ev.release();
          }
        }
      }
    } finally {
      if (baseEvent != null)
        baseEvent.release();
      if (dpao != null)
        dpao.freeOffHeapResources();
    }

    return true;
  }

  Object[] getKeysToBeLocked() {
    // Fix the updateMsg misorder issue
    // Lock the keys when doing postPutAll
    Object keys[] = new Object[putAllPRDataSize];
    for (int i = 0; i < putAllPRDataSize; ++i) {
      keys[i] = putAllPRData[i].getKey();
    }
    return keys;
  }

  void doPostPutAll(PartitionedRegion r, DistributedPutAllOperation dpao,
      BucketRegion bucketRegion, boolean lockedForPrimary) {
    try {
      // Only PutAllPRMessage knows if the thread id is fake. Event has no idea.
      // So we have to manually set useFakeEventId for this DPAO
      dpao.setUseFakeEventId(true);
      r.checkReadiness();
      bucketRegion.getDataView().postPutAll(dpao, this.versions, bucketRegion);
      r.checkReadiness();
    } finally {
      if (lockedForPrimary) {
        bucketRegion.doUnlockForPrimary();
      }
    }
  }

  public VersionedObjectList getVersions() {
    return this.versions;
  }


  @Override
  public boolean canStartRemoteTransaction() {
    return true;
  }

  @Retained
  public static EntryEventImpl getEventFromEntry(InternalRegion r, InternalDistributedMember myId,
      InternalDistributedMember eventSender, int idx, PutAllEntryData[] data,
      boolean notificationOnly, ClientProxyMembershipID bridgeContext, boolean posDup,
      boolean skipCallbacks) {
    PutAllEntryData prd = data[idx];
    // EntryEventImpl ev = EntryEventImpl.create(r,
    // prd.getOp(),
    // prd.getKey(), null/* value */, null /* callbackArg */,
    // false /* originRemote */,
    // eventSender,
    // true/* generate Callbacks */,
    // prd.getEventID());
    Object prdValue = prd.getValue(r.getCache());

    @Retained
    EntryEventImpl ev = EntryEventImpl.create(r, prd.getOp(), prd.getKey(), prdValue, null, false,
        eventSender, !skipCallbacks, prd.getEventID());
    boolean evReturned = false;
    try {

      if (prdValue == null && ev.getRegion().getAttributes().getDataPolicy() == DataPolicy.NORMAL) {
        ev.setLocalInvalid(true);
      }
      ev.setNewValue(prdValue);
      ev.setOldValue(prd.getOldValue());
      if (bridgeContext != null) {
        ev.setContext(bridgeContext);
      }
      ev.setInvokePRCallbacks(!notificationOnly);
      ev.setPossibleDuplicate(posDup);
      if (prd.filterRouting != null) {
        ev.setLocalFilterInfo(prd.filterRouting.getFilterInfo(myId));
      }
      if (prd.versionTag != null) {
        prd.versionTag.replaceNullIDs(eventSender);
        ev.setVersionTag(prd.versionTag);
      }
      // ev.setLocalFilterInfo(r.getFilterProfile().getLocalFilterRouting(ev));
      if (notificationOnly) {
        ev.setTailKey(-1L);
      } else {
        ev.setTailKey(prd.getTailKey());
      }
      evReturned = true;
      return ev;
    } finally {
      if (!evReturned) {
        ev.release();
      }
    }
  }

  // override reply processor type from PartitionMessage
  PartitionResponse createReplyProcessor(PartitionedRegion r, Set recipients, Object key) {
    return new PutAllResponse(r.getSystem(), recipients);
  }

  // override reply message type from PartitionMessage
  @Override
  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, PartitionedRegion pr, long startTime) {
    // if (!result && getOperation().isCreate()) {
    // System.err.println("DEBUG: put returning false. ifNew=" + ifNew
    // +" ifOld="+ifOld + " message=" + this);
    // }
    if (pr != null) {
      if (startTime > 0) {
        pr.getPrStats().endPartitionMessagesProcessing(startTime);
      }
      if (!pr.getConcurrencyChecksEnabled() && this.versions != null) {
        this.versions.clear();
      }
    }
    PutAllReplyMessage.send(member, procId, getReplySender(dm), this.result, this.versions, ex);
  }


  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; putAllPRDataSize=").append(putAllPRDataSize).append("; bucketId=")
        .append(bucketId);
    if (this.bridgeContext != null) {
      buff.append("; bridgeContext=").append(this.bridgeContext);
    }

    buff.append("; directAck=").append(this.directAck);

    for (int i = 0; i < putAllPRDataSize; i++) {
      buff.append("; entry").append(i).append(":").append(putAllPRData[i].getKey()).append(",")
          .append(putAllPRData[i].versionTag);
    }
  }

  public void setInternalDs(InternalDistributedSystem internalDs) {
    this.internalDs = internalDs;
  }

  public void setDirectAck(boolean directAck) {
    this.directAck = directAck;
  }

  @Override
  protected boolean mayAddToMultipleSerialGateways(ClusterDistributionManager dm) {
    return _mayAddToMultipleSerialGateways(dm);
  }

  @Override
  public String toString() {
    StringBuilder buff = new StringBuilder();
    String className = getClass().getName();
    buff.append(className.substring(className.indexOf(PN_TOKEN) + PN_TOKEN.length())); // partition.<foo>
    buff.append("(prid="); // make sure this is the first one
    buff.append(this.regionId);

    // Append name, if we have it
    String name = null;
    try {
      PartitionedRegion pr = PartitionedRegion.getPRFromId(this.regionId);
      if (pr != null) {
        name = pr.getFullPath();
      }
    } catch (Exception ignore) {
      /* ignored */
      name = null;
    }
    if (name != null) {
      buff.append(" (name = \"").append(name).append("\")");
    }

    appendFields(buff);
    buff.append(" ,distTx=");
    buff.append(this.isTransactionDistributed);
    buff.append(" ,putAlldatasize=");
    buff.append(this.putAllPRDataSize);
    // [DISTTX] TODO Disable this
    buff.append(" ,putAlldata=");
    buff.append(Arrays.toString(this.putAllPRData));
    buff.append(")");
    return buff.toString();
  }

  public static class PutAllReplyMessage extends ReplyMessage {
    /** Result of the PutAll operation */
    boolean result;
    VersionedObjectList versions;

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public PutAllReplyMessage() {}

    private PutAllReplyMessage(int processorId, boolean result, VersionedObjectList versions,
        ReplyException ex) {
      super();
      this.versions = versions;
      this.result = result;
      setProcessorId(processorId);
      setException(ex);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId, ReplySender dm,
        boolean result, VersionedObjectList versions, ReplyException ex) {
      Assert.assertTrue(recipient != null, "PutAllReplyMessage NULL reply message");
      PutAllReplyMessage m = new PutAllReplyMessage(processorId, result, versions, ex);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 rp) {
      final long startTime = getTimestamp();
      if (rp == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "{}: processor not found", this);
        }
        return;
      }
      if (rp instanceof PutAllResponse) {
        PutAllResponse processor = (PutAllResponse) rp;
        processor.setResponse(this);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", rp, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return PR_PUTALL_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.result = in.readBoolean();
      this.versions = (VersionedObjectList) DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(this.result);
      DataSerializer.writeObject(this.versions, out);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("PutAllReplyMessage ").append("processorid=").append(this.processorId)
          .append(" returning ").append(this.result).append(" exception=").append(getException())
          .append(" versions= ").append(this.versions);
      return sb.toString();
    }

  }

  /**
   * A processor to capture the value returned by {@link PutAllPRMessage}
   *
   * @since GemFire 5.8
   */
  public static class PutAllResponse extends PartitionResponse {
    private volatile boolean returnValue;
    private VersionedObjectList versions;

    public PutAllResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients, false);
    }


    public void setResponse(PutAllReplyMessage response) {
      this.returnValue = response.result;
      if (response.versions != null) {
        this.versions = response.versions;
        this.versions.replaceNullIDs(response.getSender());
      }
    }

    /**
     * @return the result of the remote put operation
     * @throws ForceReattemptException if the peer is no longer available
     * @throws CacheException if the peer generates an error
     */
    public PutAllResult waitForResult() throws CacheException, ForceReattemptException {
      waitForCacheException();
      return new PutAllResult(this.returnValue, this.versions);
    }
  }

  public static class PutAllResult {
    /** the result of the put operation */
    public boolean returnValue;
    /** version information for the changes made to the cache */
    public VersionedObjectList versions;

    public PutAllResult(boolean flag, VersionedObjectList versions) {
      this.returnValue = flag;
      this.versions = versions;
    }

    @Override
    public String toString() {
      return "PutAllResult(" + this.returnValue + ", " + this.versions + ")";
    }
  }

}
