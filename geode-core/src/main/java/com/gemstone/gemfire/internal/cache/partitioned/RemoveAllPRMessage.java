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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.DataLocationException;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import com.gemstone.gemfire.internal.cache.DistributedRemoveAllOperation;
import com.gemstone.gemfire.internal.cache.DistributedRemoveAllOperation.RemoveAllEntryData;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.PutAllPartialResultException;
import com.gemstone.gemfire.internal.cache.PutAllPartialResultException.PutAllPartialResult;
import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.versions.ConcurrentCacheModificationException;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * PR removeAll
 *
 * @since 8.1
 */
public final class RemoveAllPRMessage extends PartitionMessageWithDirectReply
{
  private static final Logger logger = LogService.getLogger();
  
  private RemoveAllEntryData[] removeAllPRData;

  private int removeAllPRDataSize = 0;

  private Integer bucketId;

  /** An additional object providing context for the operation, e.g., for BridgeServer notification */
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
   * state from operateOnRegion that must be preserved for transmission
   * from the waiting pool
   */
  transient boolean result = false;
  
  transient VersionedObjectList versions = null;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoveAllPRMessage() {
  }

  public RemoveAllPRMessage(int bucketId, int size, boolean notificationOnly,
      boolean posDup, boolean skipCallbacks, Object callbackArg) {
    this.bucketId = Integer.valueOf(bucketId);
    removeAllPRData = new RemoveAllEntryData[size];
    this.notificationOnly = notificationOnly;
    this.posDup = posDup;
    this.skipCallbacks = skipCallbacks;
    this.callbackArg = callbackArg;
    initTxMemberId();
  }

  public void addEntry(RemoveAllEntryData entry) {
    this.removeAllPRData[this.removeAllPRDataSize++] = entry;
  }

  public void initMessage(PartitionedRegion r, Set recipients, boolean notifyOnly, DirectReplyProcessor p) {
    setInternalDs(r.getSystem());
    setDirectAck(false);
    this.resetRecipients();
    if (recipients != null) {
      setRecipients(recipients);
    }
    this.regionId = r.getPRId();
    this.processor = p;
    this.processorId = p==null? 0 : p.getProcessorId();
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

  public int getSize() {
    return removeAllPRDataSize;
  }
  
  public Set getKeys() {
    Set keys = new HashSet(getSize());
    for (int i=0; i<removeAllPRData.length; i++) {
      if (removeAllPRData[i] != null) {
        keys.add(removeAllPRData[i].getKey());
      }
    }
    return keys;
  }

  /**
   * Sends a PartitionedRegion RemoveAllPRMessage to the recipient
   * @param recipient the member to which the message is sent
   * @param r  the PartitionedRegion for which the op was performed
   * @return the processor used to await acknowledgement that the op was
   *         sent, or null to indicate that no acknowledgement will be sent
   * @throws ForceReattemptException if the peer is no longer available
   */
  public PartitionResponse send(DistributedMember recipient, PartitionedRegion r)
      throws ForceReattemptException
  {
    //Assert.assertTrue(recipient != null, "RemoveAllPRMessage NULL recipient");  recipient can be null for event notifications
    Set recipients = Collections.singleton(recipient);
    RemoveAllResponse p = new RemoveAllResponse(r.getSystem(), recipients);
    initMessage(r, recipients, false, p);
    setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    if (logger.isDebugEnabled()) {
      logger.debug("RemoveAllPRMessage.send: recipient is {}, msg is {}", recipient, this);
    }

    Set failures =r.getDistributionManager().putOutgoing(this);
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
    return PR_REMOVE_ALL_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.bucketId = Integer.valueOf((int)InternalDataSerializer
        .readSignedVL(in));
    if ((flags & HAS_BRIDGE_CONTEXT) != 0) {
      this.bridgeContext = DataSerializer.readObject(in);
    }
    Version sourceVersion = InternalDataSerializer.getVersionForDataStream(in);
    this.callbackArg = DataSerializer.readObject(in);
    this.removeAllPRDataSize = (int)InternalDataSerializer.readUnsignedVL(in);
    this.removeAllPRData = new RemoveAllEntryData[removeAllPRDataSize];
    if (this.removeAllPRDataSize > 0) {
      final Version version = InternalDataSerializer
          .getVersionForDataStreamOrNull(in);
      final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
      for (int i = 0; i < this.removeAllPRDataSize; i++) {
        this.removeAllPRData[i] = new RemoveAllEntryData(in, null, i, version,
            bytesIn);
      }

      boolean hasTags = in.readBoolean();
      if (hasTags) {
        EntryVersionsList versionTags = EntryVersionsList.create(in);
        for (int i = 0; i < this.removeAllPRDataSize; i++) {
          this.removeAllPRData[i].versionTag = versionTags.get(i);
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
      InternalDataSerializer.writeSignedVL(bucketId.intValue(), out);
    }
    if (this.bridgeContext != null) {
      DataSerializer.writeObject(this.bridgeContext, out);
    }
    DataSerializer.writeObject(this.callbackArg, out);
    InternalDataSerializer.writeUnsignedVL(this.removeAllPRDataSize, out);
    if (this.removeAllPRDataSize > 0) {
      EntryVersionsList versionTags = new EntryVersionsList(removeAllPRDataSize);

      boolean hasTags = false;
      // get the "keyRequiresRegionContext" flag from first element assuming
      // all key objects to be uniform
      final boolean requiresRegionContext =
        (this.removeAllPRData[0].getKey() instanceof KeyWithRegionContext);
      for (int i = 0; i < this.removeAllPRDataSize; i++) {
        // If sender's version is >= 7.0.1 then we can send versions list.
        if (!hasTags && removeAllPRData[i].versionTag != null) {
          hasTags = true;
        }

        VersionTag<?> tag = removeAllPRData[i].versionTag;
        versionTags.add(tag);
        removeAllPRData[i].versionTag = null;
        removeAllPRData[i].toData(out, requiresRegionContext);
        removeAllPRData[i].versionTag = tag;
        // RemoveAllEntryData's toData did not serialize eventID to save
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
    if (this.bridgeContext != null) s |= HAS_BRIDGE_CONTEXT;
    if (this.skipCallbacks) s |= SKIP_CALLBACKS;
    return s;
  }

  @Override
  protected void setBooleans(short s, DataInput in) throws IOException,
      ClassNotFoundException {
    super.setBooleans(s, in);
    this.skipCallbacks = ((s & SKIP_CALLBACKS) != 0);
  }

  @Override
  public EventID getEventID() {
    if (this.removeAllPRData.length > 0) {
      return this.removeAllPRData[0].getEventID();
    }
    return null;
  }

  /**
   * This method is called upon receipt and make the desired changes to the
   * PartitionedRegion Note: It is very important that this message does NOT
   * cause any deadlocks as the sender will wait indefinitely for the
   * acknowledgement
   */
  @Override
  protected final boolean operateOnPartitionedRegion(DistributionManager dm,
      PartitionedRegion r, long startTime)  throws EntryExistsException,
      ForceReattemptException, DataLocationException
  {
    boolean sendReply = true;

    InternalDistributedMember eventSender = getSender();

    try {
      result = doLocalRemoveAll(r, eventSender, true);
    }
    catch (ForceReattemptException fre) {
      sendReply(getSender(), getProcessorId(), dm, 
          new ReplyException(fre), r, startTime);
      return false;
    }

    if (sendReply) {
      sendReply(getSender(), getProcessorId(), dm, null, r, startTime);
    }
    return false;
  }

  /* we need a event with content for waitForNodeOrCreateBucket() */
  public EntryEventImpl getFirstEvent(PartitionedRegion r) {
    if (removeAllPRDataSize == 0) {
      return null;
    }
    
    EntryEventImpl ev = EntryEventImpl.create(r, 
        removeAllPRData[0].getOp(),
        removeAllPRData[0].getKey(), 
        null /*value*/, 
        this.callbackArg,
        false /* originRemote */,
        getSender(),
        true/* generate Callbacks */,
        removeAllPRData[0].getEventID());
    return ev;
  }
  
  @Override
  protected Object clone() throws CloneNotSupportedException {
    // TODO Auto-generated method stub
    return super.clone();
  }

  /**
   * This method is called by both operateOnPartitionedRegion() when processing a remote msg
   * or by sendMsgByBucket() when processing a msg targeted to local Jvm. 
   * PartitionedRegion Note: It is very important that this message does NOT
   * cause any deadlocks as the sender will wait indefinitely for the
   * acknowledgment
   * @param r partitioned region
   * @param eventSender the endpoint server who received request from client
   * @param cacheWrite if true invoke cacheWriter before desrtoy
   * @return If succeeds, return true, otherwise, throw exception
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IMSE_DONT_CATCH_IMSE")
  public final boolean doLocalRemoveAll(PartitionedRegion r, InternalDistributedMember eventSender, boolean cacheWrite)
  throws EntryExistsException,
  ForceReattemptException,DataLocationException
  {
    boolean didRemove=false;
    long clientReadTimeOut = PoolFactory.DEFAULT_READ_TIMEOUT;
    if (r.hasServerProxy()) {
      clientReadTimeOut = r.getServerProxy().getPool().getReadTimeout();
      if (logger.isDebugEnabled()) {
        logger.debug("RemoveAllPRMessage: doLocalRemoveAll: clientReadTimeOut is {}", clientReadTimeOut);
      }
    }
    
    DistributedRemoveAllOperation op = null;
    EntryEventImpl baseEvent = null;
    BucketRegion bucketRegion = null;
    PartitionedRegionDataStore ds = r.getDataStore();
    InternalDistributedMember myId = r.getDistributionManager().getDistributionManagerId();
    try {
    
    if (!notificationOnly) {
      // bucketRegion is not null only when !notificationOnly
      bucketRegion = ds.getInitializedBucketForId(null, bucketId);
      
      this.versions = new VersionedObjectList(this.removeAllPRDataSize, true, bucketRegion.getAttributes().getConcurrencyChecksEnabled());

      // create a base event and a DPAO for RemoveAllMessage distributed btw redundant buckets
      baseEvent = EntryEventImpl.create(
          bucketRegion, Operation.REMOVEALL_DESTROY,
          null, null, this.callbackArg, true, eventSender, !skipCallbacks, true);
      // set baseEventId to the first entry's event id. We need the thread id for DACE
      baseEvent.setEventId(removeAllPRData[0].getEventID());
      if (this.bridgeContext != null) {
        baseEvent.setContext(this.bridgeContext);
      }
      baseEvent.setPossibleDuplicate(this.posDup);
      if (logger.isDebugEnabled()) {
        logger.debug("RemoveAllPRMessage.doLocalRemoveAll: eventSender is {}, baseEvent is {}, msg is {}",
            eventSender, baseEvent, this);
      }
      op = new DistributedRemoveAllOperation(baseEvent, removeAllPRDataSize, false);
    }

    // Fix the updateMsg misorder issue
    // Lock the keys when doing postRemoveAll
    Object keys[] = new Object[removeAllPRDataSize];
    final boolean keyRequiresRegionContext = r.keyRequiresRegionContext();
    for (int i = 0; i < removeAllPRDataSize; ++i) {
      keys[i] = removeAllPRData[i].getKey();
      if (keyRequiresRegionContext) {
        ((KeyWithRegionContext)keys[i]).setRegionContext(r);
      }
    }

    if (!notificationOnly) {
      try {
        if(removeAllPRData.length > 0) {
          if (this.posDup && bucketRegion.getConcurrencyChecksEnabled()) {
            if (logger.isDebugEnabled()) {
              logger.debug("attempting to locate version tags for retried event");
            }
            // bug #48205 - versions may have already been generated for a posdup event
            // so try to recover them before wiping out the eventTracker's record
            // of the previous attempt
            for (int i=0; i<removeAllPRDataSize; i++) {
              if (removeAllPRData[i].versionTag == null) {
                removeAllPRData[i].versionTag = bucketRegion.findVersionTagForClientBulkOp(removeAllPRData[i].getEventID());
                if (removeAllPRData[i].versionTag != null) {
                  removeAllPRData[i].versionTag.replaceNullIDs(bucketRegion.getVersionMember());
                }
              }
            }
          }
          EventID eventID = removeAllPRData[0].getEventID();
          ThreadIdentifier membershipID = new ThreadIdentifier(
              eventID.getMembershipID(), eventID.getThreadID());
          bucketRegion.recordBulkOpStart(membershipID);
        }
        bucketRegion.waitUntilLocked(keys);
        boolean lockedForPrimary = false;
        final ArrayList<Object> succeeded = new ArrayList<Object>();
        PutAllPartialResult partialKeys = new PutAllPartialResult(removeAllPRDataSize);
        Object key = keys[0];
        try {
          bucketRegion.doLockForPrimary(false);
          lockedForPrimary = true;
      
          /* The real work to be synchronized, it will take long time. We don't 
           * worry about another thread to send any msg which has the same key
           * in this request, because these request will be blocked by foundKey
           */
          for (int i=0; i<removeAllPRDataSize; i++) {
            EntryEventImpl ev = getEventFromEntry(r, myId, eventSender, i,removeAllPRData,notificationOnly,bridgeContext,posDup,skipCallbacks);
            try {
            key = ev.getKey();

            ev.setRemoveAllOperation(op);

            // ev will be added into the op in removeLocally()
            // real operation will be modified into ev in removeLocally()
            // then in basicPutPart3(), the ev is added into op
            try {
              r.getDataView().destroyOnRemote(ev, cacheWrite, null);
              didRemove = true;
              if (logger.isDebugEnabled()) {
                logger.debug("RemoveAllPRMessage.doLocalRemoveAll:removeLocally success for "+ev);
              }
            } catch (EntryNotFoundException ignore) {
              didRemove = true;
              if (ev.getVersionTag() == null) {
                if (logger.isDebugEnabled()) {
                  logger.debug("doLocalRemoveAll:RemoveAll encoutered EntryNotFoundException: event={}", ev);
                }
              }
            } catch (ConcurrentCacheModificationException e) {
              didRemove = true;
              if (logger.isDebugEnabled()) {
                logger.debug("RemoveAllPRMessage.doLocalRemoveAll:removeLocally encountered concurrent cache modification for "+ev);
              }
            }
            removeAllPRData[i].setTailKey(ev.getTailKey());
            if (!didRemove) { // make sure the region hasn't gone away
              r.checkReadiness();
              ForceReattemptException fre = new ForceReattemptException(
              "unable to perform remove in RemoveAllPR, but operation should not fail");
              fre.setHash(ev.getKey().hashCode());
              throw fre;
            } else {
              succeeded.add(removeAllPRData[i].getKey());
              this.versions.addKeyAndVersion(removeAllPRData[i].getKey(), ev.getVersionTag());
            }
            } finally {
              ev.release();
            }
          } // for

        } catch (IllegalMonitorStateException ex) {
          ForceReattemptException fre = new ForceReattemptException(
          "unable to get lock for primary, retrying... ");
          throw fre;
        } catch (CacheWriterException cwe) {
          // encounter cacheWriter exception
          partialKeys.saveFailedKey(key, cwe);
        } finally {
          try {
            // Only RemoveAllPRMessage knows if the thread id is fake. Event has no idea.
            // So we have to manually set useFakeEventId for this op
            op.setUseFakeEventId(true);
            r.checkReadiness();
            bucketRegion.getDataView().postRemoveAll(op, this.versions, bucketRegion);
          } finally {
            if (lockedForPrimary) {
              bucketRegion.doUnlockForPrimary();
            }
          }
        }
        if (partialKeys.hasFailure()) {
          partialKeys.addKeysAndVersions(this.versions);
          if (logger.isDebugEnabled()) {
            logger.debug("RemoveAllPRMessage: partial keys applied, map to bucket {}'s keys:{}. Applied {}",
                bucketId, Arrays.toString(keys), succeeded);
          }
          throw new PutAllPartialResultException(partialKeys);
        }
      } catch(RegionDestroyedException e) {
          ds.checkRegionDestroyedOnBucket(bucketRegion ,true, e);
      } finally {
        bucketRegion.removeAndNotifyKeys(keys);
      }
    } else {
      for (int i=0; i<removeAllPRDataSize; i++) {
        EntryEventImpl ev = getEventFromEntry(r, myId, eventSender, i,removeAllPRData,notificationOnly,bridgeContext,posDup,skipCallbacks);
        try {
        ev.setOriginRemote(true);
        if (this.callbackArg != null) {
          ev.setCallbackArgument(this.callbackArg);
        }
        r.invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY, ev, r.isInitialized(), true);
        } finally {
          ev.release();
        }
      }
    }
  } finally {
    if (baseEvent != null) baseEvent.release();
    if (op != null) op.freeOffHeapResources();
  }

    return true;
  }
  
  public VersionedObjectList getVersions() {
    return this.versions;
  }

  
  @Override
  public boolean canStartRemoteTransaction() {
        return true;
  }
  
  public static EntryEventImpl getEventFromEntry(LocalRegion r,
      InternalDistributedMember myId, InternalDistributedMember eventSender,
      int idx, DistributedRemoveAllOperation.RemoveAllEntryData[] data,
      boolean notificationOnly, ClientProxyMembershipID bridgeContext,
      boolean posDup, boolean skipCallbacks) {
    RemoveAllEntryData dataItem = data[idx];
    EntryEventImpl ev = EntryEventImpl.create(r, dataItem.getOp(), dataItem.getKey(), null, null, false, eventSender, !skipCallbacks, dataItem.getEventID());
    boolean evReturned = false;
    try {

    ev.setOldValue(dataItem.getOldValue());
    if (bridgeContext != null) {
      ev.setContext(bridgeContext);
    }
    ev.setInvokePRCallbacks(!notificationOnly);
    ev.setPossibleDuplicate(posDup);
    if (dataItem.filterRouting != null) {
      ev.setLocalFilterInfo(dataItem.filterRouting.getFilterInfo(myId));
    }
    if (dataItem.versionTag != null) {
      dataItem.versionTag.replaceNullIDs(eventSender);
      ev.setVersionTag(dataItem.versionTag);
    }
    if(notificationOnly){
      ev.setTailKey(-1L);
    } else {
      ev.setTailKey(dataItem.getTailKey());
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
    return new RemoveAllResponse(r.getSystem(), recipients);
  }

  // override reply message type from PartitionMessage
  @Override
  protected void sendReply(InternalDistributedMember member, int procId, DM dm, ReplyException ex, PartitionedRegion pr, long startTime) {
    if (pr != null) {
      if (startTime > 0) {
        pr.getPrStats().endPartitionMessagesProcessing(startTime);
      }
      if (!pr.getConcurrencyChecksEnabled() && this.versions != null) {
        this.versions.clear();
      }
    }
    RemoveAllReplyMessage.send(member, procId, getReplySender(dm), this.result, this.versions, ex);
  }


  @Override
  protected final void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; removeAllPRDataSize=").append(removeAllPRDataSize)
        .append("; bucketId=").append(bucketId);
    if (this.bridgeContext != null) {
      buff.append("; bridgeContext=").append(this.bridgeContext);
    }

    buff.append("; directAck=")
        .append(this.directAck);
    
    for (int i=0; i<removeAllPRDataSize; i++) {
      buff.append("; entry"+i+":").append(removeAllPRData[i].getKey())
        .append(",").append(removeAllPRData[i].versionTag);
    }
  }

  public final InternalDistributedSystem getInternalDs()
  {
    return internalDs;
  }

  public final void setInternalDs(InternalDistributedSystem internalDs)
  {
    this.internalDs = internalDs;
  }

  public final void setDirectAck(boolean directAck)
  {
    this.directAck = directAck;
  }

  @Override
  protected boolean mayAddToMultipleSerialGateways(DistributionManager dm) {
    return _mayAddToMultipleSerialGateways(dm);
  }

  public static final class RemoveAllReplyMessage extends ReplyMessage {
    /** Result of the RemoveAll operation */
    boolean result;
    VersionedObjectList versions;

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public RemoveAllReplyMessage() {
    }

    private RemoveAllReplyMessage(int processorId, boolean result, VersionedObjectList versions, ReplyException ex) {
      super();
      this.versions = versions;
      this.result = result;
      setProcessorId(processorId);
      setException(ex);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender dm, boolean result, VersionedObjectList versions, ReplyException ex) {
      Assert.assertTrue(recipient != null, "RemoveAllReplyMessage NULL reply message");
      RemoveAllReplyMessage m = new RemoveAllReplyMessage(processorId, result, versions, ex);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message.  This method is invoked by the receiver
     * of the message.
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 rp) {
      final long startTime = getTimestamp();

      if (rp == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "{}: processor not found", this);
        }
        return;
      }
      if (rp instanceof RemoveAllResponse) {
        RemoveAllResponse processor = (RemoveAllResponse)rp;
        processor.setResponse(this);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} Processed {}", rp, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime()-startTime);
    }

    @Override
    public int getDSFID() {
      return PR_REMOVE_ALL_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.result = in.readBoolean();
      this.versions = (VersionedObjectList)DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(this.result);
      DataSerializer.writeObject(this.versions, out);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("RemoveAllReplyMessage ")
      .append("processorid=").append(this.processorId)
      .append(" returning ").append(this.result)
      .append(" exception=").append(getException())
      .append(" versions= ").append(this.versions);
      return sb.toString();
    }

  }
  
  /**
   * A processor to capture the value returned by {@link RemoveAllPRMessage}
   * @since 8.1
   */
  public static class RemoveAllResponse extends PartitionResponse {
    private volatile boolean returnValue;
    private VersionedObjectList versions;

    public RemoveAllResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients, false);
    }


    public void setResponse(RemoveAllReplyMessage response) {
      this.returnValue = response.result;
      if (response.versions != null) {
        this.versions = response.versions;
        this.versions.replaceNullIDs(response.getSender());
      }
    }

    /**
     * @return the result of the remote removeAll operation
     * @throws ForceReattemptException if the peer is no longer available
     * @throws CacheException if the peer generates an error
     */
    public RemoveAllResult waitForResult() throws CacheException,
        ForceReattemptException {
      try {
        waitForCacheException();
      }
      catch (ForceReattemptException e) {
        throw e;
      }
      return new RemoveAllResult(this.returnValue, this.versions);
    }
  }

  public static class RemoveAllResult {
    /** the result of the operation */
    public boolean returnValue;
    /** version information for the changes made to the cache */
    public VersionedObjectList versions;
    
    public RemoveAllResult(boolean flag, VersionedObjectList versions) {
      this.returnValue = flag;
      this.versions = versions;
    }
    @Override
    public String toString() {
      return "RemoveAllResult("+this.returnValue+", "+this.versions+")";
  }
  }

}
