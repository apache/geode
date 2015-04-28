/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CopyHelper;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.DeltaSerializationException;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.SerializedCacheValue;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.query.IndexMaintenanceException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.internal.IndexUpdater;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.cache.query.internal.index.IndexUtils;
import com.gemstone.gemfire.cache.util.TimestampedEntryEvent;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalDataSerializer.Sendable;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo.FilterInfo;
import com.gemstone.gemfire.internal.cache.delta.Delta;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage;
import com.gemstone.gemfire.internal.cache.partitioned.PutMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.pdx.internal.PeerTypeRegistration;

/**
 * Implementation of an entry event
 */
// must be public for DataSerializableFixedID
public class EntryEventImpl
  implements EntryEvent, InternalCacheEvent, DataSerializableFixedID, EntryOperation
{
  private static final Logger logger = LogService.getLogger();
  
  // PACKAGE FIELDS //
  public transient LocalRegion region;
  private transient RegionEntry re;

  protected KeyInfo keyInfo;

  //private long eventId;
  /** the event's id. Scoped by distributedMember. */
  protected EventID eventID;

  private Object newValue = null;
  /**
   * If we ever serialize the new value then it should be
   * stored in this field in case we need the serialized form
   * again later. This was added to fix bug 43781.
   * Note that we also have the "newValueBytes" field.
   * But it is only non-null if setSerializedNewValue was called.
   */
  private byte[] cachedSerializedNewValue = null;
  protected Object oldValue = null;
  protected Delta delta = null;
 
  protected short eventFlags = 0x0000;

  protected TXId txId = null;

  protected Operation op;

  /* To store the operation/modification type */
  private transient EnumListenerEvent eventType;

  /**
   * This field will be null unless this event is used for a putAll operation.
   *
   * @since 5.0
   */
  private transient DistributedPutAllOperation putAllOp;

  /**
   * This field will be null unless this event is used for a removeAll operation.
   *
   * @since 8.1
   */
  private transient DistributedRemoveAllOperation removeAllOp;

  /**
   * The member that originated this event
   *
   * @since 5.0
   */
  protected DistributedMember distributedMember;

  
  /**
   * transient storage for the message that caused the event
   */
  transient DistributionMessage causedByMessage;
  
  
  //private static long eventID = 0;

  /**
   * The originating membershipId of this event.
   *
   * @since 5.1
   */
  protected ClientProxyMembershipID context = null;
  
  /**
   * A custom context object that can be used for any other contextual
   * information. Currently used by SQL Fabric to pass around evaluated rows
   * from raw byte arrays and routing object.
   */
  private transient Object contextObj = null;

  /**
   * this holds the bytes representing the change in value effected by this
   * event.  It is used when the value implements the Delta interface.
   */
  private byte[] deltaBytes = null;

  
  /** routing information for cache clients for this event */
  private FilterInfo filterInfo;
  
  /**new value stored in serialized form*/
  protected byte[] newValueBytes;
  /**old value stored in serialized form*/
  private byte[] oldValueBytes;
  
  /** version tag for concurrency checks */
  protected VersionTag versionTag;

  
  private transient boolean isPendingSecondaryExpireDestroy = false;
  
  public final static Object SUSPECT_TOKEN = new Object();
  
  public EntryEventImpl() {
  }
  
  /**
   * create a new entry event that will be used for conveying version information
   * and anything else of use while processing another event
   * @return the empty event object
   */
  public static EntryEventImpl createVersionTagHolder() {
    return new EntryEventImpl();
  }
  
  /**
   * create a new entry event that will be used for conveying version information
   * and anything else of use while processing another event
   * @return the empty event object
   */
  public static EntryEventImpl createVersionTagHolder(VersionTag tag) {
    EntryEventImpl result = new EntryEventImpl();
    result.setVersionTag(tag);
    return result;
  }

  /**
   * Reads the contents of this message from the given input.
   */
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.eventID = (EventID)DataSerializer.readObject(in);
    Object key = DataSerializer.readObject(in);
    Object value = DataSerializer.readObject(in);
    this.keyInfo = new KeyInfo(key, value, null);
    this.op = Operation.fromOrdinal(in.readByte());
    this.eventFlags = in.readShort();
    this.keyInfo.setCallbackArg(DataSerializer.readObject(in));
    this.txId = (TXId)DataSerializer.readObject(in);

    if (in.readBoolean()) {     // isDelta
      this.delta = (Delta)DataSerializer.readObject(in);
    }
    else {
      // OFFHEAP Currently values are never deserialized to off heap memory. If that changes then this code needs to change.
      if (in.readBoolean()) {     // newValueSerialized
        this.newValueBytes = DataSerializer.readByteArray(in);
        this.cachedSerializedNewValue = this.newValueBytes;
        this.newValue = CachedDeserializableFactory.create(this.newValueBytes);
      }
      else {
        this.newValue = DataSerializer.readObject(in);
      }
    }

    // OFFHEAP Currently values are never deserialized to off heap memory. If that changes then this code needs to change.
    if (in.readBoolean()) {     // oldValueSerialized
      this.oldValueBytes = DataSerializer.readByteArray(in);
      this.oldValue = CachedDeserializableFactory.create(this.oldValueBytes);
    }
    else {
      this.oldValue = DataSerializer.readObject(in);
    }
    this.distributedMember = DSFIDFactory.readInternalDistributedMember(in);
    this.context = ClientProxyMembershipID.readCanonicalized(in);
    this.tailKey = DataSerializer.readLong(in);
  }

  public EntryEventImpl(LocalRegion region,
      Operation op,
      Object key, Object newValue, Object callbackArgument,
      boolean originRemote, DistributedMember distributedMember) {
    // Added initializeId=true as the default case. This doesn't
    // mean that the id will be initialized for sure. It only
    // means that it will not be set after the entry is instantiated.
    // UpdateOperation and DestroyOperation both set the id after the
    // entry is instantiated. Other callers do not.
    this(region, op, key, newValue, callbackArgument,
        originRemote, distributedMember, true/* generateCallbacks */,
        true /*initializeId*/);
  }

  public EntryEventImpl(LocalRegion region,
      Operation op,
      Object key,
      Object newValue,
      Object callbackArgument,
      boolean originRemote,
      DistributedMember distributedMember,
      boolean generateCallbacks) {
    // Added initializeId=true as the default case. This doesn't
    // mean that the id will be initialized for sure. It only
    // means that it will not be set after the entry is instantiated.
    // UpdateOperation and DestroyOperation both set the id after the
    // entry is instantiated. Other callers do not.
    this(region, op, key, newValue, callbackArgument, originRemote,
        distributedMember, generateCallbacks, true/*initializeId*/);
  }
  
  public EntryEventImpl(LocalRegion region, Operation op, Object key,
      boolean originRemote, DistributedMember distributedMember,
      boolean generateCallbacks, boolean fromRILocalDestroy) {
    this.region = region;
    this.op = op;
    this.keyInfo = this.region.getKeyInfo(key);
    setOriginRemote(originRemote);
    setGenerateCallbacks(generateCallbacks);
    this.distributedMember = distributedMember;
    setFromRILocalDestroy(fromRILocalDestroy);
  }

  /**
   * Doesn't specify oldValue as this will be filled in later as part of an
   * operation on the region, or lets it default to null.
   */
  public EntryEventImpl(
      final LocalRegion region,
      Operation op, Object key, Object newVal,
      Object callbackArgument,
      boolean originRemote, DistributedMember distributedMember,
      boolean generateCallbacks, boolean initializeId) {

    this.region = region;
    this.op = op;
    this.keyInfo = this.region.getKeyInfo(key, newVal, callbackArgument);

    if (newVal instanceof Delta) {
      this.delta = (Delta)newVal;
    }
    else if (!Token.isInvalid(newVal)) {
      basicSetNewValue(newVal);
    }

    this.txId = this.region.getTXId();
    /**
     * this might set txId for events done from a thread that has a tx even
     * though the op is non-tx. For example region ops.
     */
    if (newVal == Token.LOCAL_INVALID) {
      setLocalInvalid(true);
    }
    setOriginRemote(originRemote);
    setGenerateCallbacks(generateCallbacks);
    this.distributedMember = distributedMember;
  }

  /**
   * Called by BridgeEntryEventImpl to use existing EventID
   */
  public EntryEventImpl(LocalRegion region, Operation op, Object key,
      Object newValue, Object callbackArgument, boolean originRemote,
      DistributedMember distributedMember, boolean generateCallbacks,
      EventID eventID) {
    this(region, op, key, newValue,
        callbackArgument, originRemote, distributedMember, generateCallbacks,
        true /* initializeId */);
    Assert.assertTrue(eventID != null || !(region instanceof PartitionedRegion));
    this.setEventId(eventID);
  }

  /**
   * create an entry event from another entry event
   */
  public EntryEventImpl(EntryEventImpl other) {
    region = other.region;
    
    this.eventID = other.eventID;
    basicSetNewValue(other.basicGetNewValue());
    this.newValueBytes = other.newValueBytes;
    this.cachedSerializedNewValue = other.cachedSerializedNewValue;
    this.re = other.re;
    this.delta = other.delta;
    basicSetOldValue(other.basicGetOldValue(), true);
    this.oldValueBytes = other.oldValueBytes;
    eventFlags = other.eventFlags;
    setEventFlag(EventFlags.FLAG_CALLBACKS_INVOKED, false);
    txId = other.txId;
    op = other.op;
    distributedMember = other.distributedMember;
    this.filterInfo = other.filterInfo;
    if (other.getRawCallbackArgument() instanceof GatewaySenderEventCallbackArgument) {
      this.keyInfo = new KeyInfo(other.keyInfo);
      this.keyInfo.setCallbackArg((new GatewaySenderEventCallbackArgument(
          (GatewaySenderEventCallbackArgument)other.getRawCallbackArgument())));
    } else {
      this.keyInfo = new KeyInfo(other.keyInfo);
    }
    this.context = other.context;
    this.deltaBytes = other.deltaBytes;
    this.tailKey = other.tailKey;
    this.versionTag = other.versionTag;
    //set possible duplicate 
    this.setPossibleDuplicate(other.isPossibleDuplicate()); 
  }

  public EntryEventImpl(Object key2) {
    this.keyInfo = new KeyInfo(key2, null, null);
  }
  
  /**
   * This constructor is used to create a bridge event in server-side
   * command classes.  Events created with this are not intended to be
   * used in cache operations.
   * @param id the identity of the client's event
   */
  public EntryEventImpl(EventID id) {
    this.eventID = id;
  }

  /**
   * Creates a PutAllEvent given the distributed operation, the region, and the
   * entry data.
   *
   * @since 5.0
   */
  static EntryEventImpl createPutAllEvent(
      DistributedPutAllOperation putAllOp, LocalRegion region,
      Operation entryOp, Object entryKey, Object entryNewValue)
  {
    EntryEventImpl e;
    if (putAllOp != null) {
      EntryEventImpl event = putAllOp.getBaseEvent();
      if (event.isBridgeEvent()) {
        e = new EntryEventImpl(region, entryOp, entryKey, entryNewValue,
            event.getRawCallbackArgument(), false, event.distributedMember,
            event.isGenerateCallbacks());
        e.setContext(event.getContext());
      } else {
        e = new EntryEventImpl(region, entryOp, entryKey, entryNewValue, event.getCallbackArgument(),
            false, region.getMyId(), event.isGenerateCallbacks());
      }
      
    } else {
      e = new EntryEventImpl(region, entryOp, entryKey, entryNewValue, null,
          false, region.getMyId(), true);
    }
    
    e.putAllOp = putAllOp;
    return e;
  }
  
  static EntryEventImpl createRemoveAllEvent(
      DistributedRemoveAllOperation op, 
      LocalRegion region,
      Object entryKey) {
    EntryEventImpl e;
    final Operation entryOp = Operation.REMOVEALL_DESTROY;
    if (op != null) {
      EntryEventImpl event = op.getBaseEvent();
      if (event.isBridgeEvent()) {
        e = new EntryEventImpl(region, entryOp, entryKey, null,
            event.getRawCallbackArgument(), false, event.distributedMember,
            event.isGenerateCallbacks());
        e.setContext(event.getContext());
      } else {
        e = new EntryEventImpl(region, entryOp, entryKey, null, event.getCallbackArgument(),
            false, region.getMyId(), event.isGenerateCallbacks());
      }
      
    } else {
      e = new EntryEventImpl(region, entryOp, entryKey, null, null,
          false, region.getMyId(), true);
    }
    
    e.removeAllOp = op;
    return e;
  }
  public boolean isBulkOpInProgress() {
    return getPutAllOperation() != null || getRemoveAllOperation() != null;
  }
  
  /** return the putAll operation for this event, if any */
  public DistributedPutAllOperation getPutAllOperation() {
    return this.putAllOp;
  }
  public DistributedPutAllOperation setPutAllOperation(DistributedPutAllOperation nv) {
    DistributedPutAllOperation result = this.putAllOp;
    if (nv != null && nv.getBaseEvent() != null) {
      setCallbackArgument(nv.getBaseEvent().getCallbackArgument());
    }
    this.putAllOp = nv;
    return result;
  }
  public DistributedRemoveAllOperation getRemoveAllOperation() {
    return this.removeAllOp;
  }
  public DistributedRemoveAllOperation setRemoveAllOperation(DistributedRemoveAllOperation nv) {
    DistributedRemoveAllOperation result = this.removeAllOp;
    if (nv != null && nv.getBaseEvent() != null) {
      setCallbackArgument(nv.getBaseEvent().getCallbackArgument());
    }
    this.removeAllOp = nv;
    return result;
  }

  private final boolean testEventFlag(short mask)
  {
    return EventFlags.isSet(this.eventFlags, mask);
  }

  private final void setEventFlag(short mask, boolean on)
  {
    this.eventFlags = EventFlags.set(this.eventFlags, mask, on);
  }

  public DistributedMember getDistributedMember()
  {
    return this.distributedMember;
  }

  /////////////////////// INTERNAL BOOLEAN SETTERS
  public void setOriginRemote(boolean b)
  {
    setEventFlag(EventFlags.FLAG_ORIGIN_REMOTE, b);
  }

  public void setLocalInvalid(boolean b)
  {
    setEventFlag(EventFlags.FLAG_LOCAL_INVALID, b);
  }

  void setGenerateCallbacks(boolean b)
  {
    setEventFlag(EventFlags.FLAG_GENERATE_CALLBACKS, b);
  }

  /** set the the flag telling whether callbacks should be invoked for a partitioned region */
  public void setInvokePRCallbacks(boolean b) {
    setEventFlag(EventFlags.FLAG_INVOKE_PR_CALLBACKS, b);
  }

  /** get the flag telling whether callbacks should be invoked for a partitioned region */
  public boolean getInvokePRCallbacks() {
    return testEventFlag(EventFlags.FLAG_INVOKE_PR_CALLBACKS);
  }
  
  public boolean getInhibitDistribution() {
    return testEventFlag(EventFlags.FLAG_INHIBIT_DISTRIBUTION);
  }
  
  public void setInhibitDistribution(boolean b) {
    setEventFlag(EventFlags.FLAG_INHIBIT_DISTRIBUTION, b);
  }
  
  /** was the entry destroyed or missing and allowed to be destroyed again? */
  public boolean getIsRedestroyedEntry() {
    return testEventFlag(EventFlags.FLAG_REDESTROYED_TOMBSTONE);
  }
  
  public void setIsRedestroyedEntry(boolean b) {
    setEventFlag(EventFlags.FLAG_REDESTROYED_TOMBSTONE, b);
  }
  
  public void isConcurrencyConflict(boolean b) {
    setEventFlag(EventFlags.FLAG_CONCURRENCY_CONFLICT, b);
  }
  
  public boolean isConcurrencyConflict() {
    return testEventFlag(EventFlags.FLAG_CONCURRENCY_CONFLICT);
  }

  /** set the DistributionMessage that caused this event */
  public void setCausedByMessage(DistributionMessage msg) {
    this.causedByMessage = msg;
  }

  /**
   * get the PartitionMessage that caused this event, or null if
   * the event was not caused by a PartitionMessage
   */
  public PartitionMessage getPartitionMessage() {
    if (this.causedByMessage != null && this.causedByMessage instanceof PartitionMessage) {
      return (PartitionMessage)this.causedByMessage;
  }
    return null;
  }

  /**
   * get the RemoteOperationMessage that caused this event, or null if
   * the event was not caused by a RemoteOperationMessage
   */
  public RemoteOperationMessage getRemoteOperationMessage() {
    if (this.causedByMessage != null && this.causedByMessage instanceof RemoteOperationMessage) {
      return (RemoteOperationMessage)this.causedByMessage;
    }
    return null;
  }

  /////////////// BOOLEAN GETTERS
  public boolean isLocalLoad()
  {
    return this.op.isLocalLoad();
  }

  public boolean isNetSearch()
  {
    return this.op.isNetSearch();
  }

  public boolean isNetLoad()
  {
    return this.op.isNetLoad();
  }

  public boolean isDistributed()
  {
    return this.op.isDistributed();
  }

  public boolean isExpiration()
  {
    return this.op.isExpiration();
  }
  
  public boolean isEviction() {
    return this.op.isEviction();
  }

  public final boolean isPendingSecondaryExpireDestroy() {
    return this.isPendingSecondaryExpireDestroy;
  }
  
  public final void setPendingSecondaryExpireDestroy (boolean value) {
    this.isPendingSecondaryExpireDestroy = value;
  }
  // Note that isOriginRemote is sometimes set to false even though the event
  // was received from a peer.  This is done to force distribution of the
  // message to peers and to cause concurrency version stamping to be performed.
  // This is done by all one-hop operations, like RemoteInvalidateMessage.
  public boolean isOriginRemote()
  {
    return testEventFlag(EventFlags.FLAG_ORIGIN_REMOTE);
  }

  /* return whether this event originated from a WAN gateway and carries a WAN version tag */
  public boolean isFromWANAndVersioned() {
    return (this.versionTag != null && this.versionTag.isGatewayTag());
  }
  
  /* return whether this event originated in a client and carries a version tag */
  public boolean isFromBridgeAndVersioned() {
    return (this.context != null) && (this.versionTag != null);
  }

  public boolean isGenerateCallbacks()
  {
    return testEventFlag(EventFlags.FLAG_GENERATE_CALLBACKS);
  }

  public void setNewEventId(DistributedSystem sys) {
    Assert.assertTrue(this.eventID == null, "Double setting event id");
    EventID newID = new EventID(sys);
    if (this.eventID != null) {
      if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER)) {
        logger.trace(LogMarker.BRIDGE_SERVER, "Replacing event ID with {} in event {}", newID, this);
      }
    }
    this.eventID = newID;
  }
  
  public void reserveNewEventId(DistributedSystem sys, int count) {
    Assert.assertTrue(this.eventID == null, "Double setting event id");
    this.eventID = new EventID(sys);
    if (count > 1) {
      this.eventID.reserveSequenceId(count-1);
    }
  }

  public void setEventId(EventID id)
  {
    this.eventID = id;
  }

  /**
   * Return the event id, if any
   * @return null if no event id has been set
   */
  public final EventID getEventId() {
    return this.eventID;
  }

  public boolean isBridgeEvent() {
    return hasClientOrigin();
  }
  public boolean hasClientOrigin() {
    return getContext() != null;
  }

  /**
   * sets the ID of the client that initiated this event
   */
  public void setContext(ClientProxyMembershipID contx) {
    Assert.assertTrue(contx != null);
    this.context = contx;
  }

  /**
   * gets the ID of the client that initiated this event.  Null if a server-initiated event
   */
  public ClientProxyMembershipID getContext()
  {
    return this.context;
  }

  // INTERNAL
  boolean isLocalInvalid()
  {
    return testEventFlag(EventFlags.FLAG_LOCAL_INVALID);
  }

  /////////////////////////////////////////////////

  /**
   * Returns the key.
   *
   * @return the key.
   */
  public Object getKey()
  {
    return keyInfo.getKey();
  }

  /**
   * Returns the value in the cache prior to this event. When passed to an event
   * handler after an event occurs, this value reflects the value that was in
   * the cache in this VM, not necessarily the value that was in the cache VM
   * that initiated the operation.
   *
   * @return the value in the cache prior to this event.
   */
  public Object getOldValue()
  {
    try {
      if (isOriginRemote() && this.region.isProxy()) {
        return null;
      }
      Object ov = basicGetOldValue();
      if (ov == null) {
        return null;
      } else if (ov == Token.NOT_AVAILABLE) {
        return AbstractRegion.handleNotAvailable(ov);
      }
      boolean doCopyOnRead = getRegion().isCopyOnRead();
      if (ov != null) {
        if (ov instanceof CachedDeserializable) {
          CachedDeserializable cd = (CachedDeserializable)ov;
          if (doCopyOnRead) {
            return cd.getDeserializedWritableCopy(this.region, this.re);
          } else {
            return cd.getDeserializedValue(this.region, this.re);
          }
        }
        else {
          if (doCopyOnRead) {
            return CopyHelper.copy(ov);
          } else {
            return ov;
          }
        }
      }
      return null;
    } catch(IllegalArgumentException i) {
      IllegalArgumentException iae = new IllegalArgumentException(LocalizedStrings.DONT_RELEASE.toLocalizedString("Error while deserializing value for key="+getKey()));
      iae.initCause(i);
      throw iae;
    }
  }

  /**
   * Returns the new value as is; no deserialization or copying.
   * ALERT: If there is a Delta, returns that, not the (applied) new value.
   */
  public Object getRawNewValue() {
    if (this.delta != null) {
      return this.delta;
    }
    return basicGetNewValue();
  }
  
  private void basicSetNewValue(Object v) {
    if (v == this.newValue) return;
    this.newValue = v;
    this.cachedSerializedNewValue = null;
  }
  protected Object basicGetNewValue() {
    Object result = this.newValue;
    return result;
  }
  private void basicSetOldValue(Object v, boolean incRefCount) {
    if (v == this.oldValue) return;
    this.oldValue = v;
  }
  private Object basicGetOldValue() {
    Object result = this.oldValue;
    return result;
  }

  public Object getRawOldValue() {
    return basicGetOldValue();
  }

  /**
   * Get the new value preferring the real value over delta if newValue has
   * already been calculated. This is to correct size calculations when using
   * delta (used by SQLFabric). See bug #40866.
   */
  public Object getNewValueForSize() {
    applyDelta(false);
    return basicGetNewValue();
  }
  
  /**
   * Added this function to expose isCopyOnRead function to the
   * child classes of EntryEventImpl  
   * 
   */
  protected boolean isRegionCopyOnRead() {
    return getRegion().isCopyOnRead();
  }
 
  /**
   * Returns the value in the cache after this event.
   *
   * @return the value in the cache after this event.
   */
  public Object getNewValue() {
    boolean doCopyOnRead = getRegion().isCopyOnRead();
    try {
      if (applyDelta(true)) {
        Object applied = basicGetNewValue();
        if (applied == this.oldValue && doCopyOnRead) {
          applied = CopyHelper.copy(applied);
        }
        return applied;
      }
    } catch (EntryNotFoundException ex) {
      // only (broken) product code has the opportunity to call this before
      // this.oldValue is set. If oldValue is not set yet, then
      // we most likely haven't synchronized on the region entry yet.
      // (If we have, then make sure oldValue is set before
      // calling this method).
      throw new AssertionError("too early to call getNewValue");
    }
    Object nv = basicGetNewValue();
    if (nv != null) {
      if (nv == Token.NOT_AVAILABLE) {
        // I'm not sure this can even happen
        return AbstractRegion.handleNotAvailable(nv);
      }
      if (nv instanceof CachedDeserializable) {
        CachedDeserializable cd = (CachedDeserializable)nv;
        Object v = null;
        // TODO OFFHEAP currently we copy offheap new value to the heap here. Check callers of this method to see if they can be optimized to use offheap values.
        if (doCopyOnRead) {
          v = cd.getDeserializedWritableCopy(this.region, this.re);
        } else {
          v = cd.getDeserializedValue(this.region, this.re);
        }
        assert !(v instanceof CachedDeserializable) : "for key "+this.getKey()+" found nested CachedDeserializable";
        return v;
      }
      else {
        if (doCopyOnRead) {
          return CopyHelper.copy(nv);
        } else {
          return nv;
        }
      }
    }
    return null;
  }

  protected boolean applyDelta(boolean throwOnNullOldValue)
      throws EntryNotFoundException {
    if (this.newValue != null || this.delta == null) {
      return false;
    }
    if (this.oldValue == null) {
      if (throwOnNullOldValue) {
        // !!!:ezoerner:20080611 It would be nice if the client got this
        // exception
        throw new EntryNotFoundException(
            "Cannot apply a delta without an existing value");
      }
      return false;
    }
    // swizzle BucketRegion in event for Delta.
    // !!!:ezoerner:20090602 this is way ugly; this whole class severely
    // needs refactoring
    LocalRegion originalRegion = this.region;
    try {
      if (originalRegion instanceof BucketRegion) {
        this.region = ((BucketRegion)this.region).getPartitionedRegion();
      }
      basicSetNewValue(this.delta.apply(this));
    } finally {
      this.region = originalRegion;
    }
    return true;
  }

  /** Set a deserialized value */
  public void setNewValue(Object obj) {
    if (obj instanceof Delta) {
      this.delta = (Delta)obj;
      basicSetNewValue(null);
    }
    else {
      basicSetNewValue(obj);
    }
  }


  public TransactionId getTransactionId()
  {
    return this.txId;
  }

  public void setTransactionId(TransactionId txId)
  {
    this.txId = (TXId)txId;
  }

  /**
   * Answer true if this event resulted from a loader.
   *
   * @return true if isLocalLoad or isNetLoad
   */
  public boolean isLoad()
  {
    return this.op.isLoad();
  }

  public void setRegion(LocalRegion r)
  {
    this.region = r;
  }

  /**
   * @see com.gemstone.gemfire.cache.CacheEvent#getRegion()
   */
  public final LocalRegion getRegion() {
    return region;
  }

  public Operation getOperation()
  {
    return this.op;
  }

  public void setOperation(Operation op)
  {
    this.op = op;
    PartitionMessage prm = getPartitionMessage();
    if (prm != null) {
      prm.setOperation(this.op);
    }
  }

  /**
   * @see com.gemstone.gemfire.cache.CacheEvent#getCallbackArgument()
   */
  public Object getCallbackArgument()
  {
    Object result = this.keyInfo.getCallbackArg();
    while (result instanceof WrappedCallbackArgument) {
      WrappedCallbackArgument wca = (WrappedCallbackArgument)result;
      result = wca.getOriginalCallbackArg();
    }
    if (result == Token.NOT_AVAILABLE) {
      result = AbstractRegion.handleNotAvailable(result);
    }
    return result;
  }
  public boolean isCallbackArgumentAvailable() {
    return this.getRawCallbackArgument() != Token.NOT_AVAILABLE;
  }

  /**
   * Returns the value of the EntryEventImpl field.
   * This is for internal use only. Customers should always call
   * {@link #getCallbackArgument}
   * @since 5.5 
   */
  public Object getRawCallbackArgument() {
    return this.keyInfo.getCallbackArg();
  }
  
  /**
   * Sets the value of raw callback argument field.
   */
  public void setRawCallbackArgument(Object newCallbackArgument) {
    this.keyInfo.setCallbackArg(newCallbackArgument);
  }

  public void setCallbackArgument(Object newCallbackArgument) {
    if (this.keyInfo.getCallbackArg() instanceof WrappedCallbackArgument) {
      ((WrappedCallbackArgument)this.keyInfo.getCallbackArg())
          .setOriginalCallbackArgument(newCallbackArgument);
    }
    else {
      this.keyInfo.setCallbackArg(newCallbackArgument);
    }
  }

  /**
   * Return the new value iff it is already in serialized form,
   * otherwise return null
   */
  public SerializedCacheValue<?> getSerializedNewValue() {
    // In the case where there is a delta that has not been applied yet,
    // do not apply it here since it would not produce a serialized new
    // value (return null instead to indicate the new value is not
    // in serialized form).
    final Object tmp = basicGetNewValue();
    if (tmp instanceof CachedDeserializable) {
      byte[] bytes = this.newValueBytes;
      if (bytes == null) {
        bytes = this.cachedSerializedNewValue;
      }
      return new SerializedCacheValueImpl(getRegion(), this.re,
          (CachedDeserializable)tmp, bytes);
    } else {
      // Note we return null even if cachedSerializedNewValue is not null.
      // This is because some callers of this method use it to indicate
      // that a CacheDeserializable should be created during deserialization.
      return null;
    }
  }
  
  /**
   * Forces this entry's new value to be in serialized form.
   * @since 5.0.2
   */
  public void makeSerializedNewValue() {
    makeSerializedNewValue(false);
  }

  /**
   * @param isSynced true if RegionEntry currently under synchronization
   */
  private void makeSerializedNewValue(boolean isSynced) {
    Object obj = this.newValue;

    // ezoerner:20080611 In the case where there is an unapplied
    // delta, do not apply the delta or serialize yet unless entry is
    // under synchronization (isSynced is true) 
    if (isSynced) {
      this.setSerializationDeferred(false);
    }
    else if (obj == null && this.delta != null) {
      // defer serialization until setNewValueInRegion
      this.setSerializationDeferred(true);
      return;
    }
    basicSetNewValue(getCachedDeserializable(obj, this));
  }

  public static Object getCachedDeserializable(Object obj) {
    return getCachedDeserializable(obj, null);
  }

  public static Object getCachedDeserializable(Object obj, EntryEventImpl ev) {
    if (obj instanceof byte[]
                            || obj == null
                            || obj instanceof CachedDeserializable
                            || obj == Token.NOT_AVAILABLE
                            || Token.isInvalidOrRemoved(obj)
                            // don't serialize delta object already serialized
                            || obj instanceof com.gemstone.gemfire.Delta
                            || obj instanceof Delta) { // internal delta
      return obj;
    }
    final CachedDeserializable cd;
    // avoid unneeded serialization of byte[][] used by SQLFabric that
    // will end up being deserialized in any case (serialization is cheap
    //   for byte[][] anyways)
    if (obj instanceof byte[][]) {
      int objSize = Sizeable.PER_OBJECT_OVERHEAD + 4;
      for (byte[] bytes : (byte[][])obj) {
        if (bytes != null) {
          objSize += CachedDeserializableFactory.getByteSize(bytes);
        }
        else {
          objSize += Sizeable.PER_OBJECT_OVERHEAD;
        }
      }
      cd = CachedDeserializableFactory.create(obj, objSize);
    }
    else {
      final byte[] b = serialize(obj);
      cd = CachedDeserializableFactory.create(b);
      if (ev != null) {
        ev.newValueBytes = b;
        ev.cachedSerializedNewValue = b;
      }
    }
    return cd;
  }
  public void setCachedSerializedNewValue(byte[] v) {
    this.cachedSerializedNewValue = v;
  }
  public byte[] getCachedSerializedNewValue() {
    return this.cachedSerializedNewValue;
  }

  public void setSerializedNewValue(byte[] serializedValue) {
    this.newValueBytes = serializedValue;
    basicSetNewValue(CachedDeserializableFactory.create(serializedValue));
    this.cachedSerializedNewValue = serializedValue;
  }

  public void setSerializedOldValue(byte[] serializedOldValue){
    this.oldValueBytes = serializedOldValue;
    basicSetOldValue(CachedDeserializableFactory.create(serializedOldValue), true);
  }

  /**
   * If true (the default) then preserve old values in events.
   * If false then mark non-null values as being NOT_AVAILABLE.
   */
  private static final boolean EVENT_OLD_VALUE = !Boolean.getBoolean("gemfire.disable-event-old-value");

  
  void putExistingEntry(final LocalRegion owner, RegionEntry entry) throws RegionClearedException {
    putExistingEntry(owner, entry, false, null);
  }
  
  /**
   * Put a newValue into the given, write synced, existing, region entry.
   * Sets oldValue in event if hasn't been set yet.
   * @param oldValueForDelta Used by Delta Propagation feature
   * 
   * @throws RegionClearedException
   */
  void putExistingEntry(final LocalRegion owner, final RegionEntry reentry,
     boolean requireOldValue, Object oldValueForDelta) throws RegionClearedException {
    makeUpdate();
    // only set oldValue if it hasn't already been set to something
    if (this.oldValue == null) {
      if (!reentry.isInvalidOrRemoved()) {
        if (requireOldValue ||
            EVENT_OLD_VALUE
            || this.region instanceof HARegion // fix for bug 37909
            ) {
          Object ov = reentry._getValueUse(owner, true); // TODO:KIRK:OK 
          if (ov == null) ov = Token.NOT_AVAILABLE;
          basicSetOldValue(ov, false);
        } else {
          basicSetOldValue(Token.NOT_AVAILABLE, true);
        }
      }
    }
    if (this.oldValue == Token.NOT_AVAILABLE) {
      FilterProfile fp = this.region.getFilterProfile();
      if (this.op.guaranteesOldValue() || 
          (fp != null /* #41532 */&& fp.entryRequiresOldValue(this.getKey()))) {
        setOldValueForQueryProcessing();
      }
    }

    //setNewValueInRegion(null);
    setNewValueInRegion(owner, reentry, oldValueForDelta);
  }

  /**
   * If we are currently a create op then turn us into an update
   *
   * @since 5.0
   */
  void makeUpdate()
  {
    setOperation(this.op.getCorrespondingUpdateOp());
  }

  /**
   * If we are currently an update op then turn us into a create
   *
   * @since 5.0
   */
  void makeCreate()
  {
    setOperation(this.op.getCorrespondingCreateOp());
  }

  /**
   * Put a newValue into the given, write synced, new, region entry.
   * @throws RegionClearedException
   */
  void putNewEntry(final LocalRegion owner, final RegionEntry reentry)
      throws RegionClearedException {
    if (!this.op.guaranteesOldValue()) {  // preserves oldValue for CM ops in clients
      basicSetOldValue(null, true);
    }
    makeCreate();
    setNewValueInRegion(owner, reentry, null);
  }

  void setRegionEntry(RegionEntry re) {
    this.re = re;
  }

  RegionEntry getRegionEntry() {
    return this.re;
  }

  private void setNewValueInRegion(final LocalRegion owner,
      final RegionEntry reentry, Object oldValueForDelta) throws RegionClearedException {
    
    boolean wasTombstone = reentry.isTombstone();
    
    // put in newValue

    if (applyDelta(this.op.isCreate())) {
      if (this.isSerializationDeferred()) {
        makeSerializedNewValue(true);
      }
    }

    // If event contains new value, then it may mean that the delta bytes should
    // not be applied. This is possible if the event originated locally.
    if (this.deltaBytes != null && this.newValue == null) {
      processDeltaBytes(oldValueForDelta);
    }

    if (owner!=null) {
      owner.generateAndSetVersionTag(this, reentry);
    } else {
      this.region.generateAndSetVersionTag(this, reentry);
    }
    
    Object v = this.newValue;
    if (v == null) {
      v = isLocalInvalid() ? Token.LOCAL_INVALID : Token.INVALID;
    }
    else {
      this.region.regionInvalid = false;
    }

    reentry.setValueResultOfSearch(this.op.isNetSearch());

    //dsmith:20090524
    //This is a horrible hack, but we need to get the size of the object
    //When we store an entry. This code is only used when we do a put
    //in the primary.
    if(v instanceof com.gemstone.gemfire.Delta && region.isUsedForPartitionedRegionBucket()) {
      int vSize;
      Object ov = basicGetOldValue();
      if(ov instanceof CachedDeserializable && !GemFireCacheImpl.DELTAS_RECALCULATE_SIZE) {
        vSize = ((CachedDeserializable) ov).getValueSizeInBytes();
      } else {
        vSize = CachedDeserializableFactory.calcMemSize(v, region.getObjectSizer(), false);
      }
      v = CachedDeserializableFactory.create(v, vSize);
      basicSetNewValue(v);
    } 
 
    v = AbstractRegionMap.prepareValueForCache(this.region, v, this);

    setNewValueBucketSize(owner, v);
    
    // ezoerner:20081030 
    // last possible moment to do index maintenance with old value in
    // RegionEntry before new value is set.
    // As part of an update, this is a remove operation as prelude to an add that
    // will come after the new value is set.
    // If this is an "update" from INVALID state, treat this as a create instead
    // for the purpose of index maintenance since invalid entries are not
    // indexed.
    
    boolean isTombstone = (v == Token.TOMBSTONE);
    
    if ((this.op.isUpdate() && !reentry.isInvalid()) || this.op.isInvalidate()) {
      IndexManager idxManager = IndexUtils.getIndexManager(this.region, false);
      if (idxManager != null) {
        try {
          idxManager.updateIndexes(reentry,
                                   IndexManager.REMOVE_ENTRY,
                                   this.op.isUpdate() ?
                                     IndexProtocol.BEFORE_UPDATE_OP :
                                     IndexProtocol.OTHER_OP);
        }
        catch (QueryException e) {
          throw new IndexMaintenanceException(e);
        }
      }
    }
    final IndexUpdater indexUpdater = this.region.getIndexUpdater();
    if (indexUpdater != null) {
      boolean success = false;
      final LocalRegion indexRegion;
      if (owner != null) {
        indexRegion = owner;
      }
      else {
        indexRegion = this.region;
      }
      try {
        indexUpdater.onEvent(indexRegion, this, reentry);
        reentry.setValueWithTombstoneCheck(v, this); // already called prepareValueForCache
        success = true;
      } finally {
        indexUpdater.postEvent(indexRegion, this, reentry, success);
      }
    }
    else {
      reentry.setValueWithTombstoneCheck(v, this); // already called prepareValueForCache
    }
    if (logger.isTraceEnabled()) {
      if (v instanceof CachedDeserializable) {
        logger.trace("EntryEventImpl.setNewValueInRegion: put CachedDeserializable({},{})",
            this.getKey(), ((CachedDeserializable)v).getStringForm());
      }
      else {
        logger.trace("EntryEventImpl.setNewValueInRegion: put({},{})",
            this.getKey(), StringUtils.forceToString(v));
      }
    }

    if (!isTombstone  &&  wasTombstone) {
      owner.unscheduleTombstone(reentry);
    }
  }

  /**
   * The size the new value contributes to a pr bucket.
   * Note if this event is not on a pr then this value will be 0.
   */
  private transient int newValueBucketSize;
  public int getNewValueBucketSize() {
    return this.newValueBucketSize;
  }
  private void setNewValueBucketSize(LocalRegion lr, Object v) {
    if (lr == null) {
      lr = this.region;
    }
    this.newValueBucketSize = lr.calculateValueSize(v);
  }

  private void processDeltaBytes(Object oldValueInVM) {
    if (!this.region.hasSeenEvent(this)) {
      if (oldValueInVM == null || Token.isInvalidOrRemoved(oldValueInVM)) {
        this.region.getCachePerfStats().incDeltaFailedUpdates();
        throw new InvalidDeltaException("Old value not found for key "
            + this.keyInfo.getKey());
      }
      FilterProfile fp = this.region.getFilterProfile();
      // If compression is enabled then we've already gotten a new copy due to the
      // serializaion and deserialization that occurs.
      boolean copy = this.region.getCompressor() == null &&
          (this.region.isCopyOnRead()
          || this.region.getCloningEnabled()
          || (fp != null && fp.getCqCount() > 0));
      Object value = oldValueInVM;
      boolean wasCD = false;
      if (value instanceof CachedDeserializable) {
        wasCD = true;
        if (copy) {
          value = ((CachedDeserializable)value).getDeserializedWritableCopy(this.region, re);
        } else {
          value = ((CachedDeserializable)value).getDeserializedValue(
              this.region, re);
        }
      } else {
        if (copy) {
          value = CopyHelper.copy(value);
        }
      }
      boolean deltaBytesApplied = false;
      try {
        long start = CachePerfStats.getStatTime();
        ((com.gemstone.gemfire.Delta)value).fromDelta(new DataInputStream(
            new ByteArrayInputStream(getDeltaBytes())));
        this.region.getCachePerfStats().endDeltaUpdate(start);
        deltaBytesApplied = true;
      } catch (RuntimeException rte) {
        throw rte;
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        throw new DeltaSerializationException(
            "Exception while deserializing delta bytes.", t);
      } finally {
        if (!deltaBytesApplied) {
          this.region.getCachePerfStats().incDeltaFailedUpdates();
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Delta has been applied for key {}", getKey());
      }
      // assert event.getNewValue() == null;
      if (wasCD) {
        CachedDeserializable old = (CachedDeserializable)oldValueInVM;
        int valueSize;
        if (GemFireCacheImpl.DELTAS_RECALCULATE_SIZE) {
          valueSize = CachedDeserializableFactory.calcMemSize(value, region
              .getObjectSizer(), false);
        } else {
          valueSize = old.getValueSizeInBytes();
        }
        value = CachedDeserializableFactory.create(value, valueSize);
      }
      setNewValue(value);
      if (this.causedByMessage != null
          && this.causedByMessage instanceof PutMessage) {
        ((PutMessage)this.causedByMessage).setDeltaValObj(value);
      }
    } else {
      this.region.getCachePerfStats().incDeltaFailedUpdates();
      throw new InvalidDeltaException(
          "Cache encountered replay of event containing delta bytes for key "
              + this.keyInfo.getKey());
    }
  }

  void setTXEntryOldValue(Object oldVal, boolean mustBeAvailable)
  {
    if (Token.isInvalidOrRemoved(oldVal) || oldVal == null) {
      basicSetOldValue(null, true);
    }
    else {
      if (mustBeAvailable) {
        basicSetOldValue(oldVal, true);
      } else if (EVENT_OLD_VALUE) {
        basicSetOldValue(oldVal, true);
      } else {
        basicSetOldValue(Token.NOT_AVAILABLE, true);
      }
    }
  }

  void putValueTXEntry(final TXEntryState tx) {
    Object v = basicGetNewValue();
    if (v == null) {
      if (deltaBytes != null) {
        // since newValue is null, and we have deltaBytes
        // there must be a nearSidePendingValue
        processDeltaBytes(tx.getNearSidePendingValue());
        v = basicGetNewValue();
      } else if (this.delta != null) {
        v = this.delta;
      } else {
        v = isLocalInvalid() ? Token.LOCAL_INVALID : Token.INVALID;
      }
    }

    if (this.op != Operation.LOCAL_INVALIDATE
        && this.op != Operation.LOCAL_DESTROY) {
      // fix for bug 34387
      tx.setPendingValue(v);
    }
    tx.setCallbackArgument(getCallbackArgument());
  }

  /** @return false if entry doesn't exist */
  public boolean setOldValueFromRegion()
  {
    try {
      RegionEntry re = this.region.getRegionEntry(getKey());
      if (re == null) return false;
      Object v = re._getValueUse(this.region, true);
        return setOldValue(v);
    }
    catch (EntryNotFoundException ex) {
      return false;
    }
  }

  /** Return true if old value is the DESTROYED token */
  boolean oldValueIsDestroyedToken()
  {
    return this.oldValue == Token.DESTROYED || this.oldValue == Token.TOMBSTONE;
  }

  void setOldValueDestroyedToken()
  {
    basicSetOldValue(Token.DESTROYED, true);
  }

  /**
   * @return false if value 'v' indicates that entry does not exist
   */
  public boolean setOldValue(Object v) {
    return setOldValue(v, false);
  }
  
  
  /**
   * @param force true if the old value should be forcibly set, used
   * for HARegions, methods like putIfAbsent, etc.,
   * where the old value must be available.
   * @return false if value 'v' indicates that entry does not exist
   */
  public boolean setOldValue(Object v, boolean force) {
    if (v == null || Token.isRemoved(v)) {
      return false;
    }
    else {
      if (Token.isInvalid(v)) {
        basicSetOldValue(null, true);
      }
      else {
        if (force ||
            (this.region instanceof HARegion) // fix for bug 37909
            ) {
          basicSetOldValue(v, true);
        } else if (EVENT_OLD_VALUE) {
          // TODO Rusty add compression support here
          basicSetOldValue(v, true);
        } else {
          basicSetOldValue(Token.NOT_AVAILABLE, true);
        }
      }
      return true;
    }
  }

  /**
   * sets the old value for concurrent map operation results received
   * from a server.
   */
  public void setConcurrentMapOldValue(Object v) {
    if (Token.isRemoved(v)) {
      return;
    } else if (Token.isInvalid(v)) {
      basicSetOldValue(null, true);
    } else {
      basicSetOldValue(v, true);
    }
  }

  /** Return true if new value available */
  public boolean hasNewValue() {
    Object tmp = this.newValue;
    if (tmp == null && this.delta != null) {
      // ???:ezoerner:20080611 what if applying the delta would produce
      // null or (strangely) NOT_AVAILABLE.. do we need to apply it here to
      // find out?
      return true;
    }
    return  tmp != null && tmp != Token.NOT_AVAILABLE;
  }

  final public boolean hasOldValue() {
    return this.oldValue != null  && basicGetOldValue() != Token.NOT_AVAILABLE;
  }

  /**
   * This should only be used in case of internal delta and <B>not for Delta of
   * Delta Propagation feature</B>.
   * 
   * @return boolean
   */
  public boolean hasDelta() {
    return (this.delta != null);
  }

  public boolean isOldValueAvailable() {
    if (isOriginRemote() && this.region.isProxy()) {
      return false;
    } else {
      return basicGetOldValue() != Token.NOT_AVAILABLE;
    }
  }
  
  public void oldValueNotAvailable() {
    basicSetOldValue(Token.NOT_AVAILABLE, true);
  }

  public static Object deserialize(byte[] bytes) {
    return deserialize(bytes, null, null);
  }

  public static Object deserialize(byte[] bytes, Version version,
      ByteArrayDataInput in) {
    if (bytes == null)
      return null;
    try {
      return BlobHelper.deserializeBlob(bytes, version, in);
    }
    catch (IOException e) {
      throw new SerializationException(LocalizedStrings.EntryEventImpl_AN_IOEXCEPTION_WAS_THROWN_WHILE_DESERIALIZING.toLocalizedString(), e);
    }
    catch (ClassNotFoundException e) {
      // fix for bug 43602
      throw new SerializationException(LocalizedStrings.EntryEventImpl_A_CLASSNOTFOUNDEXCEPTION_WAS_THROWN_WHILE_TRYING_TO_DESERIALIZE_CACHED_VALUE.toLocalizedString(), e);
    }
  }

  /**
   * Serialize an object into a <code>byte[]</code>
   *
   * @throws IllegalArgumentException
   *           If <code>obj</code> should not be serialized
   */
  public static byte[] serialize(Object obj) {
    return serialize(obj, null);
  }

   /**
     * Serialize an object into a <code>byte[]</code>
     *
     * @throws IllegalArgumentException
     *           If <code>obj</code> should not be serialized
     */
  public static byte[] serialize(Object obj, Version version)
  {
    if (obj == null || obj == Token.NOT_AVAILABLE
        || Token.isInvalidOrRemoved(obj))
      throw new IllegalArgumentException(LocalizedStrings.EntryEventImpl_MUST_NOT_SERIALIZE_0_IN_THIS_CONTEXT.toLocalizedString(obj));
    try {
      return BlobHelper.serializeToBlob(obj, version);
    }
    catch (IOException e) {
      throw new SerializationException(LocalizedStrings.EntryEventImpl_AN_IOEXCEPTION_WAS_THROWN_WHILE_SERIALIZING.toLocalizedString(), e);
    }
  }
  
  
  /**
   * Serialize an object into a <code>byte[]</code> . If the byte array
   * provided by the wrapper is sufficient to hold the data, it is used
   * otherwise a new byte array gets created & its reference is stored in the
   * wrapper. The User Bit is also appropriately set as Serialized
   * 
   * @param wrapper
   *                Object of type BytesAndBitsForCompactor which is used to fetch
   *                the serialized data. The byte array of the wrapper is used
   *                if possible else a the new byte array containing the data is
   *                set in the wrapper.
   * @throws IllegalArgumentException
   *                 If <code>obj</code> should not be serialized
   */
  public static void fillSerializedValue(BytesAndBitsForCompactor wrapper,
                                         Object obj, byte userBits) {
    if (obj == null || obj == Token.NOT_AVAILABLE
        || Token.isInvalidOrRemoved(obj))
      throw new IllegalArgumentException(
        LocalizedStrings.EntryEvents_MUST_NOT_SERIALIZE_0_IN_THIS_CONTEXT.toLocalizedString(obj));
    try {
      HeapDataOutputStream hdos = null;
      if (wrapper.getBytes().length < 32) {
        hdos = new HeapDataOutputStream(Version.CURRENT);
      }
      else {
        hdos = new HeapDataOutputStream(wrapper.getBytes());
      }
      DataSerializer.writeObject(obj, hdos);
      // return hdos.toByteArray();
      hdos.sendTo(wrapper, userBits);
    }
    catch (IOException e) {
      RuntimeException e2 = new IllegalArgumentException(
        LocalizedStrings.EntryEventImpl_AN_IOEXCEPTION_WAS_THROWN_WHILE_SERIALIZING.toLocalizedString());
      e2.initCause(e);
      throw e2;
    }
  }

  String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length()+1);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(getShortClassName());
    buf.append("[");

    buf.append("op=");
    buf.append(getOperation());
    buf.append(";key=");
    buf.append(this.getKey());
    buf.append(";oldValue=");
    buf.append(basicGetOldValue());
    buf.append(";newValue=");
    buf.append(basicGetNewValue());
    buf.append(";callbackArg=");
    buf.append(this.getRawCallbackArgument());
    buf.append(";originRemote=");
    buf.append(isOriginRemote());
    buf.append(";originMember=");
    buf.append(getDistributedMember());
//    if (this.partitionMessage != null) {
//      buf.append("; partitionMessage=");
//      buf.append(this.partitionMessage);
//    }
    if (this.isPossibleDuplicate()) {
      buf.append(";posDup");
    }
    if (callbacksInvoked()) { 
      buf.append(";callbacksInvoked");
    }
    if (this.versionTag != null) {
      buf.append(";version=").append(this.versionTag);
    }
    if (getContext() != null) {
      buf.append(";context=");
      buf.append(getContext());
    }
    if (this.eventID != null) {
      buf.append(";id=");
      buf.append(this.eventID);
    }
    if (this.deltaBytes != null) {
      buf.append(";[" + this.deltaBytes.length + " deltaBytes]");
    }
//    else {
//      buf.append(";[no deltaBytes]");
//    }
    if (this.filterInfo != null) {
      buf.append(";routing=");
      buf.append(this.filterInfo);
    }
    if (this.isFromServer()) {
      buf.append(";isFromServer");
    }
    if (this.isConcurrencyConflict()) {
      buf.append(";isInConflict");
    }
    if (this.getInhibitDistribution()) {
      buf.append(";inhibitDistribution");
    }
    buf.append("]");
    return buf.toString();
  }

  public int getDSFID() {
    return ENTRY_EVENT;
  }

  public void toData(DataOutput out) throws IOException
  {
    DataSerializer.writeObject(this.eventID, out);    
    DataSerializer.writeObject(this.getKey(), out);
    DataSerializer.writeObject(this.keyInfo.getValue(), out);
    out.writeByte(this.op.ordinal);
    out.writeShort(this.eventFlags & EventFlags.FLAG_TRANSIENT_MASK);
    DataSerializer.writeObject(this.getRawCallbackArgument(), out);
    DataSerializer.writeObject(this.txId, out);

    {
      boolean isDelta = this.delta != null;
      out.writeBoolean(isDelta);
      if (isDelta) {
        DataSerializer.writeObject(this.delta, out);
      }
      else {
        Object nv = basicGetNewValue();
        boolean newValueSerialized = nv instanceof CachedDeserializable;
        out.writeBoolean(newValueSerialized);
        if (newValueSerialized) {
          if (this.newValueBytes != null) {
            DataSerializer.writeByteArray(this.newValueBytes, out);
          } else if (this.cachedSerializedNewValue != null) {
            DataSerializer.writeByteArray(this.cachedSerializedNewValue, out);
          } else {
            CachedDeserializable cd = (CachedDeserializable)nv;
            DataSerializer.writeObjectAsByteArray(cd.getValue(), out);
          }
        }
        else {
          DataSerializer.writeObject(nv, out);
        }
      }  
    }

    {
      Object ov = basicGetOldValue();
      boolean oldValueSerialized = ov instanceof CachedDeserializable;
      out.writeBoolean(oldValueSerialized);
      if (oldValueSerialized) {
        if (this.oldValueBytes != null) {
          DataSerializer.writeByteArray(this.oldValueBytes, out);
        }
        else {
          CachedDeserializable cd = (CachedDeserializable)ov;
          DataSerializer.writeObjectAsByteArray(cd.getValue(), out);
        }
      }
      else {
        DataSerializer.writeObject(ov, out);
      }
    }
    InternalDataSerializer.invokeToData((InternalDistributedMember)this.distributedMember, out);
    DataSerializer.writeObject(getContext(), out);
    DataSerializer.writeLong(tailKey, out);
  }

  private static abstract class EventFlags
   {
    private static final short FLAG_ORIGIN_REMOTE = 0x01;
    // localInvalid: true if a null new value should be treated as a local
    // invalid.
    private static final short FLAG_LOCAL_INVALID = 0x02;
    private static final short FLAG_GENERATE_CALLBACKS = 0x04;
    private static final short FLAG_POSSIBLE_DUPLICATE = 0x08;
    private static final short FLAG_INVOKE_PR_CALLBACKS = 0x10;
    private static final short FLAG_CONCURRENCY_CONFLICT = 0x20;
    private static final short FLAG_INHIBIT_LISTENER_NOTIFICATION = 0x40;
    private static final short FLAG_CALLBACKS_INVOKED = 0x80;
    private static final short FLAG_ISCREATE = 0x100;
    private static final short FLAG_SERIALIZATION_DEFERRED = 0x200;
    private static final short FLAG_FROM_SERVER = 0x400;
    private static final short FLAG_FROM_RI_LOCAL_DESTROY = 0x800;
    private static final short FLAG_INHIBIT_DISTRIBUTION = 0x1000;
    private static final short FLAG_REDESTROYED_TOMBSTONE = 0x2000;
    private static final short FLAG_INHIBIT_ALL_NOTIFICATIONS= 0x4000;
    
    /** mask for clearing transient flags when serializing */
    private static final short FLAG_TRANSIENT_MASK =
      ~(FLAG_CALLBACKS_INVOKED
          | FLAG_ISCREATE
          | FLAG_INHIBIT_LISTENER_NOTIFICATION
          | FLAG_SERIALIZATION_DEFERRED
          | FLAG_FROM_SERVER
          | FLAG_FROM_RI_LOCAL_DESTROY
          | FLAG_INHIBIT_DISTRIBUTION
          | FLAG_REDESTROYED_TOMBSTONE
          );
    
    protected static final boolean isSet(short flags, short mask)
    {
      return (flags & mask) != 0;
    }

    /** WARNING: Does not set the bit in place, returns new short with bit set */
    protected static final short set(short flags, short mask, boolean on)
    {
      return (short)(on ? (flags | mask) : (flags & ~mask));
    }
  }

  public SerializedCacheValue<?> getSerializedOldValue() {
    final Object tmp = basicGetOldValue();
    if (tmp instanceof CachedDeserializable) {
      return new SerializedCacheValueImpl(this.region, this.re,
          (CachedDeserializable)tmp, this.oldValueBytes);
    }
    else {
      return null;
    }
  }

  /**
   * Compute an estimate of the size of the new value
   * for a PR. Since PR's always store values in a cached deserializable
   * we need to compute its size as a blob.
   *
   * @return the size of serialized bytes for the new value
   */
  public int getNewValSizeForPR()
  {
    int newSize = 0;
    applyDelta(false);
    Object v = basicGetNewValue();
    if (v != null) {
      try {
        newSize = CachedDeserializableFactory.calcSerializedSize(v)
          + CachedDeserializableFactory.overhead();
      } catch (IllegalArgumentException iae) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.EntryEventImpl_DATASTORE_FAILED_TO_CALCULATE_SIZE_OF_NEW_VALUE), iae);
        newSize = 0;
      }
    }
    return newSize;
  }

  /**
   * Compute an estimate of the size of the old value
   *
   * @return the size of serialized bytes for the old value
   */
  public int getOldValSize()
  {
    int oldSize = 0;
    if (hasOldValue()) {
      try {
        oldSize = CachedDeserializableFactory.calcMemSize(basicGetOldValue());
      } catch (IllegalArgumentException iae) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.EntryEventImpl_DATASTORE_FAILED_TO_CALCULATE_SIZE_OF_OLD_VALUE), iae);
        oldSize = 0;
      }
    }
    return oldSize;
  }

  public EnumListenerEvent getEventType() {
    return this.eventType;
  }

  /**
   * Sets the operation type.
   * @param eventType
   */
  public void setEventType(EnumListenerEvent eventType) {
    this.eventType = eventType;
  }
  
  /**
   * set this to true after dispatching the event to a cache listener
   */
  public void callbacksInvoked(boolean dispatched) {
    setEventFlag(EventFlags.FLAG_CALLBACKS_INVOKED, dispatched);
  }
  
  /**
   * has this event been dispatched to a cache listener?
   */
  public boolean callbacksInvoked() {
    return testEventFlag(EventFlags.FLAG_CALLBACKS_INVOKED);
  }
  
  /**
   * set this to true to inhibit application cache listener notification
   * during event dispatching
   */
  public void inhibitCacheListenerNotification(boolean inhibit) {
    setEventFlag(EventFlags.FLAG_INHIBIT_LISTENER_NOTIFICATION, inhibit);
  }
  
  /**
   * are events being inhibited from dispatch to application cache listeners
   * for this event?
   */
  public boolean inhibitCacheListenerNotification() {
    return testEventFlag(EventFlags.FLAG_INHIBIT_LISTENER_NOTIFICATION);
  }
  
  
  /**
   * dispatch listener events for this event
   * @param notifyGateways pass the event on to WAN queues
   */
  void invokeCallbacks(LocalRegion rgn,boolean skipListeners, boolean notifyGateways) {
    if (!callbacksInvoked()) {
      callbacksInvoked(true);
      if (this.op.isUpdate()) {
        rgn.invokePutCallbacks(EnumListenerEvent.AFTER_UPDATE, this,
            !skipListeners, notifyGateways); // gateways are notified in part2 processing
      }
      else if (this.op.isCreate()) {
        rgn.invokePutCallbacks(EnumListenerEvent.AFTER_CREATE, this,
            !skipListeners, notifyGateways);
      }
      else if (this.op.isDestroy()) {
        rgn.invokeDestroyCallbacks(EnumListenerEvent.AFTER_DESTROY,
            this, !skipListeners, notifyGateways);
      }
      else if (this.op.isInvalidate()) {
        rgn.invokeInvalidateCallbacks(EnumListenerEvent.AFTER_INVALIDATE,
            this, !skipListeners);
      }
    }
  }
  
  private void setFromRILocalDestroy(boolean on) {
    setEventFlag(EventFlags.FLAG_FROM_RI_LOCAL_DESTROY, on);
  }
  
  public boolean isFromRILocalDestroy(){
    return testEventFlag(EventFlags.FLAG_FROM_RI_LOCAL_DESTROY);
  }

  protected Long tailKey = -1L;

  /**
   * Return true if this event came from a server by the client doing a get.
   * @since 5.7
   */
  public boolean isFromServer() {
    return testEventFlag(EventFlags.FLAG_FROM_SERVER);
  }
  /**
   * Sets the fromServer flag to v.  This must be set to true if an event
   * comes from a server while the affected region entry is not locked.  Among
   * other things it causes version conflict checks to be performed to protect
   * against overwriting a newer version of the entry.
   * @since 5.7
   */
  public void setFromServer(boolean v) {
    setEventFlag(EventFlags.FLAG_FROM_SERVER, v);
  }

  /**
   * If true, the region associated with this event had already
   * applied the operation it encapsulates when an attempt was
   * made to apply the event.
   * @return the possibleDuplicate
   */
  public boolean isPossibleDuplicate() {
    return testEventFlag(EventFlags.FLAG_POSSIBLE_DUPLICATE);
  }

  /**
   * If the operation encapsulated by this event has already been
   * seen by the region to which it pertains, this flag should be
   * set to true. 
   * @param possibleDuplicate the possibleDuplicate to set
   */
  public void setPossibleDuplicate(boolean possibleDuplicate) {
    setEventFlag(EventFlags.FLAG_POSSIBLE_DUPLICATE, possibleDuplicate);
  }


  /**
   * are events being inhibited from dispatch to to gateway/async queues, 
   * client queues, cache listener and cache write. If set, sending
   * notifications for the data that is read from a persistent store (HDFS) and 
   * is being reinserted in the cache is skipped.
   */
  public boolean inhibitAllNotifications() {
    return testEventFlag(EventFlags.FLAG_INHIBIT_ALL_NOTIFICATIONS);
    
  }
  
  /**
   * set this to true to inhibit notifications that are sent to gateway/async queues, 
   * client queues, cache listener and cache write. This is used to skip sending
   * notifications for the data that is read from a persistent store (HDFS) and 
   * is being reinserted in the cache 
   */
  public void setInhibitAllNotifications(boolean inhibit) {
    setEventFlag(EventFlags.FLAG_INHIBIT_ALL_NOTIFICATIONS, inhibit);
  }
  
  /**
   * sets the routing information for cache clients
   */
  public void setLocalFilterInfo(FilterInfo info) {
    this.filterInfo = info;
  }
  
  /**
   * retrieves the routing information for cache clients in this VM
   */
  public FilterInfo getLocalFilterInfo() {
    return this.filterInfo;
  }

  
  public LocalRegion getLocalRegion() {
    return this.region;
  }

  /**
   * This method returns the delta bytes used in Delta Propagation feature.
   * <B>For internal delta, see getRawNewValue().</B>
   * 
   * @return delta bytes
   */
  public byte[] getDeltaBytes() {
    return deltaBytes;
  }

  /**
   * This method sets the delta bytes used in Delta Propagation feature. <B>For
   * internal delta, see setNewValue().</B>
   * 
   * @param deltaBytes
   */
  public void setDeltaBytes(byte[] deltaBytes) {
    this.deltaBytes = deltaBytes;
  }

  // TODO (ashetkar) Can this.op.isCreate() be used instead?
  public boolean isCreate() {
    return testEventFlag(EventFlags.FLAG_ISCREATE);
  }

  /**
   * this is used to distinguish an event that merely has Operation.CREATE
   * from one that originated from Region.create() for delta processing
   * purposes.
   */
  public EntryEventImpl setCreate(boolean isCreate) {
    setEventFlag(EventFlags.FLAG_ISCREATE, isCreate);
    return this;
  }

  public final void setContextObject(Object ctx) {
    this.contextObj = ctx;
  }

  public final Object getContextObject() {
    return this.contextObj;
  }

  /**
   * @return the keyInfo
   */
  public KeyInfo getKeyInfo() {
    return keyInfo;
  }

  public void setKeyInfo(KeyInfo keyInfo) {
    this.keyInfo = keyInfo;
  }

  /**
   * establish the old value in this event as the current cache value,
   * whether in memory or on disk
   */
  public void setOldValueForQueryProcessing() {
    RegionEntry reentry = this.region.entries.getEntry(this.getKey());
    if (reentry != null) {
      Object v = reentry.getValueOffHeapOrDiskWithoutFaultIn(this.region);
      if ( !(v instanceof Token) ) {
        basicSetOldValue(v, false);
      }
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  /**
   * @param versionTag the versionTag to set
   */
  public void setVersionTag(VersionTag versionTag) {
    this.versionTag = versionTag;
  }
  
  /**
   * @return the concurrency versioning tag for this event, if any
   */
  public VersionTag getVersionTag() {
    return this.versionTag;
  }
  
  /**
   * this method joins together version tag timestamps and the "lastModified"
   * timestamps generated and stored in entries.  If a change does not already
   * carry a lastModified timestamp 
   * @param suggestedTime
   * @return the timestamp to store in the entry
   */
  public long getEventTime(long suggestedTime) {
    long result = suggestedTime;
    if (this.versionTag != null) {
      if (suggestedTime != 0) {
        this.versionTag.setVersionTimeStamp(suggestedTime);
      } else {
        result = this.versionTag.getVersionTimeStamp();
      }
    }
    if (result <= 0) {
      LocalRegion region = this.getLocalRegion();
      if (region != null) {
        result = region.cacheTimeMillis();
      } else {
        result = System.currentTimeMillis();
      }
    }
    return result;
  }


  // TODO this class is trouble for OffHeap storage.
  // If the cd is a OffHeapCachedDeserializable we don't want to have a reference
  // to it in this class since customers can hold onto SerializedCacheValue
  // as long as they want and we would need to use finalization to know when
  // they are done with it.
  // TODO this class is also used in a number of places internally and those
  // we want to be optimized. I think those places can be changed not to use
  // this class.
  public static class SerializedCacheValueImpl
    implements SerializedCacheValue, CachedDeserializable, Sendable
  {
    private final CachedDeserializable cd;
    private final Region r;
    private final RegionEntry re;
    private final byte[] serializedValue;
    
    SerializedCacheValueImpl(Region r, RegionEntry re, CachedDeserializable cd, byte[] serializedBytes) {
      this.r = r;
      this.re = re;
      this.cd = cd;
      this.serializedValue = serializedBytes;
    }

    public byte[] getSerializedValue() {
      if(this.serializedValue != null){
        return this.serializedValue;
      }
      return this.cd.getSerializedValue();
    }
    
    public Object getDeserializedValue() {
      return getDeserializedValue(this.r, this.re);
    }
    public Object getDeserializedForReading() {
      return this.cd.getDeserializedForReading();
    }
    public Object getDeserializedWritableCopy(Region rgn, RegionEntry entry) {
      return this.cd.getDeserializedWritableCopy(rgn, entry);
    }

    public Object getDeserializedValue(Region rgn, RegionEntry reentry) {
      return this.cd.getDeserializedValue(rgn, reentry);
    }
    public Object getValue() {
      if(this.serializedValue != null){
        return this.serializedValue;
      }
      return this.cd.getValue();
    }
    public void writeValueAsByteArray(DataOutput out) throws IOException {
      if (this.serializedValue != null) {
        DataSerializer.writeByteArray(this.serializedValue, out);
      } else {
        this.cd.writeValueAsByteArray(out);
      }
    }
    public void fillSerializedValue(BytesAndBitsForCompactor wrapper, byte userBits) {
      if (this.serializedValue != null) {
        wrapper.setData(this.serializedValue, userBits, this.serializedValue.length, 
                        false /* Not Reusable as it refers to underlying value */);
      } else {
        this.cd.fillSerializedValue(wrapper, userBits);
      }
    }
    public int getValueSizeInBytes() {
      return this.cd.getValueSizeInBytes();
    }
    public int getSizeInBytes() {
      return this.cd.getSizeInBytes();
    }

    public String getStringForm() {
      return this.cd.getStringForm();
    }

    @Override
    public void sendTo(DataOutput out) throws IOException {
      DataSerializer.writeObject(this.cd, out);
    }
  }
//////////////////////////////////////////////////////////////////////////////////////////
  
  public void setTailKey(Long tailKey) {
    this.tailKey = tailKey;
  }

  public Long getTailKey() {
    return this.tailKey;
  }

  private Thread invokeCallbacksThread;
  
  /**
   * Mark this event as having its callbacks invoked by the current thread.
   * Note this is done just before the actual invocation of the callbacks.
   */
  public void setCallbacksInvokedByCurrentThread() {
    this.invokeCallbacksThread = Thread.currentThread();
  }
  
  /**
   * Return true if this event was marked as having its callbacks invoked
   * by the current thread.
   */
  public boolean getCallbacksInvokedByCurrentThread() {
    if (this.invokeCallbacksThread == null) return false;
    return Thread.currentThread().equals(this.invokeCallbacksThread);
  }
  
  /**
   * Returns whether this event is on the PDX type region.
   * @return whether this event is on the PDX type region
   */
  public boolean isOnPdxTypeRegion() {
    return PeerTypeRegistration.REGION_FULL_PATH.equals(this.region.getFullPath());
  }
  
  /**
   * returns true if it is okay to process this event even though it has
   * a null version
   */
  public boolean noVersionReceivedFromServer() {
    return versionTag == null
      && region.concurrencyChecksEnabled
      && region.getServerProxy() != null
      && !op.isLocal()
      && !isOriginRemote()
      ;
  }
  
  /** returns a copy of this event with the additional fields for WAN conflict resolution */
  public TimestampedEntryEvent getTimestampedEvent(
      final int newDSID, final int oldDSID,
      final long newTimestamp, final long oldTimestamp) {
    return new TimestampedEntryEventImpl(this, newDSID, oldDSID, newTimestamp, oldTimestamp);
  }

  private void setSerializationDeferred(boolean serializationDeferred) {
    setEventFlag(EventFlags.FLAG_SERIALIZATION_DEFERRED, serializationDeferred);
  }

  private boolean isSerializationDeferred() {
    return testEventFlag(EventFlags.FLAG_SERIALIZATION_DEFERRED);
  }
  
  public boolean isSingleHop() {
    return (this.causedByMessage != null && this.causedByMessage instanceof RemoteOperationMessage);
  }

  public boolean isSingleHopPutOp() {
    return (this.causedByMessage != null && this.causedByMessage instanceof RemotePutMessage);
  }
}
