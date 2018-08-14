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

import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.TX_ENTRY_STATE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.pdx.PdxSerializationException;

/**
 * TXEntryState is the entity that tracks transactional changes, except for those tracked by
 * {@link TXEntryUserAttrState}, to an entry.
 *
 * @since GemFire 4.0
 */
public class TXEntryState implements Releasable {
  private static final Logger logger = LogService.getLogger();

  /**
   * This field is final except for when it is nulled out during cleanup
   */
  @Retained(TX_ENTRY_STATE)
  private Object originalVersionId;
  private final Object originalValue;

  /**
   * Serial number that is set each time this entry is modified. Used to order the events in a
   * TransactionEvent.
   */
  protected int modSerialNum;
  /**
   * Used to remember the event id to use on the farSide for this entry. See bug 39434.
   *
   * @since GemFire 5.7
   */
  private int farSideEventOffset = -1;
  /**
   * Used to remember the event id to use on the nearSide for this entry. See bug 39434.
   *
   * @since GemFire 5.7
   */
  private int nearSideEventOffset = -1;

  private Object pendingValue;

  private byte[] serializedPendingValue;

  /**
   * Remember the callback argument for listener invocation
   */
  private Object callBackArgument;

  private byte op;

  /**
   * destroy field remembers the strongest destroy op perfomed on this entry
   */
  private byte destroy; // DESTROY_NONE, DESTROY_LOCAL, DESTROY_DISTRIBUTED

  // ORDER of the following is important to the implementation!
  private static final byte DESTROY_NONE = 0;

  private static final byte DESTROY_LOCAL = 1;

  private static final byte DESTROY_DISTRIBUTED = 2;

  // ORDER of the following is important to the implementation!
  private static final byte OP_NULL = 0;

  private static final byte OP_L_DESTROY = 1;

  private static final byte OP_CREATE_LD = 2;

  private static final byte OP_LLOAD_CREATE_LD = 3;

  private static final byte OP_NLOAD_CREATE_LD = 4;

  private static final byte OP_PUT_LD = 5;

  private static final byte OP_LLOAD_PUT_LD = 6;

  private static final byte OP_NLOAD_PUT_LD = 7;

  private static final byte OP_D_INVALIDATE_LD = 8;

  private static final byte OP_D_DESTROY = 9;

  private static final byte OP_L_INVALIDATE = 10;

  private static final byte OP_PUT_LI = 11;

  private static final byte OP_LLOAD_PUT_LI = 12;

  private static final byte OP_NLOAD_PUT_LI = 13;

  private static final byte OP_D_INVALIDATE = 14;

  private static final byte OP_CREATE_LI = 15;

  private static final byte OP_LLOAD_CREATE_LI = 16;

  private static final byte OP_NLOAD_CREATE_LI = 17;

  private static final byte OP_CREATE = 18;

  private static final byte OP_SEARCH_CREATE = 19;

  private static final byte OP_LLOAD_CREATE = 20;

  private static final byte OP_NLOAD_CREATE = 21;

  private static final byte OP_LOCAL_CREATE = 22;

  private static final byte OP_PUT = 23;

  private static final byte OP_SEARCH_PUT = 24;

  private static final byte OP_LLOAD_PUT = 25;

  private static final byte OP_NLOAD_PUT = 26;

  static {
    Assert.assertTrue(OP_SEARCH_PUT - OP_PUT == OP_SEARCH_CREATE - OP_CREATE,
        "search offset inconsistent");
    Assert.assertTrue(OP_LLOAD_PUT - OP_PUT == OP_LLOAD_CREATE - OP_CREATE,
        "lload offset inconsistent");
    Assert.assertTrue(OP_NLOAD_PUT - OP_PUT == OP_NLOAD_CREATE - OP_CREATE,
        "nload offset inconsistent");
  }

  /**
   * System property to be set when read conflicts should be detected. Benefits of read conflict
   * detection are at: https://wiki.gemstone.com/display/PR/Read+conflict+detection
   */
  private static final boolean DETECT_READ_CONFLICTS =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "detectReadConflicts");

  // TODO: optimize footprint by having this field on a subclass
  // that is only created by TXRegionState when it knows its region needs refCounts.
  /**
   * A reference to the RegionEntry, in committed state, that this tx entry has referenced. Note:
   * this field is only needed if the committed region has eviction or expiration enabled. In both
   * those cases the tx needs to do reference counting.
   */
  private final RegionEntry refCountEntry;

  /**
   * Set to true once this entry has been written by a tx operation.
   */
  private boolean dirty;

  /**
   * Set to true if this operation is result of a bulk op. We use this boolean to determine op type
   * rather than extending the operation algebra.
   */
  private boolean bulkOp;

  private FilterRoutingInfo filterRoutingInfo = null;
  private Set<InternalDistributedMember> adjunctRecipients = null;

  /**
   * versionTag to be sent to other members. This is propogated up from AbstractRegionMap and then
   * used while building TXCommitMessage
   */
  private VersionTag versionTag = null;

  /**
   * tailKey (used by wan) to be sent to other members. This is propagated up from AbstractRegionMap
   * and then used while building TXCommitMessage
   */
  private long tailKey = -1;

  /**
   * versionTag that is fetched from remote members, if this member's data policy is not REPLICATE
   */
  private VersionTag remoteVersionTag = null;

  /**
   * Next region version generated on the primary
   */
  private long nextRegionVersion = -1;

  /*
   * For Distributed Transaction. THis value is set when applying commit
   */
  private transient DistTxThinEntryState distTxThinEntryState;

  /**
   * Use this system property if you need to display/log string values in conflict messages
   */
  private static final boolean VERBOSE_CONFLICT_STRING =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "verboseConflictString");

  /**
   * This constructor is used to create a singleton used by LocalRegion to signal that noop
   * invalidate op has been performed. The instance returned by this constructor is just a marker;
   * it is not good for anything else.
   */
  protected TXEntryState() {
    this.op = OP_NULL;
    this.originalVersionId = Token.REMOVED_PHASE1;
    this.originalValue = Token.REMOVED_PHASE1;
    this.refCountEntry = null;
  }

  private TXRegionState txRegionState = null;

  /**
   * This constructor is used when creating an entry
   */
  protected TXEntryState(RegionEntry re, Object pvId, Object pv, TXRegionState txRegionState,
      boolean isDistributed) {
    Object vId = pvId;
    if (vId == null) {
      vId = Token.REMOVED_PHASE1;
    }
    if (pv == null) {
      pv = Token.REMOVED_PHASE1;
    }
    this.op = OP_NULL;
    this.pendingValue = pv;
    this.originalVersionId = vId;
    this.originalValue = pv;
    if (txRegionState.needsRefCounts()) {
      this.refCountEntry = re;
      if (re != null) {
        re.incRefCount();
      }
    } else {
      this.refCountEntry = null;
    }
    this.txRegionState = txRegionState;
    if (isDistributed) {
      this.distTxThinEntryState = new DistTxThinEntryState();
    }
  }

  private byte[] getSerializedPendingValue() {
    return serializedPendingValue;
  }

  private void setSerializedPendingValue(byte[] serializedPendingValue) {
    this.serializedPendingValue = serializedPendingValue;
  }

  void serializePendingValue() {
    Object pendingValue = getPendingValue();
    if (Token.isInvalidOrRemoved(pendingValue)) {
      // token
      return;
    }
    if (pendingValue instanceof byte[]) {
      // byte[]
      return;
    }

    // CachedDeserialized, Object or PDX
    if (pendingValue instanceof CachedDeserializable) {
      CachedDeserializable cachedDeserializable = (CachedDeserializable) pendingValue;
      setSerializedPendingValue(cachedDeserializable.getSerializedValue());
    } else {
      setSerializedPendingValue(EntryEventImpl.serialize(pendingValue));
    }
  }

  public TXRegionState getTXRegionState() {
    return txRegionState;
  }

  public Object getOriginalVersionId() {
    return this.originalVersionId;
  }

  public Object getOriginalValue() {
    return this.originalValue;
  }

  public Object getPendingValue() {
    return this.pendingValue;
  }

  public Object getCallbackArgument() {
    return this.callBackArgument;
  }

  /**
   * Gets the pending value for near side operations. Special cases local destroy and local
   * invalidate to fix bug 34387.
   */
  @Unretained
  public Object getNearSidePendingValue() {
    if (isOpLocalDestroy()) {
      return null;
    } else if (isOpLocalInvalidate()) {
      return Token.LOCAL_INVALID;
    } else {
      return getPendingValue();
    }
  }

  void setPendingValue(Object pv) {
    this.pendingValue = pv;
  }

  void setCallbackArgument(Object callbackArgument) {
    this.callBackArgument = callbackArgument;
  }

  /**
   * Fetches the current modification serial number from the txState and puts it in this entry.
   *
   * @param modNum the next modified serial number gotten from TXState
   *
   * @since GemFire 5.0
   */
  public void updateForWrite(final int modNum) {
    this.dirty = true;
    this.modSerialNum = modNum;
  }

  /**
   * Returns true if this entry has been written; false if only read
   *
   * @since GemFire 5.1
   */
  public boolean isDirty() {
    return DETECT_READ_CONFLICTS || this.dirty;
  }

  /**
   * Return true if entry has an operation.
   *
   * @since GemFire 5.5
   */
  public boolean hasOp() {
    return this.op != OP_NULL;
  }

  /**
   * Returns true if the transaction state has this entry existing "locally". Returns false if the
   * transaction is going to remove this entry.
   */
  public boolean existsLocally() {
    if (this.op == OP_NULL) {
      return !Token.isRemoved(getOriginalValue());
    } else {
      return this.op > OP_D_DESTROY;
    }
  }

  private boolean isOpLocalDestroy() {
    return this.op >= OP_L_DESTROY && this.op <= OP_D_INVALIDATE_LD;
  }

  private boolean isOpLocalInvalidate() {
    return this.op >= OP_L_INVALIDATE && this.op <= OP_NLOAD_CREATE_LI
        && this.op != OP_D_INVALIDATE;
  }

  /**
   * Return true if this transaction has completely invalidated or destroyed the value of this entry
   * in the entire distributed system. Return false if a netsearch should be done.
   */
  public boolean noValueInSystem() {
    if (this.op == OP_D_DESTROY || this.op == OP_D_INVALIDATE_LD || this.op == OP_D_INVALIDATE) {
      return true;
    } else if (getNearSidePendingValue() == Token.INVALID) {
      // Note that we are not interested in LOCAL_INVALID
      return (this.op >= OP_CREATE_LD && this.op != OP_L_INVALIDATE && this.op != OP_SEARCH_CREATE
          && this.op != OP_LOCAL_CREATE && this.op != OP_SEARCH_PUT);
    } else {
      return false;
    }
  }

  /**
   * Returns true if the transaction state has this entry existing locally and has a valid local
   * value. Returns false if the transaction is going to remove this entry or its local value is
   * invalid.
   */
  public boolean isLocallyValid(boolean isProxy) {
    if (this.op == OP_NULL) {
      if (isProxy) {
        // If it is a proxy that consider it locally valid
        // since we don't have any local committed state
        return true;
      } else {
        return !Token.isInvalidOrRemoved(getOriginalValue());
      }
    } else {
      return this.op >= OP_CREATE && !Token.isInvalid(getNearSidePendingValue());
    }
  }

  @Retained
  public Object getValueInVM(Object key) throws EntryNotFoundException {
    if (!existsLocally()) {
      throw new EntryNotFoundException(String.valueOf(key));
    }
    return getNearSidePendingValue();
  }

  /**
   * @return the value, or null if the value does not exist in the cache, Token.INVALID or
   *         Token.LOCAL_INVALID if the value is invalid
   */
  public Object getValue(Object key, Region r, boolean preferCD) {
    if (!existsLocally()) {
      return null;
    }
    Object v = getNearSidePendingValue();
    if (v instanceof CachedDeserializable && !preferCD) {
      // The only reason we would is if we do a read
      // in a transaction and the initial op that created the TXEntryState
      // did not call getDeserializedValue.
      // If a write op is done then the pendingValue does not belong to
      // the cache yet so we do not need the RegionEntry.
      // If we do need the RegionEntry then TXEntryState
      // will need to be changed to remember the RegionEntry it is created with.
      v = ((CachedDeserializable) v).getDeserializedValue(r, this.refCountEntry);
    }

    return v;
  }

  private boolean isOpCreate() {
    return this.op >= OP_CREATE_LI && this.op <= OP_LOCAL_CREATE;
  }

  boolean isOpCreateEvent() {
    return isOpCreate();
  }

  private boolean isOpPut() {
    return this.op >= OP_PUT;
  }

  protected boolean isOpPutEvent() {
    return isOpPut();
  }

  private boolean isOpInvalidate() {
    // Note that OP_CREATE_LI, OP_LLOAD_CREATE_LI, and OP_NLOAD_CREATE_LI
    // do not return true here because they are actually creates
    // with a value of LOCAL_INVALID locally and some other value remotely.
    return this.op <= OP_D_INVALIDATE && this.op >= OP_L_INVALIDATE;
  }

  boolean isOpInvalidateEvent() {
    return isOpInvalidate();
  }

  private boolean isOpDestroy() {
    return this.op <= OP_D_DESTROY && this.op >= OP_L_DESTROY;
  }

  boolean isOpDestroyEvent(InternalRegion r) {
    // Note that if the region is a proxy then we go ahead and distributed
    // the destroy because we can't eliminate it based on committed state
    return isOpDestroy()
        && (r.isProxy() || (getOriginalValue() != null && !Token.isRemoved(getOriginalValue())));
  }

  /**
   * Returns true if this operation has an event for the tx listener
   *
   * @since GemFire 5.0
   */
  boolean isOpAnyEvent(InternalRegion r) {
    return isOpPutEvent() || isOpCreateEvent() || isOpInvalidateEvent() || isOpDestroyEvent(r);
  }

  boolean isOpSearch() {
    return this.op == OP_SEARCH_CREATE || this.op == OP_SEARCH_PUT;
  }

  String opToString() {
    return opToString(this.op);
  }

  private String opToString(byte opCode) {
    switch (opCode) {
      case OP_NULL:
        return "OP_NULL";
      case OP_L_DESTROY:
        return "OP_L_DESTROY";
      case OP_CREATE_LD:
        return "OP_CREATE_LD";
      case OP_LLOAD_CREATE_LD:
        return "OP_LLOAD_CREATE_LD";
      case OP_NLOAD_CREATE_LD:
        return "OP_NLOAD_CREATE_LD";
      case OP_PUT_LD:
        return "OP_PUT_LD";
      case OP_LLOAD_PUT_LD:
        return "OP_LLOAD_PUT_LD";
      case OP_NLOAD_PUT_LD:
        return "OP_NLOAD_PUT_LD";
      case OP_D_INVALIDATE_LD:
        return "OP_D_INVALIDATE_LD";
      case OP_D_DESTROY:
        return "OP_D_DESTROY";
      case OP_L_INVALIDATE:
        return "OP_L_INVALIDATE";
      case OP_PUT_LI:
        return "OP_PUT_LI";
      case OP_LLOAD_PUT_LI:
        return "OP_LLOAD_PUT_LI";
      case OP_NLOAD_PUT_LI:
        return "OP_NLOAD_PUT_LI";
      case OP_D_INVALIDATE:
        return "OP_D_INVALIDATE";
      case OP_CREATE_LI:
        return "OP_CREATE_LI";
      case OP_LLOAD_CREATE_LI:
        return "OP_LLOAD_CREATE_LI";
      case OP_NLOAD_CREATE_LI:
        return "OP_NLOAD_CREATE_LI";
      case OP_CREATE:
        return "OP_CREATE";
      case OP_SEARCH_CREATE:
        return "OP_SEARCH_CREATE";
      case OP_LLOAD_CREATE:
        return "OP_LLOAD_CREATE";
      case OP_NLOAD_CREATE:
        return "OP_NLOAD_CREATE";
      case OP_LOCAL_CREATE:
        return "OP_LOCAL_CREATE";
      case OP_PUT:
        return "OP_PUT";
      case OP_SEARCH_PUT:
        return "OP_SEARCH_PUT";
      case OP_LLOAD_PUT:
        return "OP_LLOAD_PUT";
      case OP_NLOAD_PUT:
        return "OP_NLOAD_PUT";
      default:
        return "<unhandled op " + opCode + " >";
    }
  }

  /**
   * Returns an Operation instance that matches what the transactional operation done on this entry
   * in the cache the the transaction was performed in.
   */
  protected Operation getNearSideOperation() {
    switch (this.op) {
      case OP_NULL:
        return null;
      case OP_L_DESTROY:
        return Operation.LOCAL_DESTROY;
      case OP_CREATE_LD:
        return Operation.LOCAL_DESTROY;
      case OP_LLOAD_CREATE_LD:
        return Operation.LOCAL_DESTROY;
      case OP_NLOAD_CREATE_LD:
        return Operation.LOCAL_DESTROY;
      case OP_PUT_LD:
        return Operation.LOCAL_DESTROY;
      case OP_LLOAD_PUT_LD:
        return Operation.LOCAL_DESTROY;
      case OP_NLOAD_PUT_LD:
        return Operation.LOCAL_DESTROY;
      case OP_D_INVALIDATE_LD:
        return Operation.LOCAL_DESTROY;
      case OP_D_DESTROY:
        return getDestroyOperation();
      case OP_L_INVALIDATE:
        return Operation.LOCAL_INVALIDATE;
      case OP_PUT_LI:
        return Operation.LOCAL_INVALIDATE;
      case OP_LLOAD_PUT_LI:
        return Operation.LOCAL_INVALIDATE;
      case OP_NLOAD_PUT_LI:
        return Operation.LOCAL_INVALIDATE;
      case OP_D_INVALIDATE:
        return Operation.INVALIDATE;
      case OP_CREATE_LI:
        return getCreateOperation();
      case OP_LLOAD_CREATE_LI:
        return getCreateOperation();
      case OP_NLOAD_CREATE_LI:
        return getCreateOperation();
      case OP_CREATE:
        return getCreateOperation();
      case OP_SEARCH_CREATE:
        return Operation.SEARCH_CREATE;
      case OP_LLOAD_CREATE:
        return Operation.LOCAL_LOAD_CREATE;
      case OP_NLOAD_CREATE:
        return Operation.NET_LOAD_CREATE;
      case OP_LOCAL_CREATE:
        return getCreateOperation();
      case OP_PUT:
        return getUpdateOperation();
      case OP_SEARCH_PUT:
        return Operation.SEARCH_UPDATE;
      case OP_LLOAD_PUT:
        return Operation.LOCAL_LOAD_UPDATE;
      case OP_NLOAD_PUT:
        return Operation.NET_LOAD_CREATE;
      default:
        throw new IllegalStateException(
            LocalizedStrings.TXEntryState_UNHANDLED_OP_0.toLocalizedString(Byte.valueOf(this.op)));
    }
  }

  /**
   * @return true when the operation is the result of a bulk op
   */
  private boolean isBulkOp() {
    return this.bulkOp;
  }

  /**
   * Calculate and return the event offset based on the sequence id on TXState.
   *
   * @since GemFire 5.7
   */
  private static int generateEventOffset(TXState txState) {
    long seqId = EventID.reserveSequenceId();
    int offset = (int) (seqId - txState.getBaseSequenceId());
    return offset;
  }

  /**
   * Generate offsets for different eventIds; one for nearside and one for farside for the ops for
   * this entry.
   *
   * @since GemFire 5.7
   */
  private void generateBothEventOffsets(TXState txState) {
    assert this.farSideEventOffset == -1;
    this.farSideEventOffset = generateEventOffset(txState);
    generateNearSideEventOffset(txState);
  }

  /**
   * Generate the offset for an eventId that will be used for both a farside and nearside op for
   * this entry.
   *
   * @since GemFire 5.7
   */
  private void generateSharedEventOffset(TXState txState) {
    assert this.farSideEventOffset == -1;
    generateNearSideEventOffset(txState);
    this.farSideEventOffset = this.nearSideEventOffset;
  }

  /**
   * Generate the offset for an eventId that will be used for the nearside op for this entry. No
   * farside op will be done.
   *
   * @since GemFire 5.7
   */
  private void generateNearSideOnlyEventOffset(TXState txState) {
    generateNearSideEventOffset(txState);
  }

  private void generateNearSideEventOffset(TXState txState) {
    assert this.nearSideEventOffset == -1;
    this.nearSideEventOffset = generateEventOffset(txState);
  }

  private int getFarSideEventOffset() {
    assert this.nearSideEventOffset != -1;
    return this.nearSideEventOffset;
  }

  private static EventID createEventID(TXState txState, int offset) {
    return new EventID(txState.getBaseMembershipId(), txState.getBaseThreadId(),
        txState.getBaseSequenceId() + offset);
  }

  /**
   * Calculate (if farside has not already done so) and return then eventID to use for near side op
   * applications.
   *
   * @since GemFire 5.7
   */
  private EventID getNearSideEventId(TXState txState) {
    assert this.nearSideEventOffset != -1;
    return createEventID(txState, this.nearSideEventOffset);
  }

  /**
   * Calculate and return the event offset for this entry's farSide operation.
   *
   * @since GemFire 5.7
   */
  void generateEventOffsets(TXState txState) {
    switch (this.op) {
      case OP_NULL:
        // no eventIds needed
        break;
      case OP_L_DESTROY:
        generateNearSideOnlyEventOffset(txState);
        break;
      case OP_CREATE_LD:
        generateBothEventOffsets(txState);
        break;
      case OP_LLOAD_CREATE_LD:
        generateBothEventOffsets(txState);
        break;
      case OP_NLOAD_CREATE_LD:
        generateBothEventOffsets(txState);
        break;
      case OP_PUT_LD:
        generateBothEventOffsets(txState);
        break;
      case OP_LLOAD_PUT_LD:
        generateBothEventOffsets(txState);
        break;
      case OP_NLOAD_PUT_LD:
        generateBothEventOffsets(txState);
        break;
      case OP_D_INVALIDATE_LD:
        generateBothEventOffsets(txState);
        break;
      case OP_D_DESTROY:
        generateSharedEventOffset(txState);
        break;
      case OP_L_INVALIDATE:
        generateNearSideOnlyEventOffset(txState);
        break;
      case OP_PUT_LI:
        generateBothEventOffsets(txState);
        break;
      case OP_LLOAD_PUT_LI:
        generateBothEventOffsets(txState);
        break;
      case OP_NLOAD_PUT_LI:
        generateBothEventOffsets(txState);
        break;
      case OP_D_INVALIDATE:
        generateSharedEventOffset(txState);
        break;
      case OP_CREATE_LI:
        generateBothEventOffsets(txState);
        break;
      case OP_LLOAD_CREATE_LI:
        generateBothEventOffsets(txState);
        break;
      case OP_NLOAD_CREATE_LI:
        generateBothEventOffsets(txState);
        break;
      case OP_CREATE:
        generateSharedEventOffset(txState);
        break;
      case OP_SEARCH_CREATE:
        generateNearSideOnlyEventOffset(txState);
        break;
      case OP_LLOAD_CREATE:
        generateSharedEventOffset(txState);
        break;
      case OP_NLOAD_CREATE:
        generateSharedEventOffset(txState);
        break;
      case OP_LOCAL_CREATE:
        generateNearSideOnlyEventOffset(txState);
        break;
      case OP_PUT:
        generateSharedEventOffset(txState);
        break;
      case OP_SEARCH_PUT:
        generateNearSideOnlyEventOffset(txState);
        break;
      case OP_LLOAD_PUT:
        generateSharedEventOffset(txState);
        break;
      case OP_NLOAD_PUT:
        generateSharedEventOffset(txState);
        break;
      default:
        throw new IllegalStateException("<unhandled op " + this.op + " >");
    }
  }

  /**
   * Gets the operation code for the operation done on this entry in caches remote from the
   * originator of the tx (i.e. the "far side").
   *
   * @return null if no far side operation
   */
  private Operation getFarSideOperation() {
    switch (this.op) {
      case OP_NULL:
        return null;
      case OP_L_DESTROY:
        return null;
      case OP_CREATE_LD:
        return getCreateOperation();
      case OP_LLOAD_CREATE_LD:
        return Operation.LOCAL_LOAD_CREATE;
      case OP_NLOAD_CREATE_LD:
        return Operation.NET_LOAD_CREATE;
      case OP_PUT_LD:
        return getUpdateOperation();
      case OP_LLOAD_PUT_LD:
        return Operation.LOCAL_LOAD_UPDATE;
      case OP_NLOAD_PUT_LD:
        return Operation.NET_LOAD_UPDATE;
      case OP_D_INVALIDATE_LD:
        return Operation.INVALIDATE;
      case OP_D_DESTROY:
        return getDestroyOperation();
      case OP_L_INVALIDATE:
        return null;
      case OP_PUT_LI:
        return getUpdateOperation();
      case OP_LLOAD_PUT_LI:
        return Operation.LOCAL_LOAD_UPDATE;
      case OP_NLOAD_PUT_LI:
        return Operation.NET_LOAD_UPDATE;
      case OP_D_INVALIDATE:
        return Operation.INVALIDATE;
      case OP_CREATE_LI:
        return getCreateOperation();
      case OP_LLOAD_CREATE_LI:
        return Operation.LOCAL_LOAD_CREATE;
      case OP_NLOAD_CREATE_LI:
        return Operation.NET_LOAD_CREATE;
      case OP_CREATE:
        return getCreateOperation();
      case OP_SEARCH_CREATE:
        return null;
      case OP_LLOAD_CREATE:
        return Operation.LOCAL_LOAD_CREATE;
      case OP_NLOAD_CREATE:
        return Operation.NET_LOAD_CREATE;
      case OP_LOCAL_CREATE:
        return getCreateOperation();
      case OP_PUT:
        return getUpdateOperation();
      case OP_SEARCH_PUT:
        return null;
      case OP_LLOAD_PUT:
        return Operation.LOCAL_LOAD_UPDATE;
      case OP_NLOAD_PUT:
        return Operation.NET_LOAD_UPDATE;
      default:
        throw new IllegalStateException(
            LocalizedStrings.TXEntryState_UNHANDLED_OP_0.toLocalizedString(Byte.valueOf(this.op)));
    }
  }

  @Retained
  EntryEvent getEvent(InternalRegion r, Object key, TXState txs) {
    InternalRegion eventRegion = r;
    if (r.isUsedForPartitionedRegionBucket()) {
      eventRegion = r.getPartitionedRegion();
    }
    @Retained
    EntryEventImpl result = new TxEntryEventImpl(eventRegion, key);
    boolean returnedResult = false;
    try {
      if (this.destroy == DESTROY_NONE || isOpDestroy()) {
        result.setOldValue(getOriginalValue());
      }
      if (txs.isOriginRemoteForEvents()) {
        result.setOriginRemote(true);
      } else {
        result.setOriginRemote(false);
      }
      result.setTransactionId(txs.getTransactionId());
      returnedResult = true;
      return result;
    } finally {
      if (!returnedResult)
        result.release();
    }
  }

  /**
   * @return true if invalidate was done
   */
  public boolean invalidate(EntryEventImpl event) throws EntryNotFoundException {
    if (event.isLocalInvalid()) {
      performOp(adviseOp(OP_L_INVALIDATE, event), event);
    } else {
      performOp(adviseOp(OP_D_INVALIDATE, event), event);
    }

    return true;
  }

  /**
   * @return true if destroy was done
   */
  public boolean destroy(EntryEventImpl event, boolean cacheWrite, boolean originRemote)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {
    InternalRegion internalRegion = event.getRegion();
    CacheWriter cWriter = internalRegion.basicGetWriter();
    byte advisedOp = adviseOp((cacheWrite ? OP_D_DESTROY : OP_L_DESTROY), event);
    if (cWriter != null && cacheWrite && !event.inhibitAllNotifications()) {
      boolean oldOriginRemote = event.isOriginRemote();
      if (event.hasClientOrigin() || originRemote) {
        event.setOriginRemote(true);
      }
      cWriter.beforeDestroy(event);
      event.setOriginRemote(oldOriginRemote);
    }

    if (advisedOp != OP_NULL) {
      this.bulkOp = event.getOperation().isRemoveAll();
      if (cacheWrite) {
        performOp(advisedOp, event);
        this.destroy = DESTROY_DISTRIBUTED;
      } else {
        performOp(advisedOp, event);
        if (this.destroy != DESTROY_DISTRIBUTED) {
          this.destroy = DESTROY_LOCAL;
        }
      }
    }

    return true;
  }

  /**
   * @param event the event object for this operation, with the exception that the oldValue
   *        parameter is not yet filled in. The oldValue will be filled in by this operation.
   *
   * @param ifNew true if this operation must not overwrite an existing key
   * @param originRemote value for cacheWriter's isOriginRemote flag
   * @return true if put was done; otherwise returns false
   */
  public boolean basicPut(EntryEventImpl event, boolean ifNew, boolean originRemote)
      throws CacheWriterException, TimeoutException {
    byte putOp = OP_CREATE;
    boolean doingCreate = true;
    if (!ifNew) {
      if (existsLocally()) {
        putOp = OP_PUT;
        doingCreate = false;
      }
    }
    if (doingCreate) {
      event.makeCreate();
    } else {
      event.makeUpdate();
    }
    if (event.isNetSearch()) {
      putOp += (byte) (OP_SEARCH_PUT - OP_PUT);
    } else if (event.isLocalLoad()) {
      putOp += (byte) (OP_LLOAD_PUT - OP_PUT);
    } else if (event.isNetLoad()) {
      putOp += (byte) (OP_NLOAD_PUT - OP_PUT);
    }
    this.bulkOp = event.getOperation().isPutAll();
    byte advisedOp = adviseOp(putOp, event);

    InternalRegion internalRegion = event.getRegion();
    if (!event.isNetSearch()) {
      CacheWriter cWriter = internalRegion.basicGetWriter();
      boolean oldOriginRemote = event.isOriginRemote();
      if (event.hasClientOrigin() || originRemote) {
        event.setOriginRemote(true);
      }
      if (cWriter != null && event.isGenerateCallbacks() && !event.inhibitAllNotifications()) {
        if (doingCreate) {
          cWriter.beforeCreate(event);
        } else {
          cWriter.beforeUpdate(event);
        }
      }
      event.setOriginRemote(oldOriginRemote);
    }

    performOp(advisedOp, event);

    if (putOp == OP_CREATE) {
      fetchRemoteVersionTag(event);
    }
    return true;
  }

  /**
   * We will try to establish TXState on members with dataPolicy REPLICATE, this is done for the
   * first region to be involved in a transaction. For subsequent region if the dataPolicy is not
   * REPLICATE, we fetch the VersionTag from replicate members.
   */
  private void fetchRemoteVersionTag(EntryEventImpl event) {
    if (event.getRegion() instanceof DistributedRegion) {
      DistributedRegion dr = (DistributedRegion) event.getRegion();
      if (dr.getDataPolicy() == DataPolicy.NORMAL || dr.getDataPolicy() == DataPolicy.PRELOADED) {
        VersionTag tag = null;
        try {
          tag = dr.fetchRemoteVersionTag(event.getKey());
        } catch (EntryNotFoundException e) {
          // ignore
        }
        if (tag != null) {
          setRemoteVersionTag(tag);
        }
      }
    }
  }

  /**
   * Perform operation algebra
   *
   * @return false if operation was not done
   */
  private byte adviseOp(byte requestedOpCode, EntryEventImpl event) {
    { // Set event old value
      Object oldVal;
      if (this.op == OP_NULL) {
        oldVal = getOriginalValue();
      } else {
        oldVal = getNearSidePendingValue();
      }

      Region region = event.getRegion();
      boolean needOldValue = region instanceof HARegion // fix for bug 37909
          || region instanceof BucketRegion;
      event.setTXEntryOldValue(oldVal, needOldValue);
    }

    byte advisedOpCode = OP_NULL;
    // Determine new operation based on
    // the requested operation 'requestedOpCode' and
    // the previous operation 'this.op'
    switch (requestedOpCode) {
      case OP_L_DESTROY:
        switch (this.op) {
          case OP_NULL:
            advisedOpCode = requestedOpCode;
            break;
          case OP_L_DESTROY:
          case OP_CREATE_LD:
          case OP_LLOAD_CREATE_LD:
          case OP_NLOAD_CREATE_LD:
          case OP_PUT_LD:
          case OP_LLOAD_PUT_LD:
          case OP_NLOAD_PUT_LD:
          case OP_D_INVALIDATE_LD:
          case OP_D_DESTROY:
            throw new IllegalStateException(
                LocalizedStrings.TXEntryState_UNEXPECTED_CURRENT_OP_0_FOR_REQUESTED_OP_1
                    .toLocalizedString(new Object[] {opToString(), opToString(requestedOpCode)}));
          case OP_L_INVALIDATE:
            advisedOpCode = requestedOpCode;
            break;
          case OP_PUT_LI:
            advisedOpCode = OP_PUT_LD;
            break;
          case OP_LLOAD_PUT_LI:
            advisedOpCode = OP_LLOAD_PUT_LD;
            break;
          case OP_NLOAD_PUT_LI:
            advisedOpCode = OP_NLOAD_PUT_LD;
            break;
          case OP_D_INVALIDATE:
            advisedOpCode = OP_D_INVALIDATE_LD;
            break;
          case OP_CREATE_LI:
            advisedOpCode = OP_CREATE_LD;
            break;
          case OP_LLOAD_CREATE_LI:
            advisedOpCode = OP_LLOAD_CREATE_LD;
            break;
          case OP_NLOAD_CREATE_LI:
            advisedOpCode = OP_NLOAD_CREATE_LD;
            break;
          case OP_CREATE:
            advisedOpCode = OP_CREATE_LD;
            break;
          case OP_SEARCH_CREATE:
            advisedOpCode = requestedOpCode;
            break;
          case OP_LLOAD_CREATE:
            advisedOpCode = OP_LLOAD_CREATE_LD;
            break;
          case OP_NLOAD_CREATE:
            advisedOpCode = OP_NLOAD_CREATE_LD;
            break;
          case OP_LOCAL_CREATE:
            advisedOpCode = requestedOpCode;
            break;
          case OP_PUT:
            advisedOpCode = OP_PUT_LD;
            break;
          case OP_SEARCH_PUT:
            advisedOpCode = requestedOpCode;
            break;
          case OP_LLOAD_PUT:
            advisedOpCode = OP_LLOAD_PUT_LD;
            break;
          case OP_NLOAD_PUT:
            advisedOpCode = OP_NLOAD_PUT_LD;
            break;
          default:
            throw new IllegalStateException(
                LocalizedStrings.TXEntryState_UNHANDLED_0.toLocalizedString(opToString()));
        }
        break;
      case OP_D_DESTROY:
        Assert.assertTrue(!isOpDestroy(), "Transactional destroy assertion op=" + this.op);
        advisedOpCode = requestedOpCode;
        break;
      case OP_L_INVALIDATE:
        switch (this.op) {
          case OP_NULL:
            advisedOpCode = requestedOpCode;
            break;
          case OP_L_DESTROY:
          case OP_CREATE_LD:
          case OP_LLOAD_CREATE_LD:
          case OP_NLOAD_CREATE_LD:
          case OP_PUT_LD:
          case OP_LLOAD_PUT_LD:
          case OP_NLOAD_PUT_LD:
          case OP_D_DESTROY:
          case OP_D_INVALIDATE_LD:
            throw new IllegalStateException(
                LocalizedStrings.TXEntryState_UNEXPECTED_CURRENT_OP_0_FOR_REQUESTED_OP_1
                    .toLocalizedString(new Object[] {opToString(), opToString(requestedOpCode)}));
          case OP_L_INVALIDATE:
            advisedOpCode = requestedOpCode;
            break;
          case OP_LLOAD_PUT_LI:
          case OP_NLOAD_PUT_LI:
          case OP_LLOAD_CREATE_LI:
          case OP_NLOAD_CREATE_LI:
            advisedOpCode = this.op;
            break;
          case OP_PUT_LI:
            advisedOpCode = OP_PUT_LI;
            break;
          case OP_CREATE_LI:
            advisedOpCode = OP_CREATE_LI;
            break;
          case OP_D_INVALIDATE:
            advisedOpCode = OP_D_INVALIDATE;
            break;
          case OP_CREATE:
            advisedOpCode = OP_CREATE_LI;
            break;
          case OP_SEARCH_CREATE:
            advisedOpCode = OP_LOCAL_CREATE;
            // pendingValue will be set to LOCAL_INVALID
            break;
          case OP_LLOAD_CREATE:
            advisedOpCode = OP_LLOAD_CREATE_LI;
            break;
          case OP_NLOAD_CREATE:
            advisedOpCode = OP_NLOAD_CREATE_LI;
            break;
          case OP_LOCAL_CREATE:
            advisedOpCode = OP_LOCAL_CREATE;
            break;
          case OP_PUT:
            advisedOpCode = OP_PUT_LI;
            break;
          case OP_SEARCH_PUT:
            advisedOpCode = requestedOpCode;
            break;
          case OP_LLOAD_PUT:
            advisedOpCode = OP_LLOAD_PUT_LI;
            break;
          case OP_NLOAD_PUT:
            advisedOpCode = OP_NLOAD_PUT_LI;
            break;
          default:
            throw new IllegalStateException(
                LocalizedStrings.TXEntryState_UNHANDLED_0.toLocalizedString(opToString()));
        }
        break;
      case OP_D_INVALIDATE:
        switch (this.op) {
          case OP_NULL:
            advisedOpCode = requestedOpCode;
            break;
          case OP_L_DESTROY:
          case OP_CREATE_LD:
          case OP_LLOAD_CREATE_LD:
          case OP_NLOAD_CREATE_LD:
          case OP_PUT_LD:
          case OP_LLOAD_PUT_LD:
          case OP_NLOAD_PUT_LD:
          case OP_D_INVALIDATE_LD:
          case OP_D_DESTROY:
            throw new IllegalStateException(
                LocalizedStrings.TXEntryState_UNEXPECTED_CURRENT_OP_0_FOR_REQUESTED_OP_1
                    .toLocalizedString(new Object[] {opToString(), opToString(requestedOpCode)}));
          case OP_D_INVALIDATE:
          case OP_L_INVALIDATE:
            advisedOpCode = OP_D_INVALIDATE;
            break;

          case OP_PUT_LI:
          case OP_LLOAD_PUT_LI:
          case OP_NLOAD_PUT_LI:
          case OP_CREATE_LI:
          case OP_LLOAD_CREATE_LI:
          case OP_NLOAD_CREATE_LI:
            /*
             * No change, keep it how it was.
             */
            advisedOpCode = this.op;
            break;
          case OP_CREATE:
            advisedOpCode = OP_CREATE;
            // pendingValue will be set to INVALID turning it into create invalid
            break;
          case OP_SEARCH_CREATE:
            advisedOpCode = OP_LOCAL_CREATE;
            // pendingValue will be set to INVALID to indicate dinvalidate
            break;
          case OP_LLOAD_CREATE:
            advisedOpCode = OP_CREATE;
            // pendingValue will be set to INVALID turning it into create invalid
            break;
          case OP_NLOAD_CREATE:
            advisedOpCode = OP_CREATE;
            // pendingValue will be set to INVALID turning it into create invalid
            break;
          case OP_LOCAL_CREATE:
            advisedOpCode = OP_LOCAL_CREATE;
            // pendingValue will be set to INVALID to indicate dinvalidate
            break;
          case OP_PUT:
          case OP_SEARCH_PUT:
          case OP_LLOAD_PUT:
          case OP_NLOAD_PUT:
            advisedOpCode = requestedOpCode;
            break;
          default:
            throw new IllegalStateException(
                LocalizedStrings.TXEntryState_UNHANDLED_0.toLocalizedString(opToString()));
        }
        break;
      case OP_CREATE:
      case OP_SEARCH_CREATE:
      case OP_LLOAD_CREATE:
      case OP_NLOAD_CREATE:
        advisedOpCode = requestedOpCode;
        break;
      case OP_PUT:
        switch (this.op) {
          case OP_CREATE:
          case OP_SEARCH_CREATE:
          case OP_LLOAD_CREATE:
          case OP_NLOAD_CREATE:
          case OP_LOCAL_CREATE:
          case OP_CREATE_LI:
          case OP_LLOAD_CREATE_LI:
          case OP_NLOAD_CREATE_LI:
          case OP_CREATE_LD:
          case OP_LLOAD_CREATE_LD:
          case OP_NLOAD_CREATE_LD:
          case OP_PUT_LD:
          case OP_LLOAD_PUT_LD:
          case OP_NLOAD_PUT_LD:
          case OP_D_INVALIDATE_LD:
          case OP_L_DESTROY:
          case OP_D_DESTROY:
            advisedOpCode = OP_CREATE;
            break;
          default:
            advisedOpCode = requestedOpCode;
            break;
        }
        break;
      case OP_SEARCH_PUT:
        switch (this.op) {
          case OP_NULL:
            advisedOpCode = requestedOpCode;
            break;
          case OP_L_INVALIDATE:
            advisedOpCode = requestedOpCode;
            break;
          // The incoming search put value should match
          // the pendingValue from the previous tx operation.
          // So it is ok to simply drop the _LI from the op
          case OP_PUT_LI:
            advisedOpCode = OP_PUT;
            break;
          case OP_LLOAD_PUT_LI:
            advisedOpCode = OP_LLOAD_PUT;
            break;
          case OP_NLOAD_PUT_LI:
            advisedOpCode = OP_NLOAD_PUT;
            break;
          case OP_CREATE_LI:
            advisedOpCode = OP_CREATE;
            break;
          case OP_LLOAD_CREATE_LI:
            advisedOpCode = OP_LLOAD_CREATE;
            break;
          case OP_NLOAD_CREATE_LI:
            advisedOpCode = OP_NLOAD_CREATE;
            break;
          default:
            // Note that OP_LOCAL_CREATE and OP_CREATE with invalid values
            // are not possible because they would cause the netsearch to
            // fail and we would do a load or a total miss.
            // Note that OP_D_INVALIDATE followed by OP_SEARCH_PUT is not
            // possible since the netsearch will alwsys "miss" in this case.
            throw new IllegalStateException(
                LocalizedStrings.TXEntryState_PREVIOUS_OP_0_UNEXPECTED_FOR_REQUESTED_OP_1
                    .toLocalizedString(new Object[] {opToString(), opToString(requestedOpCode)}));
        }
        break;
      case OP_LLOAD_PUT:
      case OP_NLOAD_PUT:
        switch (this.op) {
          case OP_NULL:
          case OP_L_INVALIDATE:
          case OP_PUT_LI:
          case OP_LLOAD_PUT_LI:
          case OP_NLOAD_PUT_LI:
          case OP_D_INVALIDATE:
            advisedOpCode = requestedOpCode;
            break;
          case OP_CREATE:
          case OP_LOCAL_CREATE:
          case OP_CREATE_LI:
          case OP_LLOAD_CREATE_LI:
          case OP_NLOAD_CREATE_LI:
            if (requestedOpCode == OP_LLOAD_PUT) {
              advisedOpCode = OP_LLOAD_CREATE;
            } else {
              advisedOpCode = OP_NLOAD_CREATE;
            }
            break;
          default:
            // note that other invalid states are covered by this default
            // case because they should have caused a OP_SEARCH_PUT
            // to be requested.
            throw new IllegalStateException(
                LocalizedStrings.TXEntryState_PREVIOUS_OP_0_UNEXPECTED_FOR_REQUESTED_OP_1
                    .toLocalizedString(new Object[] {opToString(), opToString(requestedOpCode)}));
        }
        break;
      default:
        throw new IllegalStateException(
            LocalizedStrings.TXEntryState_OPCODE_0_SHOULD_NEVER_BE_REQUESTED
                .toLocalizedString(opToString(requestedOpCode)));
    }
    return advisedOpCode;
  }

  private void performOp(byte advisedOpCode, EntryEventImpl event) {
    this.op = advisedOpCode;
    event.putValueTXEntry(this);
  }

  private boolean areIdentical(Object o1, Object o2) {
    if (o1 == o2)
      return true;
    if (o1 instanceof StoredObject) {
      if (o1.equals(o2))
        return true;
    }
    return false;
  }

  void checkForConflict(InternalRegion r, Object key) throws CommitConflictException {
    if (!isDirty()) {
      // All we did was read the entry and we don't do read/write conflicts; yet.
      return;
    }
    if (isOpSearch()) {
      // netsearch never cause conflicts
      // Note that we don't care, in this case, if region still exists.
      // Search should never cause a conflict
      return;
    }
    try {
      r.checkReadiness();
      RegionEntry re = r.basicGetEntry(key);
      Object curCmtVersionId = null;
      try {
        if ((re == null) || re.isValueNull()) {
          curCmtVersionId = Token.REMOVED_PHASE1;
        } else {
          if (re.getValueWasResultOfSearch()) {
            return;
          }

          /*
           * The originalVersionId may be compressed so grab the value as stored in the map which
           * will match if compression is turned on.
           */
          curCmtVersionId = re.getTransformedValue();
        }
        if (!areIdentical(getOriginalVersionId(), curCmtVersionId)) {
          // I'm not sure it is a good idea to call getDeserializedValue.
          // Might be better to add a toString impl on CachedDeserializable.
          // if (curCmtVersionId instanceof CachedDeserializable) {
          // curCmtVersionId =
          // ((CachedDeserializable)curCmtVersionId).getDeserializedValue();
          // }
          if (VERBOSE_CONFLICT_STRING || logger.isDebugEnabled()) {
            String fromString = calcConflictString(getOriginalVersionId());
            String toString = calcConflictString(curCmtVersionId);
            if (!fromString.equals(toString)) {
              throw new CommitConflictException(
                  LocalizedStrings.TXEntryState_ENTRY_FOR_KEY_0_ON_REGION_1_HAD_ALREADY_BEEN_CHANGED_FROM_2_TO_3
                      .toLocalizedString(
                          new Object[] {key, r.getDisplayName(), fromString, toString}));
            }
          }
          throw new CommitConflictException(
              LocalizedStrings.TXEntryState_ENTRY_FOR_KEY_0_ON_REGION_1_HAD_A_STATE_CHANGE
                  .toLocalizedString(new Object[] {key, r.getDisplayName()}));
        }
      } finally {
        OffHeapHelper.release(curCmtVersionId);
      }
    } catch (CacheRuntimeException ex) {
      r.getCancelCriterion().checkCancelInProgress(null);
      throw new CommitConflictException(
          LocalizedStrings.TXEntryState_CONFLICT_CAUSED_BY_CACHE_EXCEPTION.toLocalizedString(), ex);
    }
  }

  private String calcConflictString(Object obj) {
    Object o = obj;
    if (o instanceof CachedDeserializable) {
      if (o instanceof StoredObject && ((StoredObject) o).isCompressed()) {
        // fix for bug 52113
        return "<compressed value of size " + ((StoredObject) o).getValueSizeInBytes() + ">";
      }
      try {
        o = ((CachedDeserializable) o).getDeserializedForReading();
      } catch (PdxSerializationException e) {
        o = "<value unavailable>";
      }
    }
    if (o == null) {
      return "<null>";
    } else if (Token.isRemoved(o)) {
      return "<null>";
    } else if (o == Token.INVALID) {
      return "<invalidated>";
    } else if (o == Token.LOCAL_INVALID) {
      return "<local invalidated>";
    }
    return "<" + StringUtils.forceToString(o) + ">";
  }

  private boolean didDestroy() {
    return this.destroy != DESTROY_NONE;
  }

  private boolean didDistributedDestroy() {
    return this.destroy == DESTROY_DISTRIBUTED;
  }

  /**
   * Returns the total number of modifications made by this transaction to this entry. The result
   * will be +1 for a create and -1 for a destroy.
   */
  int entryCountMod() {
    switch (this.op) {
      case OP_L_DESTROY:
      case OP_CREATE_LD:
      case OP_LLOAD_CREATE_LD:
      case OP_NLOAD_CREATE_LD:
      case OP_PUT_LD:
      case OP_LLOAD_PUT_LD:
      case OP_NLOAD_PUT_LD:
      case OP_D_INVALIDATE_LD:
      case OP_D_DESTROY:
        if (getOriginalValue() == null || Token.isRemoved(getOriginalValue())) {
          return 0;
        } else {
          return -1;
        }
      case OP_CREATE_LI:
      case OP_LLOAD_CREATE_LI:
      case OP_NLOAD_CREATE_LI:
      case OP_CREATE:
      case OP_SEARCH_CREATE:
      case OP_LLOAD_CREATE:
      case OP_NLOAD_CREATE:
      case OP_LOCAL_CREATE:
        if (getOriginalValue() == null || Token.isRemoved(getOriginalValue())) {
          return 1;
        } else {
          return 0;
        }
      case OP_NULL:
      case OP_L_INVALIDATE:
      case OP_PUT_LI:
      case OP_LLOAD_PUT_LI:
      case OP_NLOAD_PUT_LI:
      case OP_D_INVALIDATE:
      case OP_PUT:
      case OP_SEARCH_PUT:
      case OP_LLOAD_PUT:
      case OP_NLOAD_PUT:
      default:
        return 0;
    }
  }

  /**
   * Returns true if this entry may have been created by this transaction.
   */
  boolean wasCreatedByTX() {
    return isOpCreate();
  }

  private void txApplyDestroyLocally(InternalRegion r, Object key, TXState txState) {
    try {
      r.txApplyDestroy(key, txState.getTransactionId(), null, false/* inTokenMode */,
          getDestroyOperation(), getNearSideEventId(txState), callBackArgument,
          txState.getPendingCallbacks(),
          getFilterRoutingInfo(), txState.bridgeContext, false, this, null, -1);
    } catch (RegionDestroyedException ignore) {
    } catch (EntryDestroyedException ignore) {
    }
  }

  private void txApplyInvalidateLocally(InternalRegion r, Object key, Object newValue,
      boolean didDestroy, TXState txState) {
    try {
      r.txApplyInvalidate(key, newValue, didDestroy, txState.getTransactionId(), null,
          isOpLocalInvalidate() ? true : false, getNearSideEventId(txState), callBackArgument,
          txState.getPendingCallbacks(), getFilterRoutingInfo(), txState.bridgeContext, this, null,
          -1);
    } catch (RegionDestroyedException ignore) {
    } catch (EntryDestroyedException ignore) {
    }
  }

  private void txApplyPutLocally(InternalRegion r, Operation putOp, Object key, Object newValue,
      boolean didDestroy, TXState txState) {
    try {
      r.txApplyPut(putOp, key, newValue, didDestroy, txState.getTransactionId(), null,
          getNearSideEventId(txState), callBackArgument, txState.getPendingCallbacks(),
          getFilterRoutingInfo(), txState.bridgeContext, this, null, -1);
    } catch (RegionDestroyedException ignore) {
    } catch (EntryDestroyedException ignore) {
    }
  }

  /**
   * If the entry is not dirty (read only) then clean it up.
   *
   * @return true if this entry is not dirty.
   */
  boolean cleanupNonDirty(InternalRegion r) {
    if (isDirty()) {
      return false;
    } else {
      cleanup(r);
      return true;
    }
  }

  void buildMessage(InternalRegion r, Object key, TXCommitMessage msg, Set otherRecipients) {
    if (!isDirty()) {
      // all we do was read so just return
      return;
    }
    switch (this.op) {
      case OP_NULL:
      case OP_L_DESTROY:
      case OP_L_INVALIDATE:
      case OP_SEARCH_CREATE:
      case OP_SEARCH_PUT:
        // local only
        break;

      case OP_CREATE_LD:
      case OP_LLOAD_CREATE_LD:
      case OP_NLOAD_CREATE_LD:
      case OP_PUT_LD:
      case OP_LLOAD_PUT_LD:
      case OP_NLOAD_PUT_LD:
      case OP_D_INVALIDATE_LD:
      case OP_D_DESTROY:
      case OP_PUT_LI:
      case OP_LLOAD_PUT_LI:
      case OP_NLOAD_PUT_LI:
      case OP_D_INVALIDATE:
      case OP_CREATE_LI:
      case OP_LLOAD_CREATE_LI:
      case OP_NLOAD_CREATE_LI:
      case OP_CREATE:
      case OP_LLOAD_CREATE:
      case OP_NLOAD_CREATE:
      case OP_LOCAL_CREATE:
      case OP_PUT:
      case OP_LLOAD_PUT:
      case OP_NLOAD_PUT:
        msg.addOp(r, key, this, otherRecipients);
        break;

      default:
        throw new IllegalStateException("Unhandled op=" + opToString());
    }
  }


  void buildCompleteMessage(InternalRegion r, Object key, TXCommitMessage msg,
      Set otherRecipients) {
    if (!isDirty()) {
      // all we do was read so just return
      return;
    }
    switch (this.op) {
      case OP_NULL:
      case OP_L_DESTROY:
      case OP_L_INVALIDATE:
      case OP_SEARCH_CREATE:
      case OP_SEARCH_PUT:
        // local only
        break;

      case OP_CREATE_LD:
      case OP_LLOAD_CREATE_LD:
      case OP_NLOAD_CREATE_LD:
      case OP_PUT_LD:
      case OP_LLOAD_PUT_LD:
      case OP_NLOAD_PUT_LD:
      case OP_D_INVALIDATE_LD:
      case OP_D_DESTROY:
      case OP_PUT_LI:
      case OP_LLOAD_PUT_LI:
      case OP_NLOAD_PUT_LI:
      case OP_D_INVALIDATE:
      case OP_CREATE_LI:
      case OP_LLOAD_CREATE_LI:
      case OP_NLOAD_CREATE_LI:
      case OP_CREATE:
      case OP_LLOAD_CREATE:
      case OP_NLOAD_CREATE:
      case OP_LOCAL_CREATE:
      case OP_PUT:
      case OP_LLOAD_PUT:
      case OP_NLOAD_PUT:
        msg.addOp(r, key, this, otherRecipients);
        break;

      default:
        throw new IllegalStateException("Unhandled op=" + opToString());
    }
  }



  void applyChanges(InternalRegion r, Object key, TXState txState) {
    if (logger.isDebugEnabled()) {
      logger.debug("applyChanges txState=" + txState + " ,key=" + key + " ,r=" + r.getDisplayName()
          + " ,op=" + this.op + " ,isDirty=" + isDirty());
    }
    if (!isDirty()) {
      // all we did was read so just return
      return;
    }
    switch (this.op) {
      case OP_NULL:
        // do nothing
        break;
      case OP_L_DESTROY:
        txApplyDestroyLocally(r, key, txState);
        break;
      case OP_CREATE_LD:
        txApplyDestroyLocally(r, key, txState);
        break;
      case OP_LLOAD_CREATE_LD:
        txApplyDestroyLocally(r, key, txState);
        break;
      case OP_NLOAD_CREATE_LD:
        txApplyDestroyLocally(r, key, txState);
        break;
      case OP_PUT_LD:
        txApplyDestroyLocally(r, key, txState);
        break;
      case OP_LLOAD_PUT_LD:
        txApplyDestroyLocally(r, key, txState);
        break;
      case OP_NLOAD_PUT_LD:
        txApplyDestroyLocally(r, key, txState);
        break;
      case OP_D_INVALIDATE_LD:
        txApplyDestroyLocally(r, key, txState);
        break;
      case OP_D_DESTROY:
        txApplyDestroyLocally(r, key, txState);
        break;
      case OP_L_INVALIDATE:
        txApplyInvalidateLocally(r, key, Token.LOCAL_INVALID, didDestroy(), txState);
        break;
      case OP_PUT_LI:
        txApplyPutLocally(r, getUpdateOperation(), key, Token.LOCAL_INVALID, didDestroy(), txState);
        break;
      case OP_LLOAD_PUT_LI:
        txApplyPutLocally(r, getUpdateOperation(), key, Token.LOCAL_INVALID, didDestroy(), txState);
        break;
      case OP_NLOAD_PUT_LI:
        txApplyPutLocally(r, getUpdateOperation(), key, Token.LOCAL_INVALID, didDestroy(), txState);
        break;
      case OP_D_INVALIDATE:
        txApplyInvalidateLocally(r, key, Token.INVALID, didDestroy(), txState);
        break;
      case OP_CREATE_LI:
        txApplyPutLocally(r, getCreateOperation(), key, Token.LOCAL_INVALID, didDestroy(), txState);
        break;
      case OP_LLOAD_CREATE_LI:
        txApplyPutLocally(r, getCreateOperation(), key, Token.LOCAL_INVALID, didDestroy(), txState);
        break;
      case OP_NLOAD_CREATE_LI:
        txApplyPutLocally(r, getCreateOperation(), key, Token.LOCAL_INVALID, didDestroy(), txState);
        break;
      case OP_CREATE:
        txApplyPutLocally(r, getCreateOperation(), key, getPendingValue(), didDestroy(), txState);
        break;
      case OP_SEARCH_CREATE:
        txApplyPutLocally(r, Operation.SEARCH_CREATE, key, getPendingValue(), didDestroy(),
            txState);
        break;
      case OP_LLOAD_CREATE:
        txApplyPutLocally(r, Operation.LOCAL_LOAD_CREATE, key, getPendingValue(), didDestroy(),
            txState);
        break;
      case OP_NLOAD_CREATE:
        txApplyPutLocally(r, Operation.NET_LOAD_CREATE, key, getPendingValue(), didDestroy(),
            txState);
        break;
      case OP_LOCAL_CREATE:
        txApplyPutLocally(r, getCreateOperation(), key, getPendingValue(), didDestroy(), txState);
        break;
      case OP_PUT:
        txApplyPutLocally(r, getUpdateOperation(), key, getPendingValue(), didDestroy(), txState);
        break;
      case OP_SEARCH_PUT:
        txApplyPutLocally(r, Operation.SEARCH_UPDATE, key, getPendingValue(), didDestroy(),
            txState);
        break;
      case OP_LLOAD_PUT:
        txApplyPutLocally(r, Operation.LOCAL_LOAD_UPDATE, key, getPendingValue(), didDestroy(),
            txState);
        break;
      case OP_NLOAD_PUT:
        txApplyPutLocally(r, Operation.NET_LOAD_UPDATE, key, getPendingValue(), didDestroy(),
            txState);
        break;
      default:
        throw new IllegalStateException(
            LocalizedStrings.TXEntryState_UNHANDLED_OP_0.toLocalizedString(opToString()));
    }
  }


  /**
   * @return returns {@link Operation#PUTALL_CREATE} if the operation is a result of bulk op,
   *         {@link Operation#CREATE} otherwise
   */
  private Operation getCreateOperation() {
    return isBulkOp() ? Operation.PUTALL_CREATE : Operation.CREATE;
  }

  /**
   * @return returns {@link Operation#PUTALL_UPDATE} if the operation is a result of bulk op,
   *         {@link Operation#UPDATE} otherwise
   */
  private Operation getUpdateOperation() {
    return isBulkOp() ? Operation.PUTALL_UPDATE : Operation.UPDATE;
  }

  @Override
  @Released(TX_ENTRY_STATE)
  public void release() {
    Object tmp = this.originalVersionId;
    if (OffHeapHelper.release(tmp)) {
      this.originalVersionId = null; // fix for bug 47900
    }
  }

  private Operation getDestroyOperation() {
    if (isOpLocalDestroy()) {
      return Operation.LOCAL_DESTROY;
    } else if (isBulkOp()) {
      return Operation.REMOVEALL_DESTROY;
    } else {
      return Operation.DESTROY;
    }
  }

  /**
   * Serializes this entry state to a data output stream for a far side consumer. Make sure this
   * method is backwards compatible if changes are made.
   *
   * <p>
   * The fromData for this is TXCommitMessage$RegionCommit$FarSideEntryOp#fromData.
   *
   * @param largeModCount true if modCount needs to be represented by an int; false if a byte is
   *        enough
   * @param sendVersionTag true if versionTag should be sent to clients 7.0 and above
   * @param sendShadowKey true if wan shadowKey should be sent to peers 7.0.1 and above
   *
   * @since GemFire 5.0
   */
  void toFarSideData(DataOutput out, boolean largeModCount, boolean sendVersionTag,
      boolean sendShadowKey) throws IOException {
    Operation operation = getFarSideOperation();
    out.writeByte(operation.ordinal);
    if (largeModCount) {
      out.writeInt(this.modSerialNum);
    } else {
      out.writeByte(this.modSerialNum);
    }
    DataSerializer.writeObject(getCallbackArgument(), out);
    DataSerializer.writeObject(getFilterRoutingInfo(), out);
    if (sendVersionTag) {
      DataSerializer.writeObject(getVersionTag(), out);
      assert getVersionTag() != null || !txRegionState.getRegion().getConcurrencyChecksEnabled()
          || txRegionState.getRegion().getDataPolicy() != DataPolicy.REPLICATE : "tag:"
              + getVersionTag() + " r:" + txRegionState.getRegion() + " op:" + opToString()
              + " key:";
    }
    if (sendShadowKey) {
      out.writeLong(this.tailKey);
    }
    out.writeInt(getFarSideEventOffset());
    if (!operation.isDestroy()) {
      out.writeBoolean(didDistributedDestroy());
      if (!operation.isInvalidate()) {
        boolean isTokenOrByteArray = Token.isInvalidOrRemoved(getPendingValue());
        isTokenOrByteArray = isTokenOrByteArray || getPendingValue() instanceof byte[];
        out.writeBoolean(isTokenOrByteArray);
        if (isTokenOrByteArray) {
          // this is a token or byte[] only
          DataSerializer.writeObject(getPendingValue(), out);
        } else {
          // this is a CachedDeserializable, Object and PDX
          DataSerializer.writeByteArray(getSerializedPendingValue(), out);
        }
      }
    }
  }

  public FilterRoutingInfo getFilterRoutingInfo() {
    return filterRoutingInfo;
  }

  void cleanup(InternalRegion r) {
    if (this.refCountEntry != null) {
      r.txDecRefCount(refCountEntry);
    }
    close();
  }

  /**
   * Returns the sort key for this entry.
   */
  int getSortValue() {
    return this.modSerialNum;
  }

  /**
   * Just like an EntryEventImpl but also has access to TxEntryState to make it Comparable
   *
   * @since GemFire 5.0
   */
  public class TxEntryEventImpl extends EntryEventImpl implements Comparable {
    /**
     * Creates a local tx entry event
     */
    @Retained
    TxEntryEventImpl(InternalRegion r, Object key) {
      super(r, getNearSideOperation(), key, getNearSidePendingValue(),
          TXEntryState.this.getCallbackArgument(), false, r.getMyId(), true/* generateCallbacks */,
          true /* initializeId */);
    }

    /**
     * Returns the value to use to sort us
     */
    private int getSortValue() {
      return TXEntryState.this.getSortValue();
    }

    public int compareTo(Object o) {
      TxEntryEventImpl other = (TxEntryEventImpl) o;
      return getSortValue() - other.getSortValue();
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || !(o instanceof TxEntryEventImpl))
        return false;
      return compareTo(o) == 0;
    }

    @Override
    public int hashCode() {
      return getSortValue();
    }
  }

  private static final TXEntryStateFactory factory = new TXEntryStateFactory() {

    public TXEntryState createEntry() {
      return new TXEntryState();
    }

    public TXEntryState createEntry(RegionEntry re, Object vId, Object pendingValue,
        Object entryKey, TXRegionState txrs, boolean isDistributed) {
      return new TXEntryState(re, vId, pendingValue, txrs, isDistributed);
    }

  };

  public static TXEntryStateFactory getFactory() {
    return factory;
  }

  public void setFilterRoutingInfo(FilterRoutingInfo fri) {
    this.filterRoutingInfo = fri;
  }

  public Set<InternalDistributedMember> getAdjunctRecipients() {
    return adjunctRecipients;
  }

  public void setAdjunctRecipients(Set<InternalDistributedMember> members) {
    this.adjunctRecipients = members;
  }

  public VersionTag getVersionTag() {
    return versionTag;
  }

  public void setVersionTag(VersionTag versionTag) {
    this.versionTag = versionTag;
  }

  public VersionTag getRemoteVersionTag() {
    return remoteVersionTag;
  }

  public void setRemoteVersionTag(VersionTag remoteVersionTag) {
    this.remoteVersionTag = remoteVersionTag;
  }

  public long getTailKey() {
    return tailKey;
  }

  public void setTailKey(long tailKey) {
    this.tailKey = tailKey;
  }

  public void close() {
    release();
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("{").append(super.toString()).append(" ");
    str.append(this.op);
    str.append("}");
    return str.toString();
  }

  public DistTxThinEntryState getDistTxEntryStates() {
    return this.distTxThinEntryState;
  }

  public void setDistTxEntryStates(DistTxThinEntryState thinEntryState) {
    this.distTxThinEntryState = thinEntryState;
  }

  /**
   * For Distributed Transaction Usage
   * <p>
   * This class is used to bring relevant information for DistTxEntryEvent from primary, after end
   * of precommit. Same information are sent to all replicates during commit.
   * <p>
   * Whereas see DistTxEntryEvent is used for storing entry event information on TxCoordinator and
   * carry same to replicates.
   */
  public static class DistTxThinEntryState implements DataSerializableFixedID {

    private long regionVersion = 1L;
    private long tailKey = -1L;
    private String memberID;

    // For Serialization
    public DistTxThinEntryState() {}

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }

    @Override
    public int getDSFID() {
      return DIST_TX_THIN_ENTRY_STATE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeLong(this.regionVersion, out);
      DataSerializer.writeLong(this.tailKey, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.regionVersion = DataSerializer.readLong(in);
      this.tailKey = DataSerializer.readLong(in);
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append("DistTxThinEntryState: ");
      buf.append(" ,regionVersion=" + this.regionVersion);
      buf.append(" ,tailKey=" + this.tailKey);
      buf.append(" ,memberID=" + this.memberID);
      return buf.toString();
    }

    public long getRegionVersion() {
      return this.regionVersion;
    }

    public void setRegionVersion(long regionVersion) {
      this.regionVersion = regionVersion;
    }

    public long getTailKey() {
      return this.tailKey;
    }

    public void setTailKey(long tailKey) {
      this.tailKey = tailKey;
    }

    public String getMemberID() {
      return memberID;
    }

    public void setMemberID(String memberID) {
      this.memberID = memberID;
    }
  }
}
