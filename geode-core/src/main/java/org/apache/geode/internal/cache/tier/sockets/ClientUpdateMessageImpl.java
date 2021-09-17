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
package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireIOException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Sendable;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.EntryEventImpl.NewValueImporter;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.WrappedCallbackArgument;
import org.apache.geode.internal.cache.ha.HAContainerRegion;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Class {@code ClientUpdateMessageImpl} is a message representing a cache operation that is
 * sent from a server to an interested client.
 *
 * @since GemFire 4.2
 */
public class ClientUpdateMessageImpl implements ClientUpdateMessage, Sizeable, NewValueImporter {
  private static final long serialVersionUID = 7037106666445312400L;
  private static final Logger logger = LogService.getLogger();

  /**
   * The operation performed (e.g. AFTER_CREATE, AFTER_UPDATE, AFTER_DESTROY, AFTER_INVALIDATE,
   * AFTER_REGION_DESTROY)
   */
  EnumListenerEvent _operation;

  /**
   * The name of the <code>Region</code> that was updated
   */
  private String _regionName;

  /**
   * The key that was updated
   */
  private Object _keyOfInterest;

  /**
   * The new value
   */
  private Object _value;

  /**
   * Whether the value is a serialized object or just a byte[]
   */
  private byte _valueIsObject;

  /**
   * The callback argument
   */
  private Object _callbackArgument;

  /**
   * The membership id of the originator of the event
   */
  ClientProxyMembershipID _membershipId;

  /**
   * The event id of the event
   */
  EventID _eventIdentifier;

  private boolean _shouldConflate = false;

  /**
   * To determine if this client message is part of InterestList.
   */
  private volatile boolean _isInterestListPassed;

  /**
   * To determine if this client message is part of CQs.
   */
  private volatile boolean _hasCqs = false;

  /**
   * Map containing clientId and the cqs satisfied for the client.
   */
  private ClientCqConcurrentMap _clientCqs = null;

  /**
   * Client list satisfying the interestList who want values
   */
  private volatile Set<ClientProxyMembershipID> _clientInterestList;

  /**
   * Client list satisfying the interestList who want invalidations
   */
  private volatile Set<ClientProxyMembershipID> _clientInterestListInv;

  /**
   * To determine if the message is result of netLoad. If its net load the message is not delivered
   * to the client that has requested the load.
   */
  private transient boolean _isNetLoad = false;

  /**
   * Represents the changed bytes of this event's _value.
   *
   * @since GemFire 6.1
   */
  private byte[] deltaBytes = null;

  private VersionTag<?> versionTag;

  /* added up all constants and form single value */
  private static final int CONSTANT_MEMORY_OVERHEAD;

  /**
   * Constructor.
   *
   * @param operation The operation performed (e.g. AFTER_CREATE, AFTER_UPDATE, AFTER_DESTROY,
   *        AFTER_INVALIDATE, AFTER_REGION_DESTROY)
   * @param region The <code>Region</code> that was updated
   * @param keyOfInterest The key that was updated
   * @param value The new value
   * @param valueIsObject false if value is an actual byte[] that isn't serialized info
   * @param callbackArgument The callback argument
   * @param memberId membership id of the originator of the event
   */
  public ClientUpdateMessageImpl(EnumListenerEvent operation, InternalRegion region,
      Object keyOfInterest, Object value, byte valueIsObject, Object callbackArgument,
      ClientProxyMembershipID memberId, EventID eventIdentifier) {
    this(operation, region, keyOfInterest, value, null, valueIsObject, callbackArgument, memberId,
        eventIdentifier, null);
  }

  public ClientUpdateMessageImpl(EnumListenerEvent operation, InternalRegion region,
      Object keyOfInterest, Object value, byte[] delta, byte valueIsObject, Object callbackArgument,
      ClientProxyMembershipID memberId, EventID eventIdentifier, VersionTag<?> versionTag) {
    _operation = operation;
    _regionName = region.getFullPath();
    _keyOfInterest = keyOfInterest;
    _value = value;
    _valueIsObject = valueIsObject;
    _callbackArgument = callbackArgument;
    _membershipId = memberId;
    _eventIdentifier = eventIdentifier;
    _shouldConflate = isUpdate() && region.getEnableConflation();
    deltaBytes = delta;
    this.versionTag = versionTag;
  }

  /**
   * Constructor used by ClientInstantiatorMessage
   *
   * @param operation The operation performed (e.g. AFTER_CREATE, AFTER_UPDATE, AFTER_DESTROY,
   *        AFTER_INVALIDATE, AFTER_REGION_DESTROY)
   * @param memberId membership id of the originator of the event
   * @param eventIdentifier EventID of this message
   */
  protected ClientUpdateMessageImpl(EnumListenerEvent operation, ClientProxyMembershipID memberId,
      EventID eventIdentifier) {
    _operation = operation;
    _membershipId = memberId;
    _eventIdentifier = eventIdentifier;
  }

  /**
   * default constructor
   *
   */
  public ClientUpdateMessageImpl() {

  }

  @Override
  public String getRegionName() {
    return _regionName;
  }

  @Override
  public Object getKeyOfInterest() {
    return _keyOfInterest;
  }

  @Override
  public EnumListenerEvent getOperation() {
    return _operation;
  }

  @Override
  public Object getValue() {
    return _value;
  }

  @Override
  public boolean valueIsObject() {
    return (_valueIsObject == 0x01);
  }

  /**
   * @return the callback argument
   */
  public Object getCallbackArgument() {
    return _callbackArgument;
  }

  /// Conflatable interface methods ///

  /**
   * Determines whether or not to conflate this message. This method will answer true IFF the
   * message's operation is AFTER_UPDATE and its region has enabled are conflation. Otherwise, this
   * method will answer false. Messages whose operation is AFTER_CREATE, AFTER_DESTROY,
   * AFTER_INVALIDATE or AFTER_REGION_DESTROY are not conflated.
   *
   * @return Whether to conflate this message
   */
  @Override
  public boolean shouldBeConflated() {
    // If the message is an update, it may be conflatable. If it is a
    // create, destroy, invalidate or destroy-region, it is not conflatable.
    // Only updates are conflated. If it is an update, then verify that
    // the region has conflation enabled.
    return _shouldConflate;
  }

  @Override
  public String getRegionToConflate() {
    return _regionName;
  }

  @Override
  public Object getKeyToConflate() {
    return _keyOfInterest;
  }

  @Override
  public Object getValueToConflate() {
    return _value;
  }

  @Override
  public void setLatestValue(Object value) {
    // does this also need to set _valueIsObject
    _value = value;
  }

  /// End Conflatable interface methods ///

  @Override
  public ClientProxyMembershipID getMembershipId() {
    return _membershipId;
  }

  /**
   * Returns the unqiue event eventifier for event corresponding to this message.
   *
   * @return the unqiue event eventifier for event corresponding to this message.
   */
  @Override
  public EventID getEventId() {
    return _eventIdentifier;
  }

  @Override
  public VersionTag<?> getVersionTag() {
    return versionTag;
  }

  @Override
  public boolean isCreate() {
    return _operation == EnumListenerEvent.AFTER_CREATE;
  }

  @Override
  public boolean isUpdate() {
    return _operation == EnumListenerEvent.AFTER_UPDATE;
  }

  @Override
  public boolean isDestroy() {
    return _operation == EnumListenerEvent.AFTER_DESTROY;
  }

  @Override
  public boolean isInvalidate() {
    return _operation == EnumListenerEvent.AFTER_INVALIDATE;
  }

  @Override
  public boolean isDestroyRegion() {
    return _operation == EnumListenerEvent.AFTER_REGION_DESTROY;
  }

  @Override
  public boolean isClearRegion() {
    return _operation == EnumListenerEvent.AFTER_REGION_CLEAR;
  }

  private boolean isInvalidateRegion() {
    return _operation == EnumListenerEvent.AFTER_REGION_INVALIDATE;
  }

  @Override
  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException {
    // the MessageDispatcher uses getMessage(CacheClientProxy, byte[]) for this class
    throw new Error("ClientUpdateMessage.getMessage(proxy) should not be invoked");
  }

  /**
   * Returns a <code>Message</code> generated from the fields of this
   * <code>ClientUpdateMessage</code>.
   *
   * @param latestValue Object containing the latest value to use. This could be the original value
   *        if conflation is not enabled, or it could be a conflated value if conflation is enabled.
   * @return a <code>Message</code> generated from the fields of this
   *         <code>ClientUpdateMessage</code>
   * @see org.apache.geode.internal.cache.tier.sockets.Message
   */

  protected Message getMessage(CacheClientProxy proxy, byte[] latestValue) throws IOException {
    KnownVersion clientVersion = proxy.getVersion();
    byte[] serializedValue = null;
    Message message;
    boolean conflation;
    conflation = (proxy.clientConflation == Handshake.CONFLATION_ON)
        || (proxy.clientConflation == Handshake.CONFLATION_DEFAULT && shouldBeConflated());

    if (latestValue != null) {
      serializedValue = latestValue;
    } else {
      /*
       * This means latestValue is instance of Delta, and its delta has already been extracted and
       * put into deltaBytes. We serialize the value.
       */
      if (deltaBytes == null || isCreate()) {
        // Delta could not be extracted. We would need to send full value.
        // OR A CREATE operation has a value which has delta. But we send full value for CREATE.
        // So serialize it.
        _value = serializedValue = CacheServerHelper.serialize(latestValue);
      }
    }
    if (clientVersion.isNotOlderThan(KnownVersion.GFE_70)) {
      message = getGFE70Message(proxy, serializedValue, conflation, clientVersion);
    } else {
      throw new IOException(
          "Unsupported client version for server-to-client message creation: " + clientVersion);
    }

    return message;
  }

  private Message getGFE70Message(CacheClientProxy proxy, byte[] p_latestValue,
      boolean conflation, KnownVersion clientVersion) throws IOException {
    byte[] latestValue = p_latestValue;

    Message message;
    ClientProxyMembershipID proxyId = proxy.getProxyID();
    // Add CQ info.
    int cqMsgParts = 0;
    boolean clientHasCq = _hasCqs && (getCqs(proxyId) != null);

    if (clientHasCq) {
      cqMsgParts = (getCqs(proxyId).length * 2) + 1;
    }

    if (isCreate() || isUpdate()) {
      // Create or update event
      if (_clientInterestListInv != null && _clientInterestListInv.contains(proxyId)) {
        // Client is registered for invalidates.
        if (cqMsgParts > 0) {
          cqMsgParts++; // To store base operation type for CQ.
        }

        message = getMessage(7 + cqMsgParts, clientVersion);
        message.setMessageType(MessageType.LOCAL_INVALIDATE);
        message.addStringPart(_regionName, true);
        message.addStringOrObjPart(_keyOfInterest);
      } else {
        // Notify by subscription - send the value
        message = getMessage(9 + cqMsgParts, clientVersion);
        if (isCreate()) {
          message.setMessageType(MessageType.LOCAL_CREATE);
          message.addStringPart(_regionName, true);
          message.addStringOrObjPart(_keyOfInterest);
          message.addObjPart(Boolean.FALSE); // NO delta
          // Add the value (which has already been serialized)
          message.addRawPart(latestValue, (_valueIsObject == 0x01));
        } else {
          message.setMessageType(MessageType.LOCAL_UPDATE);
          message.addStringPart(_regionName, true);
          message.addStringOrObjPart(_keyOfInterest);

          if (deltaBytes != null && !conflation && !proxy.isMarkerEnqueued()
              && !proxy.getRegionsWithEmptyDataPolicy().containsKey(_regionName)) {
            message.addObjPart(Boolean.TRUE);
            message.addBytesPart(deltaBytes);
            proxy.getStatistics().incDeltaMessagesSent();
          } else {
            message.addObjPart(Boolean.FALSE);
            if (latestValue == null) {
              if (!(_value instanceof byte[])) {
                _value = CacheServerHelper.serialize(_value);
              }
              latestValue = (byte[]) _value;
            }
            // Add the value (which has already been serialized)
            message.addRawPart(latestValue, (_valueIsObject == 0x01));
          }
        }
      }

      message.addObjPart(_callbackArgument);
      if (versionTag != null) {
        versionTag.setCanonicalIDs(proxy.getCache().getDistributionManager());
      }
      message.addObjPart(versionTag);
      message.addObjPart(isClientInterested(proxyId));
      message.addObjPart(clientHasCq);

      if (clientHasCq) {
        if (message.getMessageType() == MessageType.LOCAL_INVALIDATE) {
          // in case of invalidate, set the region operation type.
          message.addIntPart(isCreate() ? MessageType.LOCAL_CREATE : MessageType.LOCAL_UPDATE);
        }
        addCqsToMessage(proxyId, message);
      }
    } else if (isDestroy() || isInvalidate()) {
      if (isDestroy()) {
        message = getMessage(7 + cqMsgParts, clientVersion);
        message.setMessageType(MessageType.LOCAL_DESTROY);
      } else {
        if (clientHasCq) {
          cqMsgParts++;/* To store the region operation for CQ */
        }
        message = getMessage(7 + cqMsgParts, clientVersion);
        message.setMessageType(MessageType.LOCAL_INVALIDATE);
      }
      message.addStringPart(_regionName, true);
      message.addStringOrObjPart(_keyOfInterest);
      message.addObjPart(_callbackArgument);
      message.addObjPart(versionTag);
      message.addObjPart(isClientInterested(proxyId));
      message.addObjPart(clientHasCq);

      if (clientHasCq) {
        if (isInvalidate()) {
          // This is to take care when invalidate message is getting sent
          // to the Client. See the code for create/update operation.
          message.addIntPart(MessageType.LOCAL_INVALIDATE);
        }
        addCqsToMessage(proxyId, message);
      }
    } else if (isDestroyRegion()) {
      message = getMessage(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.LOCAL_DESTROY_REGION);
      message.addStringPart(_regionName, true);
      message.addObjPart(_callbackArgument);
      message.addObjPart(clientHasCq);

      if (clientHasCq) {
        addCqsToMessage(proxyId, message);
      }
    } else if (isClearRegion()) {
      message = getMessage(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.CLEAR_REGION);
      message.addStringPart(_regionName, true);
      message.addObjPart(_callbackArgument);
      message.addObjPart(clientHasCq);

      if (clientHasCq) {
        addCqsToMessage(proxyId, message);
      }
    } else if (isInvalidateRegion()) {
      message = getMessage(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.INVALIDATE_REGION);
      message.addStringPart(_regionName, true);
      message.addObjPart(_callbackArgument);

      // Add CQ status.
      message.addObjPart(clientHasCq);

      if (clientHasCq) {
        addCqsToMessage(proxyId, message);
      }
    } else {
      throw new InternalGemFireError("Don't know what kind of message");
    }

    message.setTransactionId(0);
    // Add the EventId since 5.1 (used to prevent duplicate events
    // received on the client side after a failover)
    message.addObjPart(_eventIdentifier);
    return message;
  }

  private static final ThreadLocal<Map<Integer, Message>> CACHED_MESSAGES =
      ThreadLocal.withInitial(HashMap::new);

  private Message getMessage(int numParts, KnownVersion clientVersion) {
    Message m = CACHED_MESSAGES.get().get(numParts);
    if (m == null) {
      m = new Message(numParts, KnownVersion.CURRENT);
      CACHED_MESSAGES.get().put(numParts, m);
    }
    m.clearParts();
    m.setVersion(clientVersion);
    return m;
  }

  /**
   * @return boolean true if the event is due to net load.
   */
  @Override
  public boolean isNetLoad() {
    return _isNetLoad;
  }

  /**
   * @param isNetLoad boolean true if the event is due to net load.
   */
  @Override
  public void setIsNetLoad(boolean isNetLoad) {
    _isNetLoad = isNetLoad;
  }


  /**
   * @return boolean true if cq info is present for the given proxy.
   */
  @Override
  public boolean hasCqs(ClientProxyMembershipID clientId) {
    if (_clientCqs != null) {
      CqNameToOp cqs = _clientCqs.get(clientId);
      return cqs != null && !cqs.isEmpty();
    }
    return false;
  }

  /**
   * @return boolean true if cq info is present.
   */
  @Override
  public boolean hasCqs() {
    return _hasCqs;
  }

  /**
   * Returns the cqs for the given client.
   *
   */
  private String[] getCqs(ClientProxyMembershipID clientId) {
    String[] cqNames = null;
    if (_clientCqs != null) {
      CqNameToOp cqs = _clientCqs.get(clientId);

      if (cqs != null && !cqs.isEmpty()) {
        cqNames = cqs.getNames();
      }
    }
    return cqNames;
  }

  public ClientCqConcurrentMap getClientCqs() {
    return _clientCqs;
  }

  public void addOrSetClientCqs(ClientProxyMembershipID proxyID, ClientCqConcurrentMap clientCqs) {
    if (_clientCqs == null) {
      _clientCqs = clientCqs;
    } else {
      _clientCqs.put(proxyID, clientCqs.get(proxyID));
    }
  }

  synchronized void addClientCq(ClientProxyMembershipID clientId, String cqName, Integer cqEvent) {
    if (_clientCqs == null) {
      _clientCqs = new ClientCqConcurrentMap();
      _hasCqs = true;
    }
    CqNameToOp cqInfo = _clientCqs.get(clientId);
    if (cqInfo == null) {
      cqInfo = new CqNameToOpSingleEntry(cqName, cqEvent);
      _clientCqs.put(clientId, cqInfo);
    } else if (!cqInfo.isFull()) {
      cqInfo.add(cqName, cqEvent);
    } else {
      cqInfo = new CqNameToOpHashMap((CqNameToOpSingleEntry) cqInfo);
      cqInfo.add(cqName, cqEvent);
      _clientCqs.put(clientId, cqInfo);
    }
  }

  private void addCqsToMessage(ClientProxyMembershipID proxyId, Message message) {
    if (_clientCqs != null) {
      CqNameToOp cqs = _clientCqs.get(proxyId);
      if (cqs != null) {
        message.addIntPart(cqs.size() * 2);
        cqs.addToMessage(message);
      }
    }
  }

  public void removeClientCq(ClientProxyMembershipID clientId, InternalCqQuery cqToClose) {
    CqNameToOp cqs = getClientCq(clientId);
    if (cqs != null) {
      cqs.delete(cqToClose.getName());
      // remove clientId key if no more cqs exist for this clientId
      if (cqs.isEmpty()) {
        _clientCqs.remove(clientId);
      }
    }
  }

  /**
   * Set the region name that was updated.
   */
  @Override
  public void setRegionName(String regionName) {
    _regionName = regionName;
  }

  /**
   * @see HAEventWrapper#fromData(DataInput, DeserializationContext)
   * @see HAContainerRegion#get(Object)
   */
  public void setEventIdentifier(EventID eventId) {
    if (_eventIdentifier == null) {
      _eventIdentifier = eventId;
    }
  }

  /**
   * @see HAEventWrapper#fromData(DataInput, DeserializationContext)
   * @see HAContainerRegion#get(Object)
   */
  public void setClientCqs(ClientCqConcurrentMap clientCqs) {
    if (_clientCqs == null) {
      _clientCqs = clientCqs;
    }
  }

  void addClientInterestList(Set<ClientProxyMembershipID> clientIds, boolean receiveValues) {
    if (receiveValues) {
      if (_clientInterestList == null) {
        _clientInterestList = clientIds;
      } else {
        _clientInterestList.addAll(clientIds);
      }
    } else {
      if (_clientInterestListInv == null) {
        _clientInterestListInv = clientIds;
      } else {
        _clientInterestListInv.addAll(clientIds);
      }
    }
  }

  public void addClientInterestList(ClientProxyMembershipID clientId, boolean receiveValues) {
    // This happens under synchronization on HAContainer.
    if (receiveValues) {
      if (_clientInterestList == null) {
        _clientInterestList = ConcurrentHashMap.newKeySet();
      }
      _clientInterestList.add(clientId);
    } else {
      if (_clientInterestListInv == null) {
        _clientInterestListInv = ConcurrentHashMap.newKeySet();
      }
      _clientInterestListInv.add(clientId);
    }
  }

  @Override
  public boolean isClientInterested(ClientProxyMembershipID clientId) {
    return (_clientInterestList != null && _clientInterestList.contains(clientId))
        || (_clientInterestListInv != null && _clientInterestListInv.contains(clientId));
  }

  public boolean isClientInterestedInUpdates(ClientProxyMembershipID clientId) {
    return (_clientInterestList != null && _clientInterestList.contains(clientId));
  }

  public boolean isClientInterestedInInvalidates(ClientProxyMembershipID clientId) {
    return (_clientInterestListInv != null && _clientInterestListInv.contains(clientId));
  }

  @VisibleForTesting
  boolean hasClientsInterestedInUpdates() {
    return _clientInterestList != null;
  }

  @VisibleForTesting
  boolean hasClientsInterestedInInvalidates() {
    return _clientInterestListInv != null;
  }

  private Object deserialize(byte[] serializedBytes) {
    Object deserializedObject = serializedBytes;
    // This is a debugging method so ignore all exceptions like
    // ClassNotFoundException
    try (ByteArrayDataInput dis = new ByteArrayDataInput(serializedBytes)) {
      deserializedObject = DataSerializer.readObject(dis);
    } catch (Exception ignored) {
    }
    return deserializedObject;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("ClientUpdateMessageImpl[").append("op=").append(_operation)
        .append(";region=").append(_regionName).append(";key=").append(_keyOfInterest);
    if (logger.isTraceEnabled()) {
      buffer.append(";value=").append(
          (_value instanceof byte[]) ? deserialize((byte[]) _value) : _value);
    }
    buffer.append(";isObject=").append(_valueIsObject).append(";cbArg=")
        .append(_callbackArgument).append(";memberId=").append(_membershipId)
        .append(";eventId=").append(_eventIdentifier).append(";shouldConflate=")
        .append(_shouldConflate).append(";versionTag=").append(versionTag).append(";hasCqs=")
        .append(_hasCqs)
        // skip _logger :-)
        .append("]");
    return buffer.toString();
  }

  @Override
  public int getDSFID() {
    return CLIENT_UPDATE_MESSAGE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    out.writeByte(_operation.getEventCode());
    DataSerializer.writeString(_regionName, out);
    DataSerializer.writeObject(_keyOfInterest, out);
    if (_value instanceof byte[]) {
      DataSerializer.writeByteArray((byte[]) _value, out);
    } else {
      DataSerializer.writeByteArray(CacheServerHelper.serialize(_value), out);
    }
    out.writeByte(_valueIsObject);
    DataSerializer.writeObject(_membershipId, out);
    out.writeBoolean(_shouldConflate);
    out.writeBoolean(_isInterestListPassed);
    DataSerializer.writeByteArray(deltaBytes, out);
    out.writeBoolean(_hasCqs);
    DataSerializer.writeObject(_callbackArgument, out);

    HashSet<ClientProxyMembershipID> clientInterestListSnapshot =
        _clientInterestList != null
            ? new HashSet<>(_clientInterestList)
            : null;
    DataSerializer.writeHashSet(clientInterestListSnapshot, out);

    HashSet<ClientProxyMembershipID> clientInterestListInvSnapshot =
        _clientInterestListInv != null
            ? new HashSet<>(_clientInterestListInv)
            : null;
    DataSerializer.writeHashSet(clientInterestListInvSnapshot, out);

    DataSerializer.writeObject(versionTag, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    _operation = EnumListenerEvent.getEnumListenerEvent(in.readByte());
    _regionName = DataSerializer.readString(in);
    _keyOfInterest = DataSerializer.readObject(in);
    _value = DataSerializer.readByteArray(in);
    _valueIsObject = in.readByte();
    _membershipId = ClientProxyMembershipID.readCanonicalized(in);
    _shouldConflate = in.readBoolean();
    _isInterestListPassed = in.readBoolean();
    deltaBytes = DataSerializer.readByteArray(in);
    _hasCqs = in.readBoolean();
    _callbackArgument = DataSerializer.readObject(in);

    CacheClientNotifier ccn = CacheClientNotifier.getInstance();

    Set<ClientProxyMembershipID> clientInterestList = DataSerializer.readHashSet(in);
    _clientInterestList = ccn != null && clientInterestList != null
        ? ccn.getProxyIDs(clientInterestList)
        : null;

    Set<ClientProxyMembershipID> clientInterestListInv = DataSerializer.readHashSet(in);
    _clientInterestListInv = ccn != null && clientInterestListInv != null
        ? ccn.getProxyIDs(clientInterestListInv)
        : null;

    versionTag = DataSerializer.readObject(in);
  }

  private Object getOriginalCallbackArgument() {
    Object result = _callbackArgument;
    while (result instanceof WrappedCallbackArgument) {
      WrappedCallbackArgument wca = (WrappedCallbackArgument) result;
      result = wca.getOriginalCallbackArg();
    }
    return result;
  }

  /*
   * Statically calculate constant overhead for ClientUpdateMessageImpl instance.
   */
  static {

    // The sizes of the following variables are calculated:
    // - primitive and object instance variable references
    //
    // The sizes of the following variables are not calculated:

    // - the key because it is a reference
    // - the region and regionName because they are references
    // - the operation because it is a reference
    // - the membershipId because it is a reference
    // - the logger because it is a reference
    // - the keyOfInterest because it is a reference
    // - the clientCqs because it is a reference
    // - the clientInterestList because it is a reference
    // - the eventIdentifier because it is a reference

    // The size of instances of the following internal datatypes were estimated
    // using a NullDataOutputStream and hardcoded into this method:

    // - the id (an instance of EventId)
    int size = 0;

    // Add overhead for this instance.
    size += Sizeable.PER_OBJECT_OVERHEAD;

    // Add object references
    // _operation reference = 4 bytes
    // _regionName reference = 4 bytes
    // _keyOfInterest reference = 4 bytes
    // _value reference = 4 bytes
    // _callbackArgument reference = 4 bytes
    // _membershipId reference = 4 bytes
    // _eventIdentifier reference = 4 bytes
    // _logger reference = 4 bytes
    // _clientCqs reference = 4 bytes
    // _clientInterestList reference = 4 bytes
    size += 40;

    // Add primitive references
    // byte _valueIsObject = 1 byte
    // boolean _shouldConflate = 1 byte
    // boolean _isInterestListPassed = 1 byte
    // boolean _hasCqs = 1 byte
    // boolean _isNetLoad = 1 byte
    size += 5;

    // not sure on the kind on wrapper is around callbackArgument
    // The callback argument (a GatewayEventCallbackArgument wrapping an Object
    // which is the original callback argument)
    // The hardcoded value below represents the GatewayEventCallbackArgument
    // and was estimated using a NullDataOutputStream
    size += Sizeable.PER_OBJECT_OVERHEAD + 194; // do we need it
    // add overhead for callback Argument
    size += Sizeable.PER_OBJECT_OVERHEAD;
    // total overhead
    CONSTANT_MEMORY_OVERHEAD = size;
  }

  @Override
  public int getSizeInBytes() {

    int size = CONSTANT_MEMORY_OVERHEAD;

    // The value (a byte[])
    if (_value != null) {
      size += CachedDeserializableFactory.calcMemSize(_value);
    }

    // The sizeOf call gets the size of the input callback argument.
    size += sizeOf(getOriginalCallbackArgument());
    return size;
  }

  private int sizeOf(Object obj) {
    int size = 0;
    if (obj == null) {
      return size;
    }
    if (obj instanceof String) {
      size = ObjectSizer.DEFAULT.sizeof(obj);
    } else if (obj instanceof Integer) {
      size = 4; // estimate
    } else if (obj instanceof Long) {
      size = 8; // estimate
    } else {
      size = CachedDeserializableFactory.calcMemSize(obj) - Sizeable.PER_OBJECT_OVERHEAD;
    }
    return size;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage#needsNoAuthorizationCheck()
   */
  @Override
  public boolean needsNoAuthorizationCheck() {
    return false;
  }

  @Override
  public CqNameToOp getClientCq(ClientProxyMembershipID clientId) {
    if (_clientCqs != null) {
      return _clientCqs.get(clientId);
    } else {
      return null;
    }
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }


  /**
   * Even though this class is just a ConcurrentHashMap I wanted it to be its own class so it could
   * be easily identified in heap dumps. The concurrency level on these should be 1 to keep their
   * memory footprint down.
   */
  public static class ClientCqConcurrentMap
      extends ConcurrentHashMap<ClientProxyMembershipID, CqNameToOp> {
    public ClientCqConcurrentMap(int initialCapacity, float loadFactor, int concurrencyLevel) {
      super(initialCapacity, loadFactor, concurrencyLevel);
    }

    ClientCqConcurrentMap() {
      super(16, 1.0f, 1);
    }
  }
  /**
   * Replaces what used to be a HashMap<String, Integer>.
   */
  public interface CqNameToOp extends Sendable {
    boolean isEmpty();

    /**
     * Returns true if calling add would fail.
     */
    boolean isFull();

    void addToMessage(Message message);

    int size();

    String[] getNames();

    void add(String name, Integer op);

    void delete(String name);
  }
  /**
   * Contains either zero or one String to int tuples. This is a common case and this impl has a
   * much smaller memory footprint than a HashMap with one entry.
   */
  public static class CqNameToOpSingleEntry implements CqNameToOp {
    private String[] name;
    private int op;

    private static final String[] EMPTY_NAMES_ARRAY = new String[0];


    CqNameToOpSingleEntry(String name, Integer op) {
      initializeName(name);
      this.op = op;
    }

    private void initializeName(String name) {
      this.name = new String[] {name};
    }

    @Override
    public void sendTo(DataOutput out) throws IOException {
      // When serialized it needs to look just as if writeObject was called on a HASH_MAP
      out.writeByte(DSCODE.HASH_MAP.toByte());
      int size = size();
      InternalDataSerializer.writeArrayLength(size, out);
      if (size > 0) {
        DataSerializer.writeObject(name[0], out);
        DataSerializer.writeObject(op, out);
      }
    }

    @Override
    public boolean isEmpty() {
      return name == null;
    }

    @Override
    public void addToMessage(Message message) {
      if (!isEmpty()) {
        message.addStringPart(name[0], true);
        message.addIntPart(op);
      }
    }

    @Override
    public int size() {
      return isEmpty() ? 0 : 1;
    }

    @Override
    public String[] getNames() {
      return (isEmpty()) ? EMPTY_NAMES_ARRAY : name;
    }

    @Override
    public void add(String name, Integer op) {
      if (isEmpty()) {
        this.name = new String[] {name};
        this.op = op;
      } else if (this.name[0].equals(name)) {
        this.op = op;
      } else {
        throw new IllegalStateException("tried to add to a full CqNameToOpSingleEntry");
      }
    }

    @Override
    public void delete(String name) {
      if (name.equals(this.name[0])) {
        this.name = null;
      }
    }

    @Override
    public boolean isFull() {
      return !isEmpty();
    }
  }
  /**
   * Basically just a ConcurrentHashMap<String, Integer> but limits itself to the CqNameToOp
   * interface.
   */
  public static class CqNameToOpHashMap extends ConcurrentHashMap<String, Integer>
      implements CqNameToOp {
    public CqNameToOpHashMap(int initialCapacity) {
      super(initialCapacity, 1.0f);
    }

    CqNameToOpHashMap(CqNameToOpSingleEntry se) {
      super(2, 1.0f);
      add(se.name[0], se.op);
    }

    @Override
    public void sendTo(DataOutput out) throws IOException {
      // When serialized it needs to look just as if writeObject was called on a HASH_MAP
      out.writeByte(DSCODE.HASH_MAP.toByte());
      DataSerializer.writeConcurrentHashMap(this, out);
    }

    @Override
    public String[] getNames() {
      String[] cqNames = new String[size()];
      cqNames = keySet().toArray(cqNames);
      return cqNames;
    }

    @Override
    public void addToMessage(Message message) {
      for (Entry<String, Integer> entry : entrySet()) {
        // Add CQ Name.
        String cq = entry.getKey();
        message.addStringPart(cq, true);
        // Add CQ Op.
        int op = entry.getValue();
        message.addIntPart(op);
      }
    }

    @Override
    public void add(String name, Integer op) {
      put(name, op);
    }

    @Override
    public void delete(String name) {
      remove(name);
    }

    @Override
    public boolean isFull() {
      return false;
    }
  }

  // NewValueImporter methods

  @Override
  public boolean prefersNewSerialized() {
    return true;
  }

  @Override
  public boolean isUnretainedNewReferenceOk() {
    return false;
  }

  @Override
  public void importNewObject(Object nv, boolean isSerialized) {
    if (!isSerialized) {
      throw new IllegalStateException("Expected importNewBytes to be called.");
    }
    try {
      _value = CacheServerHelper.serialize(nv);
    } catch (IOException e) {
      throw new GemFireIOException("Exception serializing entry value", e);
    }
  }

  @Override
  public void importNewBytes(byte[] nv, boolean isSerialized) {
    if (!isSerialized) {
      // The value is already a byte[]. Set _valueIsObject flag to 0x00
      // (not an object)
      _valueIsObject = 0x00;
    }
    _value = nv;
  }

}
