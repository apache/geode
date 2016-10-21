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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireIOException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Sendable;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.EntryEventImpl.NewValueImporter;
import org.apache.geode.internal.cache.EntryEventImpl.SerializedCacheValueImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.WrappedCallbackArgument;
import org.apache.geode.internal.cache.ha.HAContainerRegion;
import org.apache.geode.internal.cache.lru.Sizeable;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;


/**
 * Class <code>ClientUpdateMessageImpl</code> is a message representing a cache operation that is
 * sent from a server to an interested client.
 *
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
  protected EnumListenerEvent _operation;

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
  protected byte _valueIsObject;

  /**
   * The callback argument
   */
  protected Object _callbackArgument;

  /**
   * The membership id of the originator of the event
   */
  protected ClientProxyMembershipID _membershipId;

  /**
   * The event id of the event
   */
  protected EventID _eventIdentifier;

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

  private VersionTag versionTag;

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
  public ClientUpdateMessageImpl(EnumListenerEvent operation, LocalRegion region,
      Object keyOfInterest, Object value, byte valueIsObject, Object callbackArgument,
      ClientProxyMembershipID memberId, EventID eventIdentifier) {
    this(operation, region, keyOfInterest, value, null, valueIsObject, callbackArgument, memberId,
        eventIdentifier, null);
  }

  public ClientUpdateMessageImpl(EnumListenerEvent operation, LocalRegion region,
      Object keyOfInterest, Object value, byte[] delta, byte valueIsObject, Object callbackArgument,
      ClientProxyMembershipID memberId, EventID eventIdentifier, VersionTag versionTag) {
    // this._clientInterestList = new HashSet();
    // this._clientInterestListInv = new HashSet();
    this._operation = operation;
    this._regionName = region.getFullPath();
    this._keyOfInterest = keyOfInterest;
    this._value = value;
    this._valueIsObject = valueIsObject;
    this._callbackArgument = callbackArgument;
    this._membershipId = memberId;
    this._eventIdentifier = eventIdentifier;
    this._shouldConflate = (isUpdate() && region.getEnableConflation());
    this.deltaBytes = delta;
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
    // this._clientInterestList = new HashSet();
    // this._clientInterestListInv = new HashSet();
    this._operation = operation;
    this._membershipId = memberId;
    this._eventIdentifier = eventIdentifier;
  }

  /**
   * default constructor
   *
   */
  public ClientUpdateMessageImpl() {

  }

  public String getRegionName() {
    return this._regionName;
  }

  public Object getKeyOfInterest() {
    return this._keyOfInterest;
  }

  public EnumListenerEvent getOperation() {
    return this._operation;
  }

  public Object getValue() {
    return this._value;
  }

  public boolean valueIsObject() {
    return (this._valueIsObject == 0x01);
  }

  /**
   * @return the callback argument
   */
  public Object getCallbackArgument() {
    return this._callbackArgument;
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
  public boolean shouldBeConflated() {
    // If the message is an update, it may be conflatable. If it is a
    // create, destroy, invalidate or destroy-region, it is not conflatable.
    // Only updates are conflated. If it is an update, then verify that
    // the region has conflation enabled.
    return this._shouldConflate;
  }

  public String getRegionToConflate() {
    return this._regionName;
  }

  public Object getKeyToConflate() {
    return this._keyOfInterest;
  }

  public Object getValueToConflate() {
    return this._value;
  }

  public void setLatestValue(Object value) {
    // does this also need to set _valueIsObject
    this._value = value;
  }

  /// End Conflatable interface methods ///

  public ClientProxyMembershipID getMembershipId() {
    return this._membershipId;
  }

  /**
   * Returns the unqiue event eventifier for event corresponding to this message.
   *
   * @return the unqiue event eventifier for event corresponding to this message.
   */
  public EventID getEventId() {
    return this._eventIdentifier;
  }

  public VersionTag getVersionTag() {
    return this.versionTag;
  }

  public boolean isCreate() {
    return this._operation == EnumListenerEvent.AFTER_CREATE;
  }

  public boolean isUpdate() {
    return this._operation == EnumListenerEvent.AFTER_UPDATE;
  }

  public boolean isDestroy() {
    return this._operation == EnumListenerEvent.AFTER_DESTROY;
  }

  public boolean isInvalidate() {
    return this._operation == EnumListenerEvent.AFTER_INVALIDATE;
  }

  public boolean isDestroyRegion() {
    return this._operation == EnumListenerEvent.AFTER_REGION_DESTROY;
  }

  public boolean isClearRegion() {
    return this._operation == EnumListenerEvent.AFTER_REGION_CLEAR;
  }

  public boolean isInvalidateRegion() {
    return this._operation == EnumListenerEvent.AFTER_REGION_INVALIDATE;
  }

  public boolean isClientCompatible() {
    return false;
  }

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
   * @throws IOException
   * @see org.apache.geode.internal.cache.tier.sockets.Message
   */

  protected Message getMessage(CacheClientProxy proxy, byte[] latestValue) throws IOException {
    Version clientVersion = proxy.getVersion();
    byte[] serializedValue = null;
    Message message = null;
    boolean conflation = false;
    conflation = (proxy.clientConflation == HandShake.CONFLATION_ON)
        || (proxy.clientConflation == HandShake.CONFLATION_DEFAULT && this.shouldBeConflated());

    if (latestValue != null) {
      serializedValue = latestValue;
    } else {
      /**
       * This means latestValue is instance of Delta, and its delta has already been extracted and
       * put into deltaBytes. We serialize the value.
       */
      if (this.deltaBytes == null || isCreate()) {
        // Delta could not be extracted. We would need to send full value.
        // OR A CREATE operation has a value which has delta. But we send full value for CREATE.
        // So serialize it.
        this._value = serializedValue = CacheServerHelper.serialize(latestValue);
      }
    }
    if (clientVersion.compareTo(Version.GFE_70) >= 0) {
      message = getGFE70Message(proxy, serializedValue, conflation, clientVersion);
    } else if (clientVersion.compareTo(Version.GFE_65) >= 0) {
      message = getGFE65Message(proxy, serializedValue, conflation, clientVersion);
    } else if (clientVersion.compareTo(Version.GFE_61) >= 0) {
      message = getGFE61Message(proxy, serializedValue, conflation, clientVersion);
    } else if (clientVersion.compareTo(Version.GFE_57) >= 0) {
      message = getGFEMessage(proxy.getProxyID(), latestValue, clientVersion);
    } else {
      throw new IOException(
          "Unsupported client version for server-to-client message creation: " + clientVersion);
    }

    return message;
  }

  protected Message getGFEMessage(ClientProxyMembershipID proxyId, byte[] latestValue,
      Version clientVersion) throws IOException {
    Message message = null;
    // Add CQ info.
    int cqMsgParts = 0;
    boolean clientHasCq = this._hasCqs && (this.getCqs(proxyId) != null);

    if (clientHasCq) {
      cqMsgParts = (this.getCqs(proxyId).length * 2) + 1;
    }

    if (isCreate() || isUpdate()) {
      // Create or update event
      if (this._clientInterestListInv != null && this._clientInterestListInv.contains(proxyId)) {
        // Notify all - do not send the value
        message = new Message(6, clientVersion);
        message.setMessageType(MessageType.LOCAL_INVALIDATE);
        message.addStringPart(this._regionName, true);
        // Currently serializing the key here instead of when the message
        // is put in the queue so that it can be conflated it later
        message.addStringOrObjPart(this._keyOfInterest);
        message.addObjPart(this._callbackArgument);
        message.addObjPart(Boolean.valueOf(isClientInterested(proxyId)));
        message.addObjPart(Boolean.FALSE);

      } else {
        // Notify by subscription - send the value
        message = new Message(7 + cqMsgParts, clientVersion);

        // Set message type
        if (isCreate()) {
          message.setMessageType(MessageType.LOCAL_CREATE);
        } else {
          message.setMessageType(MessageType.LOCAL_UPDATE);
        }
        message.addStringPart(this._regionName, true);
        // Currently serializing the key here instead of when the message
        // is put in the queue so that it can be conflated it later
        message.addStringOrObjPart(this._keyOfInterest);
        message.addRawPart(latestValue, (this._valueIsObject == 0x01));
        message.addObjPart(this._callbackArgument);
        message.addObjPart(Boolean.valueOf(isClientInterested(proxyId)));
        message.addObjPart(Boolean.valueOf(clientHasCq));

        if (clientHasCq) {
          this.addCqsToMessage(proxyId, message);
        }
      }
    } else if (isDestroy() || isInvalidate()) {
      message = new Message(6 + cqMsgParts, clientVersion);
      if (isDestroy()) {
        message.setMessageType(MessageType.LOCAL_DESTROY);
      } else {
        message.setMessageType(MessageType.LOCAL_INVALIDATE);
      }
      message.addStringPart(this._regionName, true);

      // Currently serializing the key here instead of when the message
      // is put in the queue so that it can be conflated it later
      message.addStringOrObjPart(this._keyOfInterest);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(isClientInterested(proxyId)));
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isDestroyRegion()) {
      message = new Message(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.LOCAL_DESTROY_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isClearRegion()) {
      message = new Message(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.CLEAR_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isInvalidateRegion()) {
      message = new Message(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.INVALIDATE_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else {
      throw new InternalGemFireError("Don't know what kind of message");
    }

    message.setTransactionId(0);
    // Add the EventId since 5.1 (used to prevent duplicate events
    // received on the client side after a failover)
    message.addObjPart(this._eventIdentifier);
    return message;
  }

  protected Message getGFE61Message(CacheClientProxy proxy, byte[] latestValue, boolean conflation,
      Version clientVersion) throws IOException {
    Message message = null;
    ClientProxyMembershipID proxyId = proxy.getProxyID();

    // Add CQ info.
    int cqMsgParts = 0;
    boolean clientHasCq = this._hasCqs && (this.getCqs(proxyId) != null);

    if (clientHasCq) {
      cqMsgParts = (this.getCqs(proxyId).length * 2) + 1;
    }

    if (isCreate() || isUpdate()) {
      // Create or update event
      if (this._clientInterestListInv != null && this._clientInterestListInv.contains(proxyId)) {
        // Notify all - do not send the value
        message = new Message(6, clientVersion);
        message.setMessageType(MessageType.LOCAL_INVALIDATE);

        // Add the region name
        message.addStringPart(this._regionName, true);

        // Add the key
        // Currently serializing the key here instead of when the message
        // is put in the queue so that it can be conflated it later
        message.addStringOrObjPart(this._keyOfInterest);

        // Add the callback argument
        message.addObjPart(this._callbackArgument);

        // Add interestlist status.
        message.addObjPart(Boolean.valueOf(isClientInterested(proxyId)));

        // Add CQ status.
        message.addObjPart(Boolean.FALSE);

      } else {
        boolean isClientInterested = isClientInterested(proxyId);
        // Notify by subscription - send the value
        message = new Message(8 + cqMsgParts, clientVersion);

        // Set message type
        if (isCreate()) {
          message.setMessageType(MessageType.LOCAL_CREATE);

          // Add the region name
          message.addStringPart(this._regionName, true);

          // Add the key
          // Currently serializing the key here instead of when the message
          // is put in the queue so that it can be conflated it later
          message.addStringOrObjPart(this._keyOfInterest);

          message.addObjPart(Boolean.FALSE); // NO delta
          // Add the value (which has already been serialized)
          message.addRawPart(latestValue, (this._valueIsObject == 0x01));
        } else {
          message.setMessageType(MessageType.LOCAL_UPDATE);

          // Add the region name
          message.addStringPart(this._regionName, true);

          // Add the key
          // Currently serializing the key here instead of when the message
          // is put in the queue so that it can be conflated it later
          message.addStringOrObjPart(this._keyOfInterest);

          if (this.deltaBytes != null && !conflation && !proxy.isMarkerEnqueued()
              && !proxy.getRegionsWithEmptyDataPolicy().containsKey(_regionName)) {
            message.addObjPart(Boolean.TRUE);
            message.addBytesPart(this.deltaBytes);
            proxy.getStatistics().incDeltaMessagesSent();
          } else {
            message.addObjPart(Boolean.FALSE);
            byte[] l = latestValue;
            if (l == null) {
              if (!(this._value instanceof byte[])) {
                this._value = CacheServerHelper.serialize(this._value);
              }
              l = (byte[]) this._value;
            }
            // Add the value (which has already been serialized)
            message.addRawPart(l, (this._valueIsObject == 0x01));
          }
        }

        // Add the callback argument
        message.addObjPart(this._callbackArgument);

        // Add interest list status.
        message.addObjPart(Boolean.valueOf(isClientInterested));

        // Add CQ status.
        message.addObjPart(Boolean.valueOf(clientHasCq));

        if (clientHasCq) {
          this.addCqsToMessage(proxyId, message);
        }
      }
    } else if (isDestroy() || isInvalidate()) {
      // Destroy or invalidate event
      message = new Message(6 + cqMsgParts, clientVersion);

      if (isDestroy()) {
        message.setMessageType(MessageType.LOCAL_DESTROY);
      } else {
        message.setMessageType(MessageType.LOCAL_INVALIDATE);
      }

      message.addStringPart(this._regionName, true);

      // Currently serializing the key here instead of when the message
      // is put in the queue so that it can be conflated later
      message.addStringOrObjPart(this._keyOfInterest);

      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(isClientInterested(proxyId)));
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isDestroyRegion()) {
      message = new Message(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.LOCAL_DESTROY_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isClearRegion()) {
      message = new Message(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.CLEAR_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isInvalidateRegion()) {
      message = new Message(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.INVALIDATE_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else {
      throw new InternalGemFireError("Don't know what kind of message");
    }

    message.setTransactionId(0);
    // Add the EventId since 5.1 (used to prevent duplicate events
    // received on the client side after a failover)
    message.addObjPart(this._eventIdentifier);
    return message;
  }

  protected Message getGFE65Message(CacheClientProxy proxy, byte[] p_latestValue,
      boolean conflation, Version clientVersion) throws IOException {
    byte[] latestValue = p_latestValue;
    Message message = null;
    ClientProxyMembershipID proxyId = proxy.getProxyID();

    // Add CQ info.
    int cqMsgParts = 0;
    boolean clientHasCq = this._hasCqs && (this.getCqs(proxyId) != null);

    if (clientHasCq) {
      cqMsgParts = (this.getCqs(proxyId).length * 2) + 1;
    }

    if (isCreate() || isUpdate()) {
      // Create or update event
      if (this._clientInterestListInv != null && this._clientInterestListInv.contains(proxyId)) {
        // Client is registered for invalidates.
        if (cqMsgParts > 0) {
          cqMsgParts++; // To store base operation type for CQ.
        }

        message = new Message(6 + cqMsgParts, clientVersion);
        message.setMessageType(MessageType.LOCAL_INVALIDATE);

        // Add the region name
        message.addStringPart(this._regionName, true);

        // Add the key
        // Currently serializing the key here instead of when the message
        // is put in the queue so that it can be conflated it later
        message.addStringOrObjPart(this._keyOfInterest);
      } else {
        // Notify by subscription - send the value
        message = new Message(8 + cqMsgParts, clientVersion);

        // Set message type
        if (isCreate()) {
          message.setMessageType(MessageType.LOCAL_CREATE);

          // Add the region name
          message.addStringPart(this._regionName, true);

          // Add the key
          // Currently serializing the key here instead of when the message
          // is put in the queue so that it can be conflated it later
          message.addStringOrObjPart(this._keyOfInterest);

          message.addObjPart(Boolean.FALSE); // NO delta
          // Add the value (which has already been serialized)
          message.addRawPart(latestValue, (this._valueIsObject == 0x01));
        } else {
          message.setMessageType(MessageType.LOCAL_UPDATE);

          // Add the region name
          message.addStringPart(this._regionName, true);

          // Add the key
          // Currently serializing the key here instead of when the message
          // is put in the queue so that it can be conflated it later
          message.addStringOrObjPart(this._keyOfInterest);

          if (this.deltaBytes != null && !conflation && !proxy.isMarkerEnqueued()
              && !proxy.getRegionsWithEmptyDataPolicy().containsKey(_regionName)) {
            message.addObjPart(Boolean.TRUE);
            message.addBytesPart(this.deltaBytes);
            proxy.getStatistics().incDeltaMessagesSent();
          } else {
            message.addObjPart(Boolean.FALSE);
            if (latestValue == null) {
              if (!(this._value instanceof byte[])) {
                this._value = CacheServerHelper.serialize(this._value);
              }
              latestValue = (byte[]) this._value;
            }
            // Add the value (which has already been serialized)
            message.addRawPart(latestValue, (this._valueIsObject == 0x01));
          }
        }
      }

      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(isClientInterested(proxyId)));
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        if (message.getMessageType() == MessageType.LOCAL_INVALIDATE) {
          // in case of invalidate, set the region operation type.
          message.addIntPart(isCreate() ? MessageType.LOCAL_CREATE : MessageType.LOCAL_UPDATE);
        }
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isDestroy() || isInvalidate()) {
      if (isDestroy()) {
        message = new Message(6 + cqMsgParts, clientVersion);
        message.setMessageType(MessageType.LOCAL_DESTROY);
      } else {
        if (clientHasCq) {
          cqMsgParts++;/* To store the region operation for CQ */
        }
        message = new Message(6 + cqMsgParts, clientVersion);
        message.setMessageType(MessageType.LOCAL_INVALIDATE);
      }
      message.addStringPart(this._regionName, true);
      message.addStringOrObjPart(this._keyOfInterest);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(isClientInterested(proxyId)));
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        if (isInvalidate()) {
          // This is to take care when invalidate message is getting sent
          // to the Client. See the code for create/update operation.
          message.addIntPart(MessageType.LOCAL_INVALIDATE);
        }
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isDestroyRegion()) {
      message = new Message(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.LOCAL_DESTROY_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isClearRegion()) {
      message = new Message(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.CLEAR_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isInvalidateRegion()) {
      message = new Message(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.INVALIDATE_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);

      // Add CQ status.
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else {
      throw new InternalGemFireError("Don't know what kind of message");
    }

    message.setTransactionId(0);
    // Add the EventId since 5.1 (used to prevent duplicate events
    // received on the client side after a failover)
    message.addObjPart(this._eventIdentifier);
    return message;
  }


  protected Message getGFE70Message(CacheClientProxy proxy, byte[] p_latestValue,
      boolean conflation, Version clientVersion) throws IOException {
    byte[] latestValue = p_latestValue;
    Message message = null;
    ClientProxyMembershipID proxyId = proxy.getProxyID();
    // Add CQ info.
    int cqMsgParts = 0;
    boolean clientHasCq = this._hasCqs && (this.getCqs(proxyId) != null);

    if (clientHasCq) {
      cqMsgParts = (this.getCqs(proxyId).length * 2) + 1;
    }

    if (isCreate() || isUpdate()) {
      // Create or update event
      if (this._clientInterestListInv != null && this._clientInterestListInv.contains(proxyId)) {
        // Client is registered for invalidates.
        if (cqMsgParts > 0) {
          cqMsgParts++; // To store base operation type for CQ.
        }

        message = getMessage(7 + cqMsgParts, clientVersion);
        message.setMessageType(MessageType.LOCAL_INVALIDATE);
        message.addStringPart(this._regionName, true);
        message.addStringOrObjPart(this._keyOfInterest);
      } else {
        // Notify by subscription - send the value
        message = getMessage(9 + cqMsgParts, clientVersion);
        if (isCreate()) {
          message.setMessageType(MessageType.LOCAL_CREATE);
          message.addStringPart(this._regionName, true);
          message.addStringOrObjPart(this._keyOfInterest);
          message.addObjPart(Boolean.FALSE); // NO delta
          // Add the value (which has already been serialized)
          message.addRawPart(latestValue, (this._valueIsObject == 0x01));
        } else {
          message.setMessageType(MessageType.LOCAL_UPDATE);
          message.addStringPart(this._regionName, true);
          message.addStringOrObjPart(this._keyOfInterest);

          if (this.deltaBytes != null && !conflation && !proxy.isMarkerEnqueued()
              && !proxy.getRegionsWithEmptyDataPolicy().containsKey(_regionName)) {
            message.addObjPart(Boolean.TRUE);
            message.addBytesPart(this.deltaBytes);
            proxy.getStatistics().incDeltaMessagesSent();
          } else {
            message.addObjPart(Boolean.FALSE);
            if (latestValue == null) {
              if (!(this._value instanceof byte[])) {
                this._value = CacheServerHelper.serialize(this._value);
              }
              latestValue = (byte[]) this._value;
            }
            // Add the value (which has already been serialized)
            message.addRawPart(latestValue, (this._valueIsObject == 0x01));
          }
        }
      }

      message.addObjPart(this._callbackArgument);
      if (this.versionTag != null) {
        this.versionTag.setCanonicalIDs(proxy.getCache().getDistributionManager());
      }
      message.addObjPart(this.versionTag);
      message.addObjPart(Boolean.valueOf(isClientInterested(proxyId)));
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        if (message.getMessageType() == MessageType.LOCAL_INVALIDATE) {
          // in case of invalidate, set the region operation type.
          message.addIntPart(isCreate() ? MessageType.LOCAL_CREATE : MessageType.LOCAL_UPDATE);
        }
        this.addCqsToMessage(proxyId, message);
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
      message.addStringPart(this._regionName, true);
      message.addStringOrObjPart(this._keyOfInterest);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(this.versionTag);
      message.addObjPart(Boolean.valueOf(isClientInterested(proxyId)));
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        if (isInvalidate()) {
          // This is to take care when invalidate message is getting sent
          // to the Client. See the code for create/update operation.
          message.addIntPart(MessageType.LOCAL_INVALIDATE);
        }
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isDestroyRegion()) {
      message = getMessage(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.LOCAL_DESTROY_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isClearRegion()) {
      message = getMessage(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.CLEAR_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else if (isInvalidateRegion()) {
      message = getMessage(4 + cqMsgParts, clientVersion);
      message.setMessageType(MessageType.INVALIDATE_REGION);
      message.addStringPart(this._regionName, true);
      message.addObjPart(this._callbackArgument);

      // Add CQ status.
      message.addObjPart(Boolean.valueOf(clientHasCq));

      if (clientHasCq) {
        this.addCqsToMessage(proxyId, message);
      }
    } else {
      throw new InternalGemFireError("Don't know what kind of message");
    }

    message.setTransactionId(0);
    // Add the EventId since 5.1 (used to prevent duplicate events
    // received on the client side after a failover)
    message.addObjPart(this._eventIdentifier);
    return message;
  }

  private static final ThreadLocal<Map<Integer, Message>> CACHED_MESSAGES =
      new ThreadLocal<Map<Integer, Message>>() {
        protected Map<Integer, Message> initialValue() {
          return new HashMap<Integer, Message>();
        };
      };

  private Message getMessage(int numParts, Version clientVersion) {
    Message m = CACHED_MESSAGES.get().get(numParts);
    if (m == null) {
      m = new Message(numParts, Version.CURRENT);
      CACHED_MESSAGES.get().put(numParts, m);
    }
    m.clearParts();
    m.setVersion(clientVersion);
    return m;
  }

  /**
   * @return boolean true if the event is due to net load.
   */
  public boolean isNetLoad() {
    return this._isNetLoad;
  }

  /**
   * @param isNetLoad boolean true if the event is due to net load.
   */
  public void setIsNetLoad(boolean isNetLoad) {
    this._isNetLoad = isNetLoad;
  }


  /**
   * @return boolean true if cq info is present for the given proxy.
   */
  public boolean hasCqs(ClientProxyMembershipID clientId) {
    if (this._clientCqs != null) {
      CqNameToOp cqs = this._clientCqs.get(clientId);
      if (cqs != null && !cqs.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return boolean true if cq info is present.
   */
  public boolean hasCqs() {
    return this._hasCqs;
  }

  /**
   * Returns the cqs for the given client.
   * 
   * @return cqNames
   */
  public String[] getCqs(ClientProxyMembershipID clientId) {
    String[] cqNames = null;
    if (this._clientCqs != null) {
      CqNameToOp cqs = this._clientCqs.get(clientId);

      if (cqs != null && !cqs.isEmpty()) {
        cqNames = cqs.getNames();
      }
    }
    return cqNames;
  }

  public ClientCqConcurrentMap getClientCqs() {
    return this._clientCqs;
  }

  /**
   * Add cqs for the given client.
   * 
   * @param clientId
   * @param filteredCqs
   */
  public void addClientCqs(ClientProxyMembershipID clientId, CqNameToOp filteredCqs) {
    if (this._clientCqs == null) {
      this._clientCqs = new ClientCqConcurrentMap();
      this._hasCqs = true;
    }
    this._clientCqs.put(clientId, filteredCqs);
  }

  public void addClientCq(ClientProxyMembershipID clientId, String cqName, Integer cqEvent) {
    if (this._clientCqs == null) {
      this._clientCqs = new ClientCqConcurrentMap();
      this._hasCqs = true;
    }
    CqNameToOp cqInfo = this._clientCqs.get(clientId);
    if (cqInfo == null) {
      cqInfo = new CqNameToOpSingleEntry(cqName, cqEvent);
      this._clientCqs.put(clientId, cqInfo);
    } else if (!cqInfo.isFull()) {
      cqInfo.add(cqName, cqEvent);
    } else {
      cqInfo = new CqNameToOpHashMap((CqNameToOpSingleEntry) cqInfo);
      cqInfo.add(cqName, cqEvent);
      this._clientCqs.put(clientId, cqInfo);
    }
  }

  private void addCqsToMessage(ClientProxyMembershipID proxyId, Message message) {
    if (this._clientCqs != null) {
      CqNameToOp cqs = this._clientCqs.get(proxyId);
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
        this._clientCqs.remove(clientId);
      }
    }
  }

  /**
   * Set the region name that was updated.
   */
  public void setRegionName(String regionName) {
    this._regionName = regionName;
  }

  /**
   * @param eventId
   * @see HAEventWrapper#fromData(DataInput)
   * @see HAContainerRegion#get(Object)
   */
  public void setEventIdentifier(EventID eventId) {
    if (this._eventIdentifier == null) {
      this._eventIdentifier = eventId;
    }
  }

  /**
   * @param clientCqs
   * @see HAEventWrapper#fromData(DataInput)
   * @see HAContainerRegion#get(Object)
   */
  public void setClientCqs(ClientCqConcurrentMap clientCqs) {
    if (this._clientCqs == null) {
      this._clientCqs = clientCqs;
    }
  }

  /*
   * private void writeCqInfo(ObjectOutput out) throws IOException { // Write Client CQ Size
   * out.writeInt(this._clientCqs.size()); // For each client. Iterator entries =
   * this._clientCqs.entrySet().iterator(); while (entries.hasNext()) { Map.Entry entry =
   * (Map.Entry)entries.next();
   * 
   * // Write ProxyId. ClientProxyMembershipID proxyId = (ClientProxyMembershipID)entry.getKey();
   * proxyId.toData(out);
   * 
   * HashMap cqs = (HashMap)entry.getValue(); // Write CQ size for each Client.
   * out.writeInt(cqs.size()); Iterator clients = cqs.entrySet().iterator(); while
   * (clients.hasNext()) { Map.Entry client = (Map.Entry)clients.next(); // Write CQ Name. String cq
   * = (String)client.getKey(); out.writeObject(cq); // Write CQ OP. int cqOp =
   * ((Integer)client.getValue()).intValue(); out.writeInt(cqOp); } } // while }
   */

  /*
   * private void readCqInfo(ObjectInput in) throws IOException, ClassNotFoundException { // Read
   * Client CQ Size int numClientIds = in.readInt(); this._clientCqs = new HashMap();
   * 
   * // For each Client. for (int cCnt=0; cCnt < numClientIds; cCnt++){ ClientProxyMembershipID
   * proxyId = new ClientProxyMembershipID();
   * 
   * // Read Proxy id. proxyId.fromData(in); // read CQ size for each Client. int numCqs =
   * in.readInt(); HashMap cqs = new HashMap();
   * 
   * for (int cqCnt=0; cqCnt < numCqs; cqCnt++){ // Get CQ Name and CQ Op. // Read CQ Name. String
   * cqName = (String)in.readObject(); int cqOp = in.readInt();
   * 
   * // Read CQ Op. cqs.put(cqName, Integer.valueOf(cqOp)); } this._clientCqs.put(proxyId, cqs); } }
   */

  public void addClientInterestList(Set clientIds, boolean receiveValues) {
    if (receiveValues) {
      if (this._clientInterestList == null) {
        this._clientInterestList = clientIds;
      } else {
        this._clientInterestList.addAll(clientIds);
      }
    } else {
      if (this._clientInterestListInv == null) {
        this._clientInterestListInv = clientIds;
      } else {
        this._clientInterestListInv.addAll(clientIds);
      }
    }
  }

  public void addClientInterestList(ClientProxyMembershipID clientId, boolean receiveValues) {
    // This happens under synchronization on HAContainer.
    HashSet<ClientProxyMembershipID> newInterests;
    if (receiveValues) {
      if (this._clientInterestList == null) {
        newInterests = new HashSet<ClientProxyMembershipID>();
      } else {
        newInterests = new HashSet<ClientProxyMembershipID>(this._clientInterestList);
      }
      newInterests.add(clientId);
      this._clientInterestList = newInterests;
    } else {
      if (this._clientInterestListInv == null) {
        newInterests = new HashSet<ClientProxyMembershipID>();
      } else {
        newInterests = new HashSet<ClientProxyMembershipID>(this._clientInterestListInv);
      }
      newInterests.add(clientId);
      this._clientInterestListInv = newInterests;
    }
  }

  public boolean isClientInterested(ClientProxyMembershipID clientId) {
    return (this._clientInterestList != null && this._clientInterestList.contains(clientId))
        || (this._clientInterestListInv != null && this._clientInterestListInv.contains(clientId));
  }

  public boolean isClientInterestedInUpdates(ClientProxyMembershipID clientId) {
    return (this._clientInterestList != null && this._clientInterestList.contains(clientId));
  }

  public boolean isClientInterestedInInvalidates(ClientProxyMembershipID clientId) {
    return (this._clientInterestListInv != null && this._clientInterestListInv.contains(clientId));
  }

  protected Object deserialize(byte[] serializedBytes) {
    Object deserializedObject = serializedBytes;
    // This is a debugging method so ignore all exceptions like
    // ClassNotFoundException
    try {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serializedBytes));
      deserializedObject = DataSerializer.readObject(dis);
    } catch (Exception e) {
    }
    return deserializedObject;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("ClientUpdateMessageImpl[").append("op=").append(this._operation)
        .append(";region=").append(this._regionName).append(";key=").append(this._keyOfInterest);
    if (logger.isTraceEnabled()) {
      buffer.append(";value=").append(
          (this._value instanceof byte[]) ? deserialize((byte[]) this._value) : this._value);
    }
    buffer.append(";isObject=").append(_valueIsObject).append(";cbArg=")
        .append(this._callbackArgument).append(";memberId=").append(this._membershipId)
        .append(";eventId=").append(_eventIdentifier).append(";shouldConflate=")
        .append(_shouldConflate).append(";versionTag=").append(this.versionTag).append(";hasCqs=")
        .append(this._hasCqs)
        // skip _logger :-)
        .append("]");
    return buffer.toString();
  }

  public int getDSFID() {
    return CLIENT_UPDATE_MESSAGE;
  }

  public void toData(DataOutput out) throws IOException {
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
    // DataSerializer.writeObject(_eventIdentifier,out);
    out.writeBoolean(_shouldConflate);
    out.writeBoolean(_isInterestListPassed);
    DataSerializer.writeByteArray(this.deltaBytes, out);
    out.writeBoolean(_hasCqs);
    // if (_hasCqs) {
    // DataSerializer.writeHashMap(this._clientCqs, out);
    // }
    DataSerializer.writeObject(_callbackArgument, out);
    DataSerializer.writeHashSet((HashSet) this._clientInterestList, out);
    DataSerializer.writeHashSet((HashSet) this._clientInterestListInv, out);
    DataSerializer.writeObject(this.versionTag, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this._operation = EnumListenerEvent.getEnumListenerEvent(in.readByte());
    this._regionName = DataSerializer.readString(in);
    this._keyOfInterest = DataSerializer.readObject(in);
    this._value = DataSerializer.readByteArray(in);
    this._valueIsObject = in.readByte();
    this._membershipId = ClientProxyMembershipID.readCanonicalized(in);
    // this._eventIdentifier = (EventID)DataSerializer.readObject(in);;
    this._shouldConflate = in.readBoolean();
    this._isInterestListPassed = in.readBoolean();
    this.deltaBytes = DataSerializer.readByteArray(in);
    this._hasCqs = in.readBoolean();

    // if (this._hasCqs) {
    // this._clientCqs = DataSerializer.readHashMap(in);
    // }
    this._callbackArgument = DataSerializer.readObject(in);

    CacheClientNotifier ccn = CacheClientNotifier.getInstance();

    HashSet ids = DataSerializer.readHashSet(in);

    if (ccn != null && ids != null) { // use canonical IDs in servers
      ids = (HashSet) ccn.getProxyIDs(ids);
    }
    this._clientInterestList = ids;

    ids = DataSerializer.readHashSet(in);
    if (ccn != null && ids != null) {
      ids = (HashSet) ccn.getProxyIDs(ids);
    }
    this._clientInterestListInv = ids;

    this.versionTag = (VersionTag) DataSerializer.readObject(in);
  }

  private Object getOriginalCallbackArgument() {
    Object result = this._callbackArgument;
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

  public int getSizeInBytes() {

    int size = CONSTANT_MEMORY_OVERHEAD;

    // The value (a byte[])
    if (this._value != null) {
      size += CachedDeserializableFactory.calcMemSize(this._value);
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
  public boolean needsNoAuthorizationCheck() {
    return false;
  }

  @Override
  public CqNameToOp getClientCq(ClientProxyMembershipID clientId) {
    if (this._clientCqs != null) {
      return this._clientCqs.get(clientId);
    } else {
      return null;
    }
  }

  @Override
  public Version[] getSerializationVersions() {
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

    public ClientCqConcurrentMap() {
      super(16, 1.0f, 1);
    }
  }
  /**
   * Replaces what used to be a HashMap<String, Integer>.
   */
  public interface CqNameToOp extends Sendable {
    public boolean isEmpty();

    /**
     * Returns true if calling add would fail.
     */
    public boolean isFull();

    public void addToMessage(Message message);

    public int size();

    public String[] getNames();

    public void add(String name, Integer op);

    public void delete(String name);
  }
  /**
   * Contains either zero or one String to int tuples. This is a common case and this impl has a
   * much smaller memory footprint than a HashMap with one entry.
   */
  public static class CqNameToOpSingleEntry implements CqNameToOp {
    private String name;
    private int op;

    private static final String[] EMPTY_NAMES_ARRAY = new String[0];

    private static Map<String, String[]> NAMES_ARRAY = new ConcurrentHashMap<String, String[]>();

    public CqNameToOpSingleEntry(String name, Integer op) {
      initializeName(name);
      this.op = op.intValue();
    }

    private void initializeName(String name) {
      this.name = name;
      if (!NAMES_ARRAY.containsKey(name)) {
        NAMES_ARRAY.put(name, new String[] {name});
      }
    }

    @Override
    public void sendTo(DataOutput out) throws IOException {
      // When serialized it needs to look just as if writeObject was called on a HASH_MAP
      out.writeByte(DSCODE.HASH_MAP);
      int size = size();
      InternalDataSerializer.writeArrayLength(size, out);
      if (size > 0) {
        DataSerializer.writeObject(this.name, out);
        DataSerializer.writeObject(Integer.valueOf(this.op), out);
      }
    }

    @Override
    public boolean isEmpty() {
      return this.name == null;
    }

    @Override
    public void addToMessage(Message message) {
      if (!isEmpty()) {
        message.addStringPart(this.name, true);
        message.addIntPart(this.op);
      }
    }

    @Override
    public int size() {
      return isEmpty() ? 0 : 1;
    }

    @Override
    public String[] getNames() {
      return (isEmpty()) ? EMPTY_NAMES_ARRAY : NAMES_ARRAY.get(this.name);
    }

    @Override
    public void add(String name, Integer op) {
      if (isEmpty()) {
        this.name = name;
        this.op = op.intValue();
      } else if (this.name.equals(name)) {
        this.op = op.intValue();
      } else {
        throw new IllegalStateException("tried to add to a full CqNameToOpSingleEntry");
      }
    }

    @Override
    public void delete(String name) {
      if (name.equals(this.name)) {
        this.name = null;
      }
    }

    @Override
    public boolean isFull() {
      return !isEmpty();
    }
  }
  /**
   * Basically just a HashMap<String, Integer> but limits itself to the CqNameToOp interface.
   */
  public static class CqNameToOpHashMap extends HashMap<String, Integer> implements CqNameToOp {
    public CqNameToOpHashMap(int initialCapacity) {
      super(initialCapacity, 1.0f);
    }

    public CqNameToOpHashMap(CqNameToOpSingleEntry se) {
      super(2, 1.0f);
      add(se.name, se.op);
    }

    @Override
    public void sendTo(DataOutput out) throws IOException {
      // When serialized it needs to look just as if writeObject was called on a HASH_MAP
      out.writeByte(DSCODE.HASH_MAP);
      DataSerializer.writeHashMap(this, out);
    }

    @Override
    public String[] getNames() {
      String[] cqNames = new String[size()];
      cqNames = keySet().toArray(cqNames);
      return cqNames;
    }

    @Override
    public void addToMessage(Message message) {
      Iterator<Entry<String, Integer>> entries = entrySet().iterator();
      while (entries.hasNext()) {
        Entry<String, Integer> entry = entries.next();
        // Add CQ Name.
        String cq = entry.getKey();
        message.addStringPart(cq, true);
        // Add CQ Op.
        int op = entry.getValue().intValue();
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
      this._value = CacheServerHelper.serialize(nv);
    } catch (IOException e) {
      throw new GemFireIOException("Exception serializing entry value", e);
    }
  }

  @Override
  public void importNewBytes(byte[] nv, boolean isSerialized) {
    if (!isSerialized) {
      // The value is already a byte[]. Set _valueIsObject flag to 0x00
      // (not an object)
      this._valueIsObject = 0x00;
    }
    this._value = nv;
  }

}
