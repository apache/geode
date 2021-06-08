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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.geode.InvalidDeltaException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.ResourceException;
import org.apache.geode.cache.client.internal.PutOp;
import org.apache.geode.cache.operations.PutOperationContext;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.ResourcePermission;

public class Put70 extends BaseCommand {

  @Immutable
  private static final Put70 singleton = new Put70();

  public static Command getCommand() {
    return singleton;
  }

  private Put70() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long p_start)
      throws IOException, InterruptedException {
    long start = p_start;
    final CacheServerStats stats = serverConnection.getCacheServerStats();

    // requiresResponse = true;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadPutRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    int idx = 0;

    final Part regionNamePart = clientMessage.getPart(idx++);

    final Operation operation;
    try {
      final Part operationPart = clientMessage.getPart(idx++);

      if (operationPart.isBytes()) {
        final byte[] bytes = operationPart.getSerializedForm();
        if (null == bytes || 0 == bytes.length) {
          // older clients can send empty bytes for default operation.
          operation = Operation.UPDATE;
        } else {
          operation = Operation.fromOrdinal(bytes[0]);
        }
      } else {

        // Fallback for older clients.
        if (operationPart.getObject() == null) {
          // native clients may send a null since the op is java-serialized.
          operation = Operation.UPDATE;
        } else {
          operation = (Operation) operationPart.getObject();
        }
      }
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final int flags = clientMessage.getPart(idx++).getInt();
    final boolean requireOldValue = ((flags & 0x01) == 0x01);
    final boolean haveExpectedOldValue = ((flags & 0x02) == 0x02);
    final Object expectedOldValue;
    if (haveExpectedOldValue) {
      try {
        expectedOldValue = clientMessage.getPart(idx++).getObject();
      } catch (ClassNotFoundException e) {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    } else {
      expectedOldValue = null;
    }

    final Part keyPart = clientMessage.getPart(idx++);

    final boolean isDelta;
    try {
      isDelta = ((Boolean) clientMessage.getPart(idx).getObject());
      idx += 1;
    } catch (Exception e) {
      writeException(clientMessage, MessageType.PUT_DELTA_ERROR, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      // CachePerfStats not available here.
      return;
    }

    final Part valuePart = clientMessage.getPart(idx++);
    final Part eventPart = clientMessage.getPart(idx++);

    Object callbackArg = null;
    if (clientMessage.getNumberOfParts() > idx) {
      final Part callbackArgPart = clientMessage.getPart(idx++);
      try {
        callbackArg = callbackArgPart.getObject();
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }

    final Object key;
    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final String regionName = regionNamePart.getCachedString();

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug(
          "{}: Received {}put request ({} bytes) from {} for region {} key {} txId {} posdup: {}",
          serverConnection.getName(), (isDelta ? " delta " : " "), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), regionName, key, clientMessage.getTransactionId(),
          clientMessage.isRetry());
    }

    // Process the put request
    if (key == null || regionName == null) {
      final StringBuilder errMessage = new StringBuilder();
      if (key == null) {
        final String putMsg = " The input key for the put request is null";
        if (isDebugEnabled) {
          logger.debug("{}:{}", serverConnection.getName(), putMsg);
        }
        errMessage.append(putMsg);
      }
      if (regionName == null) {
        final String putMsg = " The input region name for the put request is null";
        if (isDebugEnabled) {
          logger.debug("{}:{}", serverConnection.getName(), putMsg);
        }
        errMessage.append(putMsg);
      }
      writeErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR, errMessage.toString(),
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      final String reason = " was not found during put request";
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    if (valuePart.isNull() && operation != Operation.PUT_IF_ABSENT && region.containsKey(key)) {
      // Invalid to 'put' a null value in an existing key
      final String putMsg = " Attempted to put a null value for existing key " + key;
      if (isDebugEnabled) {
        logger.debug("{}:{}", serverConnection.getName(), putMsg);
      }
      writeErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR, putMsg,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
    final long threadId = EventID.readEventIdPartsFromOptimizedByteArray(eventIdPartsBuffer);
    final long sequenceId = EventID.readEventIdPartsFromOptimizedByteArray(eventIdPartsBuffer);

    final EventIDHolder clientEvent = new EventIDHolder(
        new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId));

    Breadcrumbs.setEventId(clientEvent.getEventId());

    // msg.isRetry might be set by v7.0 and later clients
    if (clientMessage.isRetry()) {
      if (shouldSetPossibleDuplicate(region, clientEvent)) {
        clientEvent.setPossibleDuplicate(true);
      }
    }

    boolean sendOldValue = false;
    boolean oldValueIsObject = true;
    Object oldValue = null;

    try {
      Object value = null;
      if (!isDelta) {
        value = valuePart.getSerializedForm();
      }
      boolean isObject = valuePart.isObject();
      boolean isMetaRegion = region.isUsedForMetaRegion();
      clientMessage.setMetaRegion(isMetaRegion);

      securityService.authorize(ResourcePermission.Resource.DATA,
          ResourcePermission.Operation.WRITE, regionName,
          key);

      AuthorizeRequest authzRequest = null;
      if (!isMetaRegion) {
        authzRequest = serverConnection.getAuthzRequest();
      }
      if (authzRequest != null) {
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          authzRequest.createRegionAuthorize((String) key);
        }
        // Allow PUT operations on meta regions (bug #38961)
        else {
          PutOperationContext putContext =
              authzRequest.putAuthorize(regionName, key, value, isObject, callbackArg);
          value = putContext.getValue();
          isObject = putContext.isObject();
          callbackArg = putContext.getCallbackArg();
        }
      }
      if (isDebugEnabled) {
        logger.debug("processing put65 with operation={}", operation);
      }

      // If the value is 1 byte and the byte represents null,
      // attempt to create the entry. This test needs to be
      // moved to DataSerializer or DataSerializer.NULL needs
      // to be publicly accessible.
      boolean result;
      if (operation == Operation.PUT_IF_ABSENT) {
        // try {
        if (clientMessage.isRetry() && clientEvent.getVersionTag() != null) {
          // bug #46590 the operation was successful the last time since it
          // was applied to the cache, so return success and the recovered
          // version tag
          if (isDebugEnabled) {
            logger.debug("putIfAbsent operation was successful last time with version {}",
                clientEvent.getVersionTag());
          }
          // invoke basicBridgePutIfAbsent anyway to ensure that the event is distributed to all
          // servers - bug #51664
          region.basicBridgePutIfAbsent(key, value, isObject, callbackArg,
              serverConnection.getProxyID(), true, clientEvent);
          oldValue = null;
        } else {
          oldValue = region.basicBridgePutIfAbsent(key, value, isObject, callbackArg,
              serverConnection.getProxyID(), true, clientEvent);
        }
        sendOldValue = true;
        oldValueIsObject = true;
        if (oldValue instanceof CachedDeserializable) {
          oldValue = ((CachedDeserializable) oldValue).getSerializedValue();
        } else if (oldValue instanceof byte[]) {
          oldValueIsObject = false;
        }
        result = true;
        // } catch (Exception e) {
        // writeException(msg, e, false, servConn);
        // servConn.setAsTrue(RESPONDED);
        // return;
        // }

      } else if (operation == Operation.REPLACE) {
        // try {
        if (requireOldValue) { // <V> replace(<K>, <V>)
          if (clientMessage.isRetry() && clientEvent.isConcurrencyConflict()
              && clientEvent.getVersionTag() != null) {
            if (isDebugEnabled) {
              logger.debug("replace(k,v) operation was successful last time with version {}",
                  clientEvent.getVersionTag());
            }
          }
          oldValue = region.basicBridgeReplace(key, value, isObject, callbackArg,
              serverConnection.getProxyID(), true, clientEvent);
          sendOldValue = !clientEvent.isConcurrencyConflict();
          oldValueIsObject = true;
          if (oldValue instanceof CachedDeserializable) {
            oldValue = ((CachedDeserializable) oldValue).getSerializedValue();
          } else if (oldValue instanceof byte[]) {
            oldValueIsObject = false;
          }
          if (isDebugEnabled) {
            logger.debug("returning {} from replace(K,V)", oldValue);
          }
          result = true;
        } else { // boolean replace(<K>, <V>, <V>) {
          boolean didPut;
          didPut = region.basicBridgeReplace(key, expectedOldValue, value, isObject, callbackArg,
              serverConnection.getProxyID(), true, clientEvent);
          if (clientMessage.isRetry() && clientEvent.getVersionTag() != null) {
            if (isDebugEnabled) {
              logger.debug("replace(k,v,v) operation was successful last time with version {}",
                  clientEvent.getVersionTag());
            }
            didPut = true;
          }
          sendOldValue = true;
          oldValueIsObject = true;
          oldValue = didPut ? Boolean.TRUE : Boolean.FALSE;
          if (isDebugEnabled) {
            logger.debug("returning {} from replace(K,V,V)", oldValue);
          }
          result = true;
        }
        // } catch (Exception e) {
        // writeException(msg, e, false, servConn);
        // servConn.setAsTrue(RESPONDED);
        // return;
        // }

      } else if (value == null && !isDelta) {
        // Create the null entry. Since the value is null, the value of the
        // isObject
        // the true after null doesn't matter and is not used.
        result = region.basicBridgeCreate(key, null, true, callbackArg,
            serverConnection.getProxyID(), true, clientEvent, false);
        if (clientMessage.isRetry() && clientEvent.isConcurrencyConflict()
            && clientEvent.getVersionTag() != null) {
          result = true;
          if (isDebugEnabled) {
            logger.debug("create(k,null) operation was successful last time with version {}",
                clientEvent.getVersionTag());
          }
        }
      } else {
        // Put the entry
        byte[] delta = null;
        if (isDelta) {
          delta = valuePart.getSerializedForm();
        }
        TXManagerImpl txMgr =
            (TXManagerImpl) serverConnection.getCache().getCacheTransactionManager();
        // bug 43068 - use create() if in a transaction and op is CREATE
        if (txMgr.getTXState() != null && operation.isCreate()) {
          result = region.basicBridgeCreate(key, (byte[]) value, isObject, callbackArg,
              serverConnection.getProxyID(), true, clientEvent, true);
        } else {
          result = region.basicBridgePut(key, value, delta, isObject, callbackArg,
              serverConnection.getProxyID(), true, clientEvent, true);
        }
        if (clientMessage.isRetry() && clientEvent.isConcurrencyConflict()
            && clientEvent.getVersionTag() != null) {
          if (isDebugEnabled) {
            logger.debug("put(k,v) operation was successful last time with version {}",
                clientEvent.getVersionTag());
          }
          result = true;
        }
      }
      if (result) {
        serverConnection.setModificationInfo(true, regionName, key);
      } else {
        String message = serverConnection.getName() + ": Failed to put entry for region "
            + regionName + " key " + key + " value " + valuePart;
        if (isDebugEnabled) {
          logger.debug(message);
        }
        throw new Exception(message);
      }
    } catch (RegionDestroyedException | ResourceException rde) {
      writeException(clientMessage, rde, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (InvalidDeltaException ide) {
      logger.info("Error applying delta for key {} of region {}: {}",
          new Object[] {key, regionName, ide.getMessage()});
      writeException(clientMessage, MessageType.PUT_DELTA_ERROR, ide, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      region.getCachePerfStats().incDeltaFullValuesRequested();
      return;
    } catch (Exception ce) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, ce);

      // If an exception occurs during the put, preserve the connection
      writeException(clientMessage, ce, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      if (ce instanceof GemFireSecurityException) {
        // Fine logging for security exceptions since these are already
        // logged by the security logger
        if (isDebugEnabled) {
          logger.debug("{}: Unexpected Security exception", serverConnection.getName(), ce);
        }
      } else if (isDebugEnabled) {
        logger.debug("{}: Unexpected Exception", serverConnection.getName(), ce);
      }
      return;
    } finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessPutTime(start - oldStart);
    }

    // Increment statistics and write the reply
    if (region instanceof PartitionedRegion) {
      PartitionedRegion pr = (PartitionedRegion) region;
      if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
        writeReplyWithRefreshMetadata(clientMessage, serverConnection, pr, sendOldValue,
            oldValueIsObject, oldValue, pr.getNetworkHopType(), clientEvent.getVersionTag());
        pr.clearNetworkHopData();
      } else {
        writeReply(clientMessage, serverConnection, sendOldValue, oldValueIsObject, oldValue,
            clientEvent.getVersionTag());
      }
    } else {
      writeReply(clientMessage, serverConnection, sendOldValue, oldValueIsObject, oldValue,
          clientEvent.getVersionTag());
    }
    serverConnection.setAsTrue(RESPONDED);
    if (isDebugEnabled) {
      logger.debug("{}: Sent put response back to {} for region {} key {} value {}",
          serverConnection.getName(), serverConnection.getSocketString(), regionName, key,
          valuePart);
    }
    stats.incWritePutResponseTime(DistributionStats.getStatTime() - start);


  }

  boolean shouldSetPossibleDuplicate(LocalRegion region, EventIDHolder clientEvent) {
    boolean shouldSetPossibleDuplicate = true;
    if (region.getAttributes().getConcurrencyChecksEnabled()) {
      // recover the version tag from other servers
      clientEvent.setRegion(region);
      boolean withPersistence = region.getAttributes().getDataPolicy().withPersistence();
      if (!recoverVersionTagForRetriedOperation(clientEvent) && !withPersistence) {
        // For persistent region, it is possible that all persistent copies went offline.
        // Do not reset possible duplicate in this case, as persistent data
        // can be recovered during the retry after recover of version tag failed.
        shouldSetPossibleDuplicate = false; // no-one has seen this event
      }
    }
    return shouldSetPossibleDuplicate;
  }

  protected void writeReply(Message origMsg, ServerConnection servConn, boolean sendOldValue,
      boolean oldValueIsObject, Object oldValue, VersionTag<?> versionTag) throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int parts = 2;
    if (sendOldValue) {
      flags |= PutOp.HAS_OLD_VALUE_FLAG;
      if (oldValueIsObject) {
        flags |= PutOp.OLD_VALUE_IS_OBJECT_FLAG;
      }
      parts++;
    }
    if (versionTag != null) {
      flags |= PutOp.HAS_VERSION_TAG;
      parts++;
    }
    replyMsg.setNumberOfParts(parts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(okBytes());
    replyMsg.addIntPart(flags);
    if (sendOldValue) {
      replyMsg.addObjPart(oldValue);
    }
    if (versionTag != null) {
      replyMsg.addObjPart(versionTag);
    }
    replyMsg.send(servConn);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {} parts={}", servConn.getName(), origMsg.getTransactionId(),
          replyMsg.getNumberOfParts());
    }
  }

  protected void writeReplyWithRefreshMetadata(Message origMsg, ServerConnection servConn,
      PartitionedRegion pr, boolean sendOldValue, boolean oldValueIsObject, Object oldValue,
      byte nwHopType, VersionTag<?> versionTag) throws IOException {
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    int flags = 0;
    int parts = 1;
    parts++; // flags
    if (sendOldValue) {
      flags |= PutOp.HAS_OLD_VALUE_FLAG;
      if (oldValueIsObject) {
        flags |= PutOp.OLD_VALUE_IS_OBJECT_FLAG;
      }
      parts++;
    }
    if (versionTag != null) {
      flags |= PutOp.HAS_VERSION_TAG;
      parts++;
    }
    replyMsg.setNumberOfParts(parts);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(new byte[] {pr.getMetadataVersion(), nwHopType});
    replyMsg.addIntPart(flags);
    if (sendOldValue) {
      // if (logger.fineEnabled()) {
      // logger.fine("sending old value in Put response");
      // }
      replyMsg.addObjPart(oldValue);
    }
    if (versionTag != null) {
      replyMsg.addObjPart(versionTag);
    }
    replyMsg.send(servConn);
    pr.getPrStats().incPRMetaDataSentCount();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADATA tx: {} parts={}", servConn.getName(),
          origMsg.getTransactionId(), replyMsg.getNumberOfParts());
    }
  }
}
