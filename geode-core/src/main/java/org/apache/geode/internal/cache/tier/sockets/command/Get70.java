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

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.GetOp;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.cache.operations.internal.GetOperationContextImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VersionTagHolder;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class Get70 extends BaseCommand {

  @Immutable
  private static final Get70 singleton = new Get70();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long startparam) throws IOException {
    long start = startparam;
    final CacheServerStats stats = serverConnection.getCacheServerStats();

    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadGetRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    final int parts = clientMessage.getNumberOfParts();
    final Part regionNamePart = clientMessage.getPart(0);
    final Part keyPart = clientMessage.getPart(1);
    // valuePart = null; (redundant assignment)
    Object callbackArg = null;
    if (parts > 2) {
      Part valuePart = clientMessage.getPart(2);
      try {
        callbackArg = valuePart.getObject();
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        // responded = true;
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    final String regionName = regionNamePart.getCachedString();
    final Object key;
    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      // responded = true;
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received 7.0 get request ({} bytes) from {} for region {} key {} txId {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), regionName, key, clientMessage.getTransactionId());
    }

    // Process the get request
    if (key == null || regionName == null) {
      final String errMessage;
      if ((key == null) && (regionName == null)) {
        errMessage =
            "The input region name and key for the get request are null.";
      } else if (key == null) {
        errMessage = "The input key for the get request is null.";
      } else {
        errMessage = "The input region name for the get request is null.";
      }
      logger.warn("{}: {}", serverConnection.getName(), errMessage);
      writeErrorResponse(clientMessage, MessageType.REQUESTDATAERROR, errMessage, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    final Region<?, ?> region = serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      String reason = " was not found during get request";
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    GetOperationContext getContext = null;
    try {
      // for integrated security
      securityService.authorize(Resource.DATA, Operation.READ, regionName, key);

      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        getContext = authzRequest.getAuthorize(regionName, key, callbackArg);
        callbackArg = getContext.getCallbackArg();
      }
    } catch (NotAuthorizedException ex) {
      writeException(clientMessage, ex, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // Get the value and update the statistics. Do not deserialize
    // the value if it is a byte[].
    Entry entry;
    try {
      entry = getEntry(region, key, callbackArg, serverConnection);
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    @Retained
    final Object originalData = entry.value;
    Object data = originalData;
    try {
      boolean isObject = entry.isObject;
      final VersionTag<?> versionTag = entry.versionTag;
      final boolean keyNotPresent = entry.keyNotPresent;

      try {
        AuthorizeRequestPP postAuthzRequest = serverConnection.getPostAuthzRequest();
        if (postAuthzRequest != null) {
          try {
            getContext = postAuthzRequest.getAuthorize(regionName, key, data, isObject, getContext);
            GetOperationContextImpl gci = (GetOperationContextImpl) getContext;
            Object newData = gci.getRawValue();
            if (newData != data) {
              // user changed the value
              isObject = getContext.isObject();
              data = newData;
            }
          } finally {
            if (getContext != null) {
              ((GetOperationContextImpl) getContext).release();
            }
          }
        }
      } catch (NotAuthorizedException ex) {
        writeException(clientMessage, ex, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }

      // post process
      data = securityService.postProcess(regionName, key, data, entry.isObject);

      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessGetTime(start - oldStart);

      if (region instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion) region;
        if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
          writeResponseWithRefreshMetadata(data, callbackArg, clientMessage, isObject,
              serverConnection, pr, pr.getNetworkHopType(), versionTag, keyNotPresent);
          pr.clearNetworkHopData();
        } else {
          writeResponse(data, callbackArg, clientMessage, isObject, versionTag, keyNotPresent,
              serverConnection);
        }
      } else {
        writeResponse(data, callbackArg, clientMessage, isObject, versionTag, keyNotPresent,
            serverConnection);
      }
    } finally {
      OffHeapHelper.release(originalData);
    }

    serverConnection.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Wrote get response back to {} for region {} {}", serverConnection.getName(),
          serverConnection.getSocketString(), regionName, entry);
    }
    stats.incWriteGetResponseTime(DistributionStats.getStatTime() - start);

    CachePerfStats regionPerfStats = ((InternalRegion) region).getRegionPerfStats();
    if (regionPerfStats != null) {
      regionPerfStats.endGetForClient(startparam, entry.keyNotPresent);
    }
  }

  /**
   * This method was added so that Get70 could, by default, call getEntryRetained, but the subclass
   * GetEntry70 could override it and call getValueAndIsObject. If we ever get to the point that no
   * code needs to call getValueAndIsObject then this method can go away.
   */
  @Retained
  protected Entry getEntry(Region<?, ?> region, Object key, Object callbackArg,
      ServerConnection servConn) {
    return getEntryRetained(region, key, callbackArg, servConn);
  }

  // take the result 3 element "result" as argument instead of
  // returning as the result to avoid creating the array repeatedly
  // for large number of entries like in getAll. Third element added in
  // 7.0 for retrieving version information
  public Entry getValueAndIsObject(Region<?, ?> region, Object key, Object callbackArg,
      ServerConnection servConn) {

    // Region.Entry entry;
    String regionName = region.getFullPath();
    if (servConn != null) {
      servConn.setModificationInfo(true, regionName, key);
    }

    final ClientProxyMembershipID id = servConn == null ? null : servConn.getProxyID();
    final VersionTagHolder versionHolder = new VersionTagHolder();
    Object data =
        ((LocalRegion) region).get(key, callbackArg, true, true, true, id, versionHolder, true);
    final VersionTag<?> versionTag = versionHolder.getVersionTag();

    // If the value in the VM is a CachedDeserializable,
    // get its value. If it is Token.REMOVED, Token.DESTROYED,
    // Token.INVALID, or Token.LOCAL_INVALID
    // set it to null. If it is NOT_AVAILABLE, get the value from
    // disk. If it is already a byte[], set isObject to false.
    boolean wasInvalid = false;
    boolean isObject = true;
    if (data instanceof CachedDeserializable) {
      CachedDeserializable cd = (CachedDeserializable) data;
      if (!cd.isSerialized()) {
        // it is a byte[]
        isObject = false;
        data = cd.getDeserializedForReading();
      } else {
        data = cd.getValue();
      }
    } else if (data == Token.REMOVED_PHASE1 || data == Token.REMOVED_PHASE2
        || data == Token.DESTROYED) {
      data = null;
    } else if (data == Token.INVALID || data == Token.LOCAL_INVALID) {
      data = null; // fix for bug 35884
      wasInvalid = true;
    } else if (data instanceof byte[]) {
      isObject = false;
    }
    boolean keyNotPresent = !wasInvalid && (data == null || data == Token.TOMBSTONE);
    return new Entry(data, isObject, keyNotPresent, versionTag);
  }

  /**
   * Same as getValueAndIsObject but the returned value can be a retained off-heap reference.
   */
  @Retained
  public Entry getEntryRetained(Region<?, ?> region, Object key, Object callbackArg,
      ServerConnection servConn) {

    // Region.Entry entry;
    String regionName = region.getFullPath();
    if (servConn != null) {
      servConn.setModificationInfo(true, regionName, key);
    }

    ClientProxyMembershipID id = servConn == null ? null : servConn.getProxyID();
    VersionTagHolder versionHolder = new VersionTagHolder();

    @Retained
    Object data =
        ((LocalRegion) region).getRetained(key, callbackArg, true, true, id, versionHolder, true);
    final VersionTag<?> versionTag = versionHolder.getVersionTag();

    // If it is Token.REMOVED, Token.DESTROYED,
    // Token.INVALID, or Token.LOCAL_INVALID
    // set it to null. If it is NOT_AVAILABLE, get the value from
    // disk. If it is already a byte[], set isObject to false.
    boolean wasInvalid = false;
    boolean isObject = true;
    if (data == Token.REMOVED_PHASE1 || data == Token.REMOVED_PHASE2 || data == Token.DESTROYED) {
      data = null;
    } else if (data == Token.INVALID || data == Token.LOCAL_INVALID) {
      data = null; // fix for bug 35884
      wasInvalid = true;
    } else if (data instanceof byte[]) {
      isObject = false;
    } else if (data instanceof CachedDeserializable) {
      CachedDeserializable cd = (CachedDeserializable) data;
      isObject = cd.isSerialized();
      if (cd.usesHeapForStorage()) {
        data = cd.getValue();
      }
    }
    boolean keyNotPresent = !wasInvalid && (data == null || data == Token.TOMBSTONE);
    return new Entry(data, isObject, keyNotPresent, versionTag);
  }

  /** this is used to return results from getValueAndIsObject */
  public static class Entry {
    public final Object value;
    public final boolean isObject;
    public final boolean keyNotPresent;
    public final VersionTag<?> versionTag;

    public Entry(Object value, boolean isObject, boolean keyNotPresent, VersionTag<?> versionTag) {
      this.value = value;
      this.isObject = isObject;
      this.keyNotPresent = keyNotPresent;
      this.versionTag = versionTag;
    }

    @Override
    public String toString() {
      return "value=" + value + " isObject=" + isObject + " notPresent=" + keyNotPresent
          + " version=" + versionTag;
    }
  }

  @Override
  protected void writeReply(@NotNull Message origMsg, @NotNull ServerConnection serverConnection)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  private void writeResponse(@Unretained Object data, Object callbackArg, Message origMsg,
      boolean isObject, VersionTag<?> versionTag, boolean keyNotPresent, ServerConnection servConn)
      throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());

    int numParts = 2;
    int flags = 0;

    if (callbackArg != null) {
      numParts++;
      flags |= GetOp.HAS_CALLBACK_ARG;
    }
    if (versionTag != null) {
      numParts++;
      flags |= GetOp.HAS_VERSION_TAG;
    }
    if (keyNotPresent) {
      flags |= GetOp.KEY_NOT_PRESENT;
    } else if (data == null && isObject) {
      flags |= GetOp.VALUE_IS_INVALID;
    }
    // logger.debug("returning flags " + Integer.toBinaryString(flags));

    responseMsg.setNumberOfParts(numParts);

    responseMsg.addPartInAnyForm(data, isObject);

    responseMsg.addIntPart(flags);


    if (callbackArg != null) {
      responseMsg.addObjPart(callbackArg);
    }
    if (versionTag != null) {
      responseMsg.addObjPart(versionTag);
    }
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(servConn);
    origMsg.clearParts();
  }

  private void writeResponseWithRefreshMetadata(@Unretained Object data, Object callbackArg,
      Message origMsg, boolean isObject, ServerConnection servConn, PartitionedRegion pr,
      byte nwHop, VersionTag<?> versionTag, boolean keyNotPresent) throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setTransactionId(origMsg.getTransactionId());

    int numParts = 3;

    int flags = 0;

    if (callbackArg != null) {
      numParts++;
      flags |= GetOp.HAS_CALLBACK_ARG;
    }
    if (versionTag != null) {
      numParts++;
      flags |= GetOp.HAS_VERSION_TAG;
    }
    if (keyNotPresent) {
      flags |= GetOp.KEY_NOT_PRESENT;
    } else if (data == null && isObject) {
      flags |= GetOp.VALUE_IS_INVALID;
    }
    // logger.debug("returning flags " + Integer.toBinaryString(flags));

    responseMsg.setNumberOfParts(numParts);

    responseMsg.addPartInAnyForm(data, isObject);

    responseMsg.addIntPart(flags);

    if (callbackArg != null) {
      responseMsg.addObjPart(callbackArg);
    }
    if (versionTag != null) {
      responseMsg.addObjPart(versionTag);
    }

    responseMsg.addBytesPart(new byte[] {pr.getMetadataVersion(), nwHop});
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    responseMsg.send(servConn);
    origMsg.clearParts();
  }

}
