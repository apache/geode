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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class Request extends BaseCommand {

  private static final Request singleton = new Request();

  public static Command getCommand() {
    return singleton;
  }

  Request() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    Part regionNamePart = null, keyPart = null, valuePart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    CacheServerStats stats = serverConnection.getCacheServerStats();
    String errMessage = null;

    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    // requiresResponse = true;
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadGetRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    int parts = clientMessage.getNumberOfParts();
    regionNamePart = clientMessage.getPart(0);
    keyPart = clientMessage.getPart(1);
    // valuePart = null; (redundant assignment)
    if (parts > 2) {
      valuePart = clientMessage.getPart(2);
      try {
        callbackArg = valuePart.getObject();
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        // responded = true;
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getString();
    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(clientMessage, e, false, serverConnection);
      // responded = true;
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received get request ({} bytes) from {} for region {} key {} txId {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), regionName, key, clientMessage.getTransactionId());
    }

    // Process the get request
    if (key == null || regionName == null) {
      if ((key == null) && (regionName == null)) {
        errMessage =
            "The input region name and key for the get request are null.";
      } else if (key == null) {
        errMessage = "The input key for the get request is null.";
      } else if (regionName == null) {
        errMessage = "The input region name for the get request is null.";
      }
      logger.warn("{}: {}", serverConnection.getName(), errMessage);
      writeErrorResponse(clientMessage, MessageType.REQUESTDATAERROR, errMessage, serverConnection);
      // responded = true;
      serverConnection.setAsTrue(RESPONDED);
    } else {
      Region region = serverConnection.getCache().getRegion(regionName);
      if (region == null) {
        String reason = String.format("%s was not found during get request",
            regionName);
        writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
      } else {

        GetOperationContext getContext = null;

        try {
          securityService.authorize(Resource.DATA, Operation.READ, regionName, key.toString());
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
        Object[] valueAndIsObject = new Object[3];
        try {
          getValueAndIsObject(region, key, callbackArg, serverConnection, valueAndIsObject);
        } catch (Exception e) {
          writeException(clientMessage, e, false, serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }

        Object data = valueAndIsObject[0];
        boolean isObject = ((Boolean) valueAndIsObject[1]).booleanValue();



        try {
          AuthorizeRequestPP postAuthzRequest = serverConnection.getPostAuthzRequest();
          if (postAuthzRequest != null) {
            getContext = postAuthzRequest.getAuthorize(regionName, key, data, isObject, getContext);
            byte[] serializedValue = getContext.getSerializedValue();
            if (serializedValue == null) {
              data = getContext.getObject();
            } else {
              data = serializedValue;
            }
            isObject = getContext.isObject();
          }
        } catch (NotAuthorizedException ex) {
          writeException(clientMessage, ex, false, serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }
        {
          long oldStart = start;
          start = DistributionStats.getStatTime();
          stats.incProcessGetTime(start - oldStart);
        }

        if (region instanceof PartitionedRegion) {
          PartitionedRegion pr = (PartitionedRegion) region;
          if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
            writeResponseWithRefreshMetadata(data, callbackArg, clientMessage, isObject,
                serverConnection, pr, pr.getNetworkHopType());
            pr.clearNetworkHopData();
          } else {
            writeResponse(data, callbackArg, clientMessage, isObject, serverConnection);
          }
        } else {
          writeResponse(data, callbackArg, clientMessage, isObject, serverConnection);
        }

        serverConnection.setAsTrue(RESPONDED);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Wrote get response back to {} for region {} key {} value: {}",
              serverConnection.getName(), serverConnection.getSocketString(), regionName, key,
              data);
        }
        stats.incWriteGetResponseTime(DistributionStats.getStatTime() - start);
      }
    }
  }

  // take the result 2 element "result" as argument instead of
  // returning as the result to avoid creating the array repeatedly
  // for large number of entries like in getAll
  public void getValueAndIsObject(Region region, Object key, Object callbackArg,
      ServerConnection servConn, Object[] result) {

    String regionName = region.getFullPath();
    if (servConn != null) {
      servConn.setModificationInfo(true, regionName, key);
    }

    boolean isObject = true;
    ClientProxyMembershipID id = servConn == null ? null : servConn.getProxyID();
    Object data = ((LocalRegion) region).get(key, callbackArg, true, true, true, id, null, false);

    // If the value in the VM is a CachedDeserializable,
    // get its value. If it is Token.REMOVED, Token.DESTROYED,
    // Token.INVALID, or Token.LOCAL_INVALID
    // set it to null. If it is NOT_AVAILABLE, get the value from
    // disk. If it is already a byte[], set isObject to false.
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
        || data == Token.TOMBSTONE || data == Token.DESTROYED) {
      data = null;
    } else if (data == Token.INVALID || data == Token.LOCAL_INVALID) {
      data = null; // fix for bug 35884
    } else if (data instanceof byte[]) {
      isObject = false;
    }

    result[0] = data;
    result[1] = Boolean.valueOf(isObject);
    result[2] = (data == null);
  }
}
