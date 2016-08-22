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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.GetOperationContext;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.i18n.StringId;

public class Request extends BaseCommand {

  private final static Request singleton = new Request();

  public static Command getCommand() {
    return singleton;
  }

  Request() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    Part regionNamePart = null, keyPart = null, valuePart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    StringId errMessage = null;

    servConn.setAsTrue(REQUIRES_RESPONSE);
    // requiresResponse = true;
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadGetRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    int parts = msg.getNumberOfParts();
    regionNamePart = msg.getPart(0);
    keyPart = msg.getPart(1);
//    valuePart = null;  (redundant assignment)
    if (parts > 2) {
      valuePart = msg.getPart(2);
      try {
        callbackArg = valuePart.getObject();
      }
      catch (Exception e) {
        writeException(msg, e, false, servConn);
        // responded = true;
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getString();
    try {
      key = keyPart.getStringOrObject();
    }
    catch (Exception e) {
      writeException(msg, e, false, servConn);
      // responded = true;
      servConn.setAsTrue(RESPONDED);
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received get request ({} bytes) from {} for region {} key {} txId {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString(), regionName, key, msg.getTransactionId());
    }

    // Process the get request
    if (key == null || regionName == null) {
      if ((key == null) && (regionName == null)) {
        errMessage = LocalizedStrings.Request_THE_INPUT_REGION_NAME_AND_KEY_FOR_THE_GET_REQUEST_ARE_NULL;
      } else if (key == null) {
        errMessage = LocalizedStrings.Request_THE_INPUT_KEY_FOR_THE_GET_REQUEST_IS_NULL;   
      } else if (regionName == null) {
        errMessage = LocalizedStrings.Request_THE_INPUT_REGION_NAME_FOR_THE_GET_REQUEST_IS_NULL;
      }
      String s = errMessage.toLocalizedString();
      logger.warn("{}: {}", servConn.getName(), s);
      writeErrorResponse(msg, MessageType.REQUESTDATAERROR, s, servConn);
      // responded = true;
      servConn.setAsTrue(RESPONDED);
    }
    else {
      Region region = servConn.getCache().getRegion(regionName);
      if (region == null) {
        String reason = LocalizedStrings.Request__0_WAS_NOT_FOUND_DURING_GET_REQUEST.toLocalizedString(regionName);
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        
        GetOperationContext getContext = null;
        
          try {
            this.securityService.authorizeRegionRead(regionName, key.toString());
            AuthorizeRequest authzRequest = servConn.getAuthzRequest();
              if (authzRequest != null) {
              getContext = authzRequest
                  .getAuthorize(regionName, key, callbackArg);
              callbackArg = getContext.getCallbackArg();
            }
          }
          catch (NotAuthorizedException ex) {
            writeException(msg, ex, false, servConn);
            servConn.setAsTrue(RESPONDED);
            return;
          }

        // Get the value and update the statistics. Do not deserialize
        // the value if it is a byte[].
        Object[] valueAndIsObject = new Object[3];
        try {
          getValueAndIsObject(region, key,
              callbackArg, servConn, valueAndIsObject);
        }
        catch (Exception e) {
          writeException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }

        Object data = valueAndIsObject[0];
        boolean isObject = ((Boolean) valueAndIsObject[1]).booleanValue();

        
        
          try {
            AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
              if (postAuthzRequest != null) {
              getContext = postAuthzRequest.getAuthorize(regionName, key, data,
                  isObject, getContext);
              byte[] serializedValue = getContext.getSerializedValue();
              if (serializedValue == null) {
                data = getContext.getObject();
              }
              else {
                data = serializedValue;
              }
              isObject = getContext.isObject();
            }
          }
          catch (NotAuthorizedException ex) {
            writeException(msg, ex, false, servConn);
            servConn.setAsTrue(RESPONDED);
            return;
          }
        {
          long oldStart = start;
          start = DistributionStats.getStatTime();
          stats.incProcessGetTime(start - oldStart);
        }
        
        if (region instanceof PartitionedRegion) {
          PartitionedRegion pr = (PartitionedRegion)region;
          if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
            writeResponseWithRefreshMetadata(data, callbackArg, msg, isObject,
                servConn, pr,pr.getNetworkHopType());
            pr.clearNetworkHopData();
          }
          else {
            writeResponse(data, callbackArg, msg, isObject, servConn);
          }
        }
        else {
          writeResponse(data, callbackArg, msg, isObject, servConn);
        }
        
        servConn.setAsTrue(RESPONDED);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Wrote get response back to {} for region {} key {} value: {}", servConn.getName(), servConn.getSocketString(), regionName, key, data);
        }
        stats.incWriteGetResponseTime(DistributionStats.getStatTime() - start);
      }
    }
  }

  // take the result 2 element "result" as argument instead of
  // returning as the result to avoid creating the array repeatedly
  // for large number of entries like in getAll
  public void getValueAndIsObject(Region region, Object key,
      Object callbackArg, ServerConnection servConn,
      Object[] result) {

    String regionName = region.getFullPath();
    if (servConn != null) {
      servConn.setModificationInfo(true, regionName, key);
    }

    boolean isObject = true;
    ClientProxyMembershipID id = servConn == null ? null : servConn.getProxyID();
    Object data  = ((LocalRegion) region).get(key, callbackArg, true, true, true, id, null, false);
    
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
    }
    else if (data == Token.REMOVED_PHASE1 || data == Token.REMOVED_PHASE2 || data == Token.TOMBSTONE || data == Token.DESTROYED) {
      data = null;
    }
    else if (data == Token.INVALID || data == Token.LOCAL_INVALID) {
      data = null; // fix for bug 35884
    }
    else if (data instanceof byte[]) {
      isObject = false;
    }
            
    result[0] = data;
    result[1] = Boolean.valueOf(isObject);
    result[2] = (data == null);
  }
}
