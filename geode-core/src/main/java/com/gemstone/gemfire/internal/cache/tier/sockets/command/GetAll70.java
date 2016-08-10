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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.GetOperationContext;
import com.gemstone.gemfire.cache.operations.internal.GetOperationContextImpl;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ObjectPartList;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.internal.security.SecurityService;
import com.gemstone.gemfire.security.NotAuthorizedException;

public class GetAll70 extends BaseCommand {

  private final static GetAll70 singleton = new GetAll70();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start) throws IOException, InterruptedException {
    Part regionNamePart = null, keysPart = null;
    String regionName = null;
    Object[] keys = null;
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    int partIdx = 0;

    // Retrieve the region name from the message parts
    regionNamePart = msg.getPart(partIdx++);
    regionName = regionNamePart.getString();

    // Retrieve the keys array from the message parts
    keysPart = msg.getPart(partIdx++);
    try {
      keys = (Object[]) keysPart.getObject();
    } catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    boolean requestSerializedValues;
    requestSerializedValues = msg.getPart(partIdx++).getInt() == 1;

    if (logger.isDebugEnabled()) {
      StringBuffer buffer = new StringBuffer();
      buffer.append(servConn.getName())
            .append(": Received getAll request (")
            .append(msg.getPayloadLength())
            .append(" bytes) from ")
            .append(servConn.getSocketString())
            .append(" for region ")
            .append(regionName)
            .append(" keys ");
      if (keys != null) {
        for (int i = 0; i < keys.length; i++) {
          buffer.append(keys[i]).append(" ");
        }
      } else {
        buffer.append("NULL");
      }
      logger.debug(buffer.toString());
    }

    // Process the getAll request
    if (regionName == null) {
      String message = null;
      //      if (regionName == null) (can only be null)
      {
        message = LocalizedStrings.GetAll_THE_INPUT_REGION_NAME_FOR_THE_GETALL_REQUEST_IS_NULL.toLocalizedString();
      }
      logger.warn("{}: {}", servConn.getName(), message);
      writeChunkedErrorResponse(msg, MessageType.GET_ALL_DATA_ERROR, message, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) servConn.getCache().getRegion(regionName);
    if (region == null) {
      String reason = " was not found during getAll request";
      writeRegionDestroyedEx(msg, regionName, reason, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    // Send header
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    chunkedResponseMsg.setMessageType(MessageType.RESPONSE);
    chunkedResponseMsg.setTransactionId(msg.getTransactionId());
    chunkedResponseMsg.sendHeader();

    // Send chunk response
    try {
      fillAndSendGetAllResponseChunks(region, regionName, keys, servConn, requestSerializedValues);
      servConn.setAsTrue(RESPONDED);
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(servConn, e);

      // Otherwise, write an exception message and continue
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
  }

  private void fillAndSendGetAllResponseChunks(Region region,
                                               String regionName,
                                               Object[] keys,
                                               ServerConnection servConn,
                                               boolean requestSerializedValues) throws IOException {

    // Interpret null keys object as a request to get all key,value entry pairs
    // of the region; otherwise iterate each key and perform the get behavior.
    Iterator allKeysIter;
    int numKeys;
    if (keys != null) {
      allKeysIter = null;
      numKeys = keys.length;
    } else {
      Set allKeys = region.keySet();
      allKeysIter = allKeys.iterator();
      numKeys = allKeys.size();
    }
    // Shouldn't it be 'keys != null' below?
    // The answer is no.
    // Note that the current implementation of client/server getAll the "keys" will always be non-null.
    // The server callects and returns the values in the same order as the keys it received.
    // So the server does not need to send the keys back to the client.
    // When the client receives the server's "values" it calls setKeys using the key list the client already has.
    // So the only reason we would tell the VersionedObjectList that it needs to track keys is if we are running
    // in the old mode (which may be impossible since we only used that mode pre 7.0) in which the client told us
    // to get and return all the keys and values. I think this was used for register interest.
    VersionedObjectList values = new VersionedObjectList(maximumChunkSize, keys == null, region.getAttributes()
                                                                                               .getConcurrencyChecksEnabled(), requestSerializedValues);
    try {
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
      Get70 request = (Get70) Get70.getCommand();
      final boolean isDebugEnabled = logger.isDebugEnabled();
      for (int i = 0; i < numKeys; i++) {
        // Send the intermediate chunk if necessary
        if (values.size() == maximumChunkSize) {
          // Send the chunk and clear the list
          values.setKeys(null);
          sendGetAllResponseChunk(region, values, false, servConn);
          values.clear();
        }

        Object key;
        boolean keyNotPresent = false;
        if (keys != null) {
          key = keys[i];
        } else {
          key = allKeysIter.next();
        }
        if (isDebugEnabled) {
          logger.debug("{}: Getting value for key={}", servConn.getName(), key);
        }
        // Determine if the user authorized to get this key
        GetOperationContext getContext = null;
        if (authzRequest != null) {
          try {
            getContext = authzRequest.getAuthorize(regionName, key, null);
            if (isDebugEnabled) {
              logger.debug("{}: Passed GET pre-authorization for key={}", servConn.getName(), key);
            }
          } catch (NotAuthorizedException ex) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.GetAll_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1, new Object[] {
              servConn.getName(),
              key
            }), ex);
            values.addExceptionPart(key, ex);
            continue;
          }
        }

        try {
          this.securityService.authorizeRegionRead(regionName, key.toString());
        } catch (NotAuthorizedException ex) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.GetAll_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1, new Object[] {
            servConn.getName(),
            key
          }), ex);
          values.addExceptionPart(key, ex);
          continue;
        }

        // Get the value and update the statistics. Do not deserialize
        // the value if it is a byte[].
        // Getting a value in serialized form is pretty nasty. I split this out
        // so the logic can be re-used by the CacheClientProxy.
        Get70.Entry entry = request.getEntry(region, key, null, servConn);
        @Retained
        final Object originalData = entry.value;
        Object data = originalData;
        if (logger.isDebugEnabled()) {
          logger.debug("retrieved key={} {}", key, entry);
        }
        boolean addedToValues = false;
        try {
          boolean isObject = entry.isObject;
          VersionTag versionTag = entry.versionTag;
          keyNotPresent = entry.keyNotPresent;

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
            } catch (NotAuthorizedException ex) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.GetAll_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1, new Object[] {
                servConn.getName(),
                key
              }), ex);
              values.addExceptionPart(key, ex);
              continue;
            } finally {
              if (getContext != null) {
                ((GetOperationContextImpl) getContext).release();
              }
            }
          }

          data = this.securityService.postProcess(regionName, key, data, entry.isObject);

          // Add the entry to the list that will be returned to the client
          if (keyNotPresent) {
            values.addObjectPartForAbsentKey(key, data, versionTag);
            addedToValues = true;
          } else {
            values.addObjectPart(key, data, isObject, versionTag);
            addedToValues = true;
          }
        } finally {
          if (!addedToValues || data != originalData) {
            OffHeapHelper.release(originalData);
          }
        }
      }

      // Send the last chunk even if the list is of zero size.
      if (Version.GFE_701.compareTo(servConn.getClientVersion()) <= 0) {
        // 7.0.1 and later clients do not expect the keys in the response
        values.setKeys(null);
      }
      sendGetAllResponseChunk(region, values, true, servConn);
      servConn.setAsTrue(RESPONDED);
    } finally {
      values.release();
    }
  }


  private static void sendGetAllResponseChunk(Region region,
                                              ObjectPartList list,
                                              boolean lastChunk,
                                              ServerConnection servConn) throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPartNoCopying(list);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} getAll response chunk for region={}{}", servConn.getName(), (lastChunk ? " last " : " "), region
        .getFullPath(), (logger.isTraceEnabled() ? " values=" + list + " chunk=<" + chunkedResponseMsg + ">" : ""));
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

}
