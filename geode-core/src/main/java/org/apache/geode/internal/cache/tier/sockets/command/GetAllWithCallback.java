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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.cache.operations.internal.GetOperationContextImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ObjectPartList;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.OffHeapHelper;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * Initial version copied from GetAll70.java r48777.
 */
public class GetAllWithCallback extends BaseCommand {
  private static final Logger logger = LogService.getLogger();

  private static final GetAllWithCallback singleton = new GetAllWithCallback();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Part regionNamePart = null, keysPart = null, callbackPart = null;
    String regionName = null;
    Object[] keys = null;
    Object callback = null;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    int partIdx = 0;

    // Retrieve the region name from the message parts
    regionNamePart = clientMessage.getPart(partIdx++);
    regionName = regionNamePart.getString();

    // Retrieve the keys array from the message parts
    keysPart = clientMessage.getPart(partIdx++);
    try {
      keys = (Object[]) keysPart.getObject();
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    callbackPart = clientMessage.getPart(partIdx++);
    try {
      callback = callbackPart.getObject();
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    if (logger.isDebugEnabled()) {
      StringBuilder buffer = new StringBuilder();
      buffer.append(serverConnection.getName()).append(": Received getAll request (")
          .append(clientMessage.getPayloadLength()).append(" bytes) from ")
          .append(serverConnection.getSocketString()).append(" for region ").append(regionName)
          .append(" with callback ").append(callback).append(" keys ");
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
      // if (regionName == null) (can only be null)
      {
        message = "The input region name for the getAll request is null";
      }
      logger.warn("{} : {}",
          new Object[] {serverConnection.getName(), message});
      writeChunkedErrorResponse(clientMessage, MessageType.GET_ALL_DATA_ERROR, message,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    LocalRegion region = (LocalRegion) serverConnection.getCache().getRegion(regionName);
    if (region == null) {
      String reason = " was not found during getAll request";
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    // Send header
    ChunkedMessage chunkedResponseMsg = serverConnection.getChunkedResponseMessage();
    chunkedResponseMsg.setMessageType(MessageType.RESPONSE);
    chunkedResponseMsg.setTransactionId(clientMessage.getTransactionId());
    chunkedResponseMsg.sendHeader();

    // Send chunk response
    try {
      fillAndSendGetAllResponseChunks(region, regionName, keys, serverConnection, callback,
          securityService);
      serverConnection.setAsTrue(RESPONDED);
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, e);

      // Otherwise, write an exception message and continue
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
  }

  private void fillAndSendGetAllResponseChunks(Region region, String regionName, Object[] keys,
      ServerConnection servConn, Object callback, SecurityService securityService)
      throws IOException {

    assert keys != null;
    int numKeys = keys.length;
    VersionedObjectList values = new VersionedObjectList(MAXIMUM_CHUNK_SIZE, false,
        region.getAttributes().getConcurrencyChecksEnabled(), false);
    try {
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
      Get70 request = (Get70) Get70.getCommand();
      for (int i = 0; i < numKeys; i++) {
        // Send the intermediate chunk if necessary
        if (values.size() == MAXIMUM_CHUNK_SIZE) {
          // Send the chunk and clear the list
          sendGetAllResponseChunk(region, values, false, servConn);
          values.clear();
        }

        Object key;
        boolean keyNotPresent = false;
        key = keys[i];
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Getting value for key={}", servConn.getName(), key);
        }
        // Determine if the user authorized to get this key
        GetOperationContext getContext = null;
        if (authzRequest != null) {
          try {
            getContext = authzRequest.getAuthorize(regionName, key, callback);
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Passed GET pre-authorization for key={}", servConn.getName(), key);
            }
          } catch (NotAuthorizedException ex) {
            logger.warn(String.format(
                "%s: Caught the following exception attempting to get value for key=%s",
                new Object[] {servConn.getName(), key}),
                ex);
            values.addExceptionPart(key, ex);
            continue;
          }
        }

        try {
          securityService.authorize(Resource.DATA, Operation.READ, regionName, key.toString());
        } catch (NotAuthorizedException ex) {
          logger.warn(
              String.format("%s: Caught the following exception attempting to get value for key=%s",
                  new Object[] {servConn.getName(), key}),
              ex);
          values.addExceptionPart(key, ex);
          continue;
        }

        // Get the value and update the statistics. Do not deserialize
        // the value if it is a byte[].
        // Getting a value in serialized form is pretty nasty. I split this out
        // so the logic can be re-used by the CacheClientProxy.
        Get70.Entry entry = request.getEntry(region, key, callback, servConn);
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
              getContext =
                  postAuthzRequest.getAuthorize(regionName, key, data, isObject, getContext);
              GetOperationContextImpl gci = (GetOperationContextImpl) getContext;
              Object newData = gci.getRawValue();
              if (newData != data) {
                // user changed the value
                isObject = getContext.isObject();
                data = newData;
              }
            } catch (NotAuthorizedException ex) {
              logger.warn(String.format(
                  "%s: Caught the following exception attempting to get value for key=%s",
                  new Object[] {servConn.getName(), key}),
                  ex);
              values.addExceptionPart(key, ex);
              continue;
            } finally {
              if (getContext != null) {
                ((GetOperationContextImpl) getContext).release();
              }
            }
          }
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
      sendGetAllResponseChunk(region, values, true, servConn);
      servConn.setAsTrue(RESPONDED);
    } finally {
      values.release();
    }
  }


  private static void sendGetAllResponseChunk(Region region, ObjectPartList list, boolean lastChunk,
      ServerConnection servConn) throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPartNoCopying(list);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} getAll response chunk for region={}{}", servConn.getName(),
          (lastChunk ? " last " : " "), region.getFullPath(), (logger.isTraceEnabled()
              ? " values=" + list + " chunk=<" + chunkedResponseMsg + ">" : ""));
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

}
