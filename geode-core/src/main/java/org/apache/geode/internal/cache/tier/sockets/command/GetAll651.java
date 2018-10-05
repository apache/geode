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
import java.util.Iterator;
import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ObjectPartList651;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.AuthorizeRequestPP;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class GetAll651 extends BaseCommand {

  private static final GetAll651 singleton = new GetAll651();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Part regionNamePart = null, keysPart = null;
    String regionName = null;
    Object[] keys = null;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    // Retrieve the region name from the message parts
    regionNamePart = clientMessage.getPart(0);
    regionName = regionNamePart.getString();

    // Retrieve the keys array from the message parts
    keysPart = clientMessage.getPart(1);
    try {
      keys = (Object[]) keysPart.getObject();
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
      // if (regionName == null) (can only be null)
      {
        message = "The input region name for the getAll request is null";
      }
      logger.warn("{}: {}", serverConnection.getName(), message);
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
      fillAndSendGetAllResponseChunks(region, regionName, keys, serverConnection, securityService);
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
      ServerConnection servConn, SecurityService securityService) throws IOException {

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
    ObjectPartList651 values = getObjectPartsList(keys == null);
    AuthorizeRequest authzRequest = servConn.getAuthzRequest();
    AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
    Request request = (Request) Request.getCommand();
    Object[] valueAndIsObject = new Object[3];
    final boolean isDebugEnabled = logger.isDebugEnabled();
    for (int i = 0; i < numKeys; i++) {
      // Send the intermediate chunk if necessary
      if (values.size() == MAXIMUM_CHUNK_SIZE) {
        // Send the chunk and clear the list
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
          logger.warn(
              String.format("%s: Caught the following exception attempting to get value for key=%s",
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
      request.getValueAndIsObject(region, key, null, servConn, valueAndIsObject);
      Object value = valueAndIsObject[0];
      boolean isObject = ((Boolean) valueAndIsObject[1]).booleanValue();
      keyNotPresent = ((Boolean) valueAndIsObject[2]).booleanValue();
      if (isDebugEnabled) {
        logger.debug("{}: Retrieved value for key={}: {}", servConn.getName(), key, value);
      }

      if (postAuthzRequest != null) {
        try {
          getContext = postAuthzRequest.getAuthorize(regionName, key, value, isObject, getContext);
          byte[] serializedValue = getContext.getSerializedValue();
          if (serializedValue == null) {
            value = getContext.getObject();
          } else {
            value = serializedValue;
          }
          isObject = getContext.isObject();
          if (isDebugEnabled) {
            logger.debug("{}: Passed GET post-authorization for key={}: {}", servConn.getName(),
                key, value);
          }
        } catch (NotAuthorizedException ex) {
          logger.warn(
              String.format("%s: Caught the following exception attempting to get value for key=%s",
                  new Object[] {servConn.getName(), key}),
              ex);
          values.addExceptionPart(key, ex);
          continue;
        }
      }
      value = securityService.postProcess(regionName, key, value, isObject);

      if (isDebugEnabled) {
        logger.debug("{}: Returning value for key={}: {}", servConn.getName(), key, value);
      }

      // Add the value to the list of values
      if (keyNotPresent) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: key={} is not present on server.", servConn.getName(), key);
        }
        values.addObjectPartForAbsentKey(key, value);
      } else {
        values.addObjectPart(key, value, isObject, null);
      }
    }

    // Send the last chunk even if the list is of zero size.
    sendGetAllResponseChunk(region, values, true, servConn);
    servConn.setAsTrue(RESPONDED);
  }

  /**
   * This method is protected so it can be overriden in GetAllForRI.
   *
   * @param includeKeys if the part list should include the keys
   */
  protected ObjectPartList651 getObjectPartsList(boolean includeKeys) {
    ObjectPartList651 values = new ObjectPartList651(MAXIMUM_CHUNK_SIZE, includeKeys);
    return values;
  }

  private static void sendGetAllResponseChunk(Region region, ObjectPartList651 list,
      boolean lastChunk, ServerConnection servConn) throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, false);

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} getAll response chunk for region={} values={} chunk=<{}>",
          servConn.getName(), (lastChunk ? " last " : " "), region.getFullPath(), list,
          chunkedResponseMsg);
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

}
