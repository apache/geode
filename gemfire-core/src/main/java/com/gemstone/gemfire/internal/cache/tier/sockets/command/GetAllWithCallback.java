/*=========================================================================
 * Copyright (c) 2002-2014, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.GetOperationContext;
import com.gemstone.gemfire.i18n.LogWriterI18n;
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
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.security.NotAuthorizedException;

/**
 * Initial version copied from GetAll70.java r48777.
 * 
 * @author dschneider
 *
 */
public class GetAllWithCallback extends BaseCommand {
  private static final Logger logger = LogService.getLogger();

  private final static GetAllWithCallback singleton = new GetAllWithCallback();

  public static Command getCommand() {
    return singleton;
  }

  protected GetAllWithCallback() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
          throws IOException, InterruptedException {
    Part regionNamePart = null, keysPart = null, callbackPart = null;
    String regionName = null;
    Object[] keys = null;
    Object callback = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
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
    callbackPart = msg.getPart(partIdx++);
    try {
      callback = callbackPart.getObject();
    } catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    if (logger.isDebugEnabled()) {
      StringBuffer buffer = new StringBuffer();
      buffer
              .append(servConn.getName())
              .append(": Received getAll request (")
              .append(msg.getPayloadLength())
              .append(" bytes) from ")
              .append(servConn.getSocketString())
              .append(" for region ")
              .append(regionName)
              .append(" with callback ")
              .append(callback)
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
      logger.warn(LocalizedMessage.create(LocalizedStrings.TWO_ARG_COLON, new Object[]{servConn.getName(), message}));
      writeChunkedErrorResponse(msg, MessageType.GET_ALL_DATA_ERROR, message,
              servConn);
      servConn.setAsTrue(RESPONDED);
    } else {
      LocalRegion region = (LocalRegion) crHelper.getRegion(regionName);
      if (region == null) {
        String reason = " was not found during getAll request";
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
      } else {
        // Send header
        ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
        chunkedResponseMsg.setMessageType(MessageType.RESPONSE);
        chunkedResponseMsg.setTransactionId(msg.getTransactionId());
        chunkedResponseMsg.sendHeader();

        // Send chunk response
        try {
          fillAndSendGetAllResponseChunks(region, regionName, keys, servConn, callback);
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
    }
  }

  private void fillAndSendGetAllResponseChunks(Region region,
      String regionName, Object[] keys, ServerConnection servConn, Object callback)
      throws IOException {

    assert keys != null;
    int numKeys = keys.length;
    VersionedObjectList values = new VersionedObjectList(maximumChunkSize, false, region.getAttributes().getConcurrencyChecksEnabled(), false);
    AuthorizeRequest authzRequest = servConn.getAuthzRequest();
    AuthorizeRequestPP postAuthzRequest = servConn.getPostAuthzRequest();
    Get70 request = (Get70) Get70.getCommand();
    for (int i = 0; i < numKeys; i++) {
      // Send the intermediate chunk if necessary
      if (values.size() == maximumChunkSize) {
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
          logger.warn(LocalizedMessage.create(LocalizedStrings.GetAll_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1,
                  new Object[]{servConn.getName(), key}), ex);
          values.addExceptionPart(key, ex);
          continue;
        }
      }

      // Get the value and update the statistics. Do not deserialize
      // the value if it is a byte[].
      // Getting a value in serialized form is pretty nasty. I split this out
      // so the logic can be re-used by the CacheClientProxy.
      Get70.Entry entry = request.getValueAndIsObject(region, key, callback, servConn);
      keyNotPresent = entry.keyNotPresent;
      if (logger.isDebugEnabled()) {
        logger.debug("retrieved key={} {}", key, entry);
      }

      if (postAuthzRequest != null) {
        try {
          getContext = postAuthzRequest.getAuthorize(regionName, key, entry.value,
                  entry.isObject, getContext);
          byte[] serializedValue = getContext.getSerializedValue();
          if (serializedValue == null) {
            entry.value = getContext.getObject();
          } else {
            entry.value = serializedValue;
          }
          entry.isObject = getContext.isObject();
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Passed GET post-authorization for key={}: {}", servConn.getName(), key, entry.value);
          }
        } catch (NotAuthorizedException ex) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.GetAll_0_CAUGHT_THE_FOLLOWING_EXCEPTION_ATTEMPTING_TO_GET_VALUE_FOR_KEY_1,
                  new Object[]{servConn.getName(), key}), ex);
          values.addExceptionPart(key, ex);
          continue;
        }
      }


      // Add the entry to the list that will be returned to the client

      if (keyNotPresent) {
        values.addObjectPartForAbsentKey(key, entry.value, entry.versionTag);
      } else {
        values.addObjectPart(key, entry.value, entry.isObject, entry.versionTag);
      }
    }

    // Send the last chunk even if the list is of zero size.
    sendGetAllResponseChunk(region, values, true, servConn);
    servConn.setAsTrue(RESPONDED);
  }


  private static void sendGetAllResponseChunk(Region region, ObjectPartList list,
                                              boolean lastChunk, ServerConnection servConn) throws IOException {
    ChunkedMessage chunkedResponseMsg = servConn.getChunkedResponseMessage();
    chunkedResponseMsg.setNumberOfParts(1);
    chunkedResponseMsg.setLastChunk(lastChunk);
    chunkedResponseMsg.addObjPart(list, zipValues);

    if (logger.isDebugEnabled()) {
      String str = servConn.getName() + ": Sending" +
              (lastChunk ? " last " : " ") + "getAll response chunk for region=" +
              region.getFullPath() + (logger.isTraceEnabled()? " values=" + list + " chunk=<" +
              chunkedResponseMsg + ">" : "");
      logger.debug(str);
    }

    chunkedResponseMsg.sendChunk(servConn);
  }

}
