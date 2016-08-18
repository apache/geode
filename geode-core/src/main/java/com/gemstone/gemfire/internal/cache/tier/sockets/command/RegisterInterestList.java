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

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.operations.RegisterInterestOperationContext;
import com.gemstone.gemfire.i18n.StringId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public class RegisterInterestList extends BaseCommand {

  private final static RegisterInterestList singleton = new RegisterInterestList();

  public static Command getCommand() {
    return singleton;
  }

  RegisterInterestList() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {
    Part regionNamePart = null, keyPart = null, numberOfKeysPart = null;
    String regionName = null;
    Object key = null;
    InterestResultPolicy policy;
    List keys = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    int numberOfKeys = 0, partNumber = 0;
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    ChunkedMessage chunkedResponseMsg = servConn.getRegisterInterestResponseMessage();

    // bserverStats.incLong(readDestroyRequestTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyRequestsId, 1);
    // start = DistributionStats.getStatTime();
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    regionName = regionNamePart.getString();

    // Retrieve the InterestResultPolicy
    try {
      policy = (InterestResultPolicy)msg.getPart(1).getObject();
    }
    catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    boolean isDurable = false ;
    try {
      Part durablePart = msg.getPart(2);
      byte[] durablePartBytes = (byte[])durablePart.getObject();
      isDurable = durablePartBytes[0] == 0x01;
    }
    catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    numberOfKeysPart = msg.getPart(3);
    numberOfKeys = numberOfKeysPart.getInt();
    
    partNumber = 4;
    keys = new ArrayList();
    for (int i = 0; i < numberOfKeys; i++) {
      keyPart = msg.getPart(partNumber + i);
      try {
        key = keyPart.getStringOrObject();
      }
      catch (Exception e) {
        writeChunkedException(msg, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
      keys.add(key);
    }
    
    boolean sendUpdatesAsInvalidates = false ;
    
    // VJR: Check for an extra part for client version 6.0.3 onwards for the
    // time being until refactoring into a new command version.
    if (msg.getNumberOfParts() > (numberOfKeys + partNumber)) {
      try {
        Part notifyPart = msg.getPart(numberOfKeys + partNumber);
        byte[] notifyPartBytes = (byte[])notifyPart.getObject();
        sendUpdatesAsInvalidates = notifyPartBytes[0] == 0x01;
      }
      catch (Exception e) {
        writeChunkedException(msg, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received register interest request ({} bytes) from {} for the following {} keys in region {}: {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString(), numberOfKeys, regionName, keys);
    }
    
    /*
    AcceptorImpl acceptor = servConn.getAcceptor();
    
    //  Check if the Server is running in NotifyBySubscription=true mode.
    if (!acceptor.getCacheClientNotifier().getNotifyBySubscription()) {
      // This should have been taken care at the client.
      String err = LocalizedStrings.RegisterInterest_INTEREST_REGISTRATION_IS_SUPPORTED_ONLY_FOR_SERVERS_WITH_NOTIFYBYSUBSCRIPTION_SET_TO_TRUE.toLocalizedString();
      writeChunkedErrorResponse(msg, MessageType.REGISTER_INTEREST_DATA_ERROR,
          err, servConn);
      servConn.setAsTrue(RESPONDED);  return;
    }
    */

    // Process the register interest request
    if (keys.isEmpty() || regionName == null) {
      StringId errMessage = null;
      if (keys.isEmpty() && regionName == null) {
        errMessage = LocalizedStrings.RegisterInterestList_THE_INPUT_LIST_OF_KEYS_IS_EMPTY_AND_THE_INPUT_REGION_NAME_IS_NULL_FOR_THE_REGISTER_INTEREST_REQUEST;  
      } else if (keys.isEmpty()) {
        errMessage = LocalizedStrings.RegisterInterestList_THE_INPUT_LIST_OF_KEYS_FOR_THE_REGISTER_INTEREST_REQUEST_IS_EMPTY;        
      } else if (regionName == null) {
        errMessage = LocalizedStrings.RegisterInterest_THE_INPUT_REGION_NAME_FOR_THE_REGISTER_INTEREST_REQUEST_IS_NULL;
      }
      String s = errMessage.toLocalizedString();
      logger.warn("{}: {}", servConn.getName(), s);
      writeChunkedErrorResponse(msg, MessageType.REGISTER_INTEREST_DATA_ERROR,
          s, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    // key not null
      LocalRegion region = (LocalRegion)servConn.getCache().getRegion(regionName);
      if (region == null) {
        logger.info(LocalizedMessage.create(LocalizedStrings.RegisterInterestList_0_REGION_NAMED_1_WAS_NOT_FOUND_DURING_REGISTER_INTEREST_LIST_REQUEST, new Object[]{servConn.getName(), regionName}));
        // writeChunkedErrorResponse(msg,
        // MessageType.REGISTER_INTEREST_DATA_ERROR, message);
        // responded = true;
      } // else { // region not null
      try {
        this.securityService.authorizeRegionRead(regionName);
        AuthorizeRequest authzRequest = servConn.getAuthzRequest();
        if (authzRequest != null) {
          if (!DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
            RegisterInterestOperationContext registerContext = authzRequest
                .registerInterestListAuthorize(regionName, keys, policy);
            keys = (List)registerContext.getKey();
          }
        }
        // Register interest
        servConn.getAcceptor().getCacheClientNotifier().registerClientInterest(
            regionName, keys, servConn.getProxyID(), isDurable,
            sendUpdatesAsInvalidates, false, 0, true);
      }
      catch (Exception ex) {
        // If an interrupted exception is thrown , rethrow it
        checkForInterrupt(servConn, ex);
        // Otherwise, write an exception message and continue
        writeChunkedException(msg, ex, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }

      // Update the statistics and write the reply
      // bserverStats.incLong(processDestroyTimeId,
      // DistributionStats.getStatTime() - start);
      // start = DistributionStats.getStatTime();

      boolean isPrimary = servConn.getAcceptor().getCacheClientNotifier()
          .getClientProxy(servConn.getProxyID()).isPrimary();
      if (!isPrimary) {
        chunkedResponseMsg.setMessageType(MessageType.RESPONSE_FROM_SECONDARY);
        chunkedResponseMsg.setTransactionId(msg.getTransactionId());
        chunkedResponseMsg.sendHeader();
        chunkedResponseMsg.setLastChunk(true);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Sending register interest response chunk from secondary for region: {} for key: {} chunk=<{}>", servConn.getName(), regionName, key, chunkedResponseMsg);
        }
        chunkedResponseMsg.sendChunk(servConn);
      }
      else { // isPrimary
        // Send header which describes how many chunks will follow
        chunkedResponseMsg.setMessageType(MessageType.RESPONSE_FROM_PRIMARY);
        chunkedResponseMsg.setTransactionId(msg.getTransactionId());
        chunkedResponseMsg.sendHeader();

        // Send chunk response
        try {
          fillAndSendRegisterInterestResponseChunks(region, keys,
              InterestType.KEY, policy, servConn);
          servConn.setAsTrue(RESPONDED);
        }
        catch (Exception e) {
          // If an interrupted exception is thrown , rethrow it
          checkForInterrupt(servConn, e);

          // otherwise send the exception back to client
          writeChunkedException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }

        if (logger.isDebugEnabled()) {
          // logger.debug(getName() + ": Sent chunk (1 of 1) of register interest
          // response (" + chunkedResponseMsg.getBufferLength() + " bytes) for
          // region " + regionName + " key " + key);
          logger.debug("{}: Sent register interest response for the following {} keys in region {}: {}", servConn.getName(), numberOfKeys, regionName, keys);
        }
        // bserverStats.incLong(writeDestroyResponseTimeId,
        // DistributionStats.getStatTime() - start);
        // bserverStats.incInt(destroyResponsesId, 1);
      } // isPrimary
      // } // region not null
  }

}
