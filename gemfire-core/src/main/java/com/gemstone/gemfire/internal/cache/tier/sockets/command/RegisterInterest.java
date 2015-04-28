/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.operations.RegisterInterestOperationContext;
import com.gemstone.org.jgroups.util.StringId;

import java.io.IOException;

public class RegisterInterest extends BaseCommand {

  private final static RegisterInterest singleton = new RegisterInterest();

  public static Command getCommand() {
    return singleton;
  }

  private RegisterInterest() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {
    Part regionNamePart = null, keyPart = null;
    String regionName = null;
    Object key = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    // bserverStats.incLong(readDestroyRequestTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyRequestsId, 1);
    // start = DistributionStats.getStatTime();
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    regionName = regionNamePart.getString();
    InterestResultPolicy policy = null;
    // Retrieve the interest type
    int interestType = msg.getPart(1).getInt();

    // Retrieve the InterestResultPolicy
    try {
      policy = (InterestResultPolicy)msg.getPart(2).getObject();
    }
    catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    boolean isDurable = false ;
    try {
      Part durablePart = msg.getPart(3);
      byte[] durablePartBytes = (byte[])durablePart.getObject();
      isDurable = durablePartBytes[0] == 0x01;
    }
    catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    // Retrieve the key
    keyPart = msg.getPart(4);
    regionName = regionNamePart.getString();
    try {
      key = keyPart.getStringOrObject();
    }
    catch (Exception e) {
      writeChunkedException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    
    boolean sendUpdatesAsInvalidates = false;
    
    // VJR: Check for a sixth part for client version 6.0.3 onwards for the
    // time being until refactoring into a new command version.
    if (msg.getNumberOfParts() > 5) {
      try {
        Part notifyPart = msg.getPart(5);
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
      logger.debug("{}: Received register interest request ({} bytes) from {} for region {} key {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString(), regionName, key);
    }
    
    /*
    AcceptorImpl acceptor = servConn.getAcceptor();
    
    //  Check if the Server is running in NotifyBySubscription=true mode.
    if (!acceptor.getCacheClientNotifier().getNotifyBySubscription()) {
      // This should have been taken care at the client.
      String err = LocalizedStrings.RegisterInterest_INTEREST_REGISTRATION_IS_SUPPORTED_ONLY_FOR_SERVERS_WITH_NOTIFYBYSUBSCRIPTION_SET_TO_TRUE.toLocalizedString() ;
      writeChunkedErrorResponse(msg, MessageType.REGISTER_INTEREST_DATA_ERROR,
          err, servConn);
      servConn.setAsTrue(RESPONDED);  return;
    }
    */
    
    // Process the register interest request
    if (key == null || regionName == null) {
      StringId message = null;
      if (key == null) {
        message = LocalizedStrings.RegisterInterest_THE_INPUT_KEY_FOR_THE_REGISTER_INTEREST_REQUEST_IS_NULL;
      }
      if (regionName == null) {
        message = LocalizedStrings.RegisterInterest_THE_INPUT_REGION_NAME_FOR_THE_REGISTER_INTEREST_REQUEST_IS_NULL;
      }
      logger.warn("{}: {}", servConn.getName(), message.toLocalizedString());
      writeChunkedErrorResponse(msg, MessageType.REGISTER_INTEREST_DATA_ERROR,
          message.toLocalizedString(), servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else { // input key not null
      LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
      if (region == null) {
        logger.info(LocalizedMessage.create(LocalizedStrings.RegisterInterest_0_REGION_NAMED_1_WAS_NOT_FOUND_DURING_REGISTER_INTEREST_REQUEST, new Object[] {servConn.getName(), regionName}));
        // writeChunkedErrorResponse(msg,
        // MessageType.REGISTER_INTEREST_DATA_ERROR, message);
        // responded = true;
      }
      // Register interest
      try {
        AuthorizeRequest authzRequest = servConn.getAuthzRequest();
        if (authzRequest != null) {
          // TODO SW: This is a workaround for DynamicRegionFactory
          // registerInterest calls. Remove this when the semantics of
          // DynamicRegionFactory are cleaned up.
          if (!DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
            RegisterInterestOperationContext registerContext = authzRequest
                .registerInterestAuthorize(regionName, key, interestType,
                    policy);
            key = registerContext.getKey();
          }
        }
        servConn.getAcceptor().getCacheClientNotifier().registerClientInterest(
            regionName, key, servConn.getProxyID(), interestType, isDurable,
            sendUpdatesAsInvalidates, false, 0, true);
      }
      catch (Exception e) {
        // If an interrupted exception is thrown , rethrow it
        checkForInterrupt(servConn, e);
        // Otherwise, write an exception message and continue
        writeChunkedException(msg, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }

      // System.out.println("Received register interest for " + regionName);

      // Update the statistics and write the reply
      // bserverStats.incLong(processDestroyTimeId,
      // DistributionStats.getStatTime() - start);
      // start = DistributionStats.getStatTime();

      CacheClientProxy ccp = servConn.getAcceptor().getCacheClientNotifier()
          .getClientProxy(servConn.getProxyID());
      if (ccp == null) {
        // fix for 37593
        IOException ioex = new IOException(LocalizedStrings.RegisterInterest_CACHECLIENTPROXY_FOR_THIS_CLIENT_IS_NO_LONGER_ON_THE_SERVER_SO_REGISTERINTEREST_OPERATION_IS_UNSUCCESSFUL.toLocalizedString());
        writeChunkedException(msg, ioex, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
      boolean isPrimary = ccp.isPrimary();
      ChunkedMessage chunkedResponseMsg = servConn.getRegisterInterestResponseMessage();
      if (!isPrimary) {
        chunkedResponseMsg.setMessageType(MessageType.RESPONSE_FROM_SECONDARY);
        chunkedResponseMsg.setTransactionId(msg.getTransactionId());
        chunkedResponseMsg.sendHeader();
        chunkedResponseMsg.setLastChunk(true);

        if (logger.isDebugEnabled()) {
          logger.debug("{}: Sending register interest response chunk from secondary for region: {} for key: {} chunk=<{}>", servConn.getName(), regionName, key, chunkedResponseMsg);
        }
        chunkedResponseMsg.sendChunk(servConn);
      } // !isPrimary
      else { // isPrimary

        // Send header which describes how many chunks will follow
        chunkedResponseMsg.setMessageType(MessageType.RESPONSE_FROM_PRIMARY);
        chunkedResponseMsg.setTransactionId(msg.getTransactionId());
        chunkedResponseMsg.sendHeader();

        // Send chunk response
        try {
          fillAndSendRegisterInterestResponseChunks(region, key, interestType,
              policy, servConn);
          servConn.setAsTrue(RESPONDED);
        }
        catch (Exception e) {
          writeChunkedException(msg, e, false, servConn, chunkedResponseMsg);
          servConn.setAsTrue(RESPONDED);
          return;
        }

        if (logger.isDebugEnabled()) {
          // logger.debug(getName() + ": Sent chunk (1 of 1) of register interest
          // response (" + chunkedResponseMsg.getBufferLength() + " bytes) for
          // region " + regionName + " key " + key);
          logger.debug("{}: Sent register interest response for region {} key {}", servConn.getName(), regionName, key);
        }
        // bserverStats.incLong(writeDestroyResponseTimeId,
        // DistributionStats.getStatTime() - start);
        // bserverStats.incInt(destroyResponsesId, 1);
      } // isPrimary
    } // input key not null
  }

}
