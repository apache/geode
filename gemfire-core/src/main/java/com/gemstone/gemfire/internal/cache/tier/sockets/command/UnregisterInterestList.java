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

import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.operations.UnregisterInterestOperationContext;
import com.gemstone.org.jgroups.util.StringId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class UnregisterInterestList extends BaseCommand {

  private final static UnregisterInterestList singleton = new UnregisterInterestList();

  public static Command getCommand() {
    return singleton;
  }

  private UnregisterInterestList() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    Part regionNamePart = null, keyPart = null, numberOfKeysPart = null;
    String regionName = null;
    Object key = null;
    List keys = null;
    int numberOfKeys = 0, partNumber = 0;
    servConn.setAsTrue(REQUIRES_RESPONSE);

    // bserverStats.incLong(readDestroyRequestTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyRequestsId, 1);
    // start = DistributionStats.getStatTime();
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    regionName = regionNamePart.getString();

    Part isClosingListPart = msg.getPart(1);
    byte[] isClosingListPartBytes = (byte[])isClosingListPart.getObject();
    boolean isClosingList = isClosingListPartBytes[0] == 0x01;
    boolean keepalive = false ;
    try {
      Part keepalivePart = msg.getPart(2);
      byte[] keepalivePartBytes = (byte[])keepalivePart.getObject();
      keepalive = keepalivePartBytes[0] == 0x01;
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
        writeException(msg, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
      keys.add(key);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received unregister interest request ({} bytes) from {} for the following {} keys in region {}: {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString(), numberOfKeys, regionName, keys);
    }

    // Process the unregister interest request
    if (keys.isEmpty() || regionName == null) {
      StringId errMessage = null;
      if (keys.isEmpty() && regionName == null) {
        errMessage = LocalizedStrings.UnRegisterInterestList_THE_INPUT_LIST_OF_KEYS_IS_EMPTY_AND_THE_INPUT_REGION_NAME_IS_NULL_FOR_THE_UNREGISTER_INTEREST_REQUEST;  
      } else if (keys.isEmpty()) {
        errMessage = LocalizedStrings.UnRegisterInterestList_THE_INPUT_LIST_OF_KEYS_FOR_THE_UNREGISTER_INTEREST_REQUEST_IS_EMPTY;        
      } else if (regionName == null) {
        errMessage = LocalizedStrings.UnRegisterInterest_THE_INPUT_REGION_NAME_FOR_THE_UNREGISTER_INTEREST_REQUEST_IS_NULL;
      }
      String s = errMessage.toLocalizedString();
      logger.warn("{}: {}", servConn.getName(), s);
      writeErrorResponse(msg, MessageType.UNREGISTER_INTEREST_DATA_ERROR,
          s, servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        // TODO SW: This is a workaround for DynamicRegionFactory
        // registerInterest calls. Remove this when the semantics of
        // DynamicRegionFactory are cleaned up.
        if (!DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          try {
            UnregisterInterestOperationContext unregisterContext = authzRequest
                .unregisterInterestListAuthorize(regionName, keys);
            keys = (List)unregisterContext.getKey();
          }
          catch (NotAuthorizedException ex) {
            writeException(msg, ex, false, servConn);
            servConn.setAsTrue(RESPONDED);
            return;
          }
        }
      }
      // Yogesh : bug fix for 36457 :
      /*
       * Region destroy message from server to client results in client calling
       * unregister to server (an unnecessary callback). The unregister
       * encounters an error because the region has been destroyed on the server
       * and hence falsely marks the server dead.
       */
      /*
       * Region region = crHelper.getRegion(regionName); if (region == null) {
       * logger.warning(this.name + ": Region named " + regionName + " was not
       * found during register interest list request"); writeErrorResponse(msg,
       * MessageType.UNREGISTER_INTEREST_DATA_ERROR); responded = true; } else {
       */
      // Register interest
      servConn.getAcceptor().getCacheClientNotifier().unregisterClientInterest(
          regionName, keys, isClosingList, servConn.getProxyID(), keepalive);

      // Update the statistics and write the reply
      // bserverStats.incLong(processDestroyTimeId,
      // DistributionStats.getStatTime() - start);
      // start = DistributionStats.getStatTime(); WHY ARE GETTING START AND NOT
      // USING IT?
      writeReply(msg, servConn);
      servConn.setAsTrue(RESPONDED);
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sent unregister interest response for the following {} keys in region {}: {}", servConn.getName(), numberOfKeys, regionName, keys);
      }
      // bserverStats.incLong(writeDestroyResponseTimeId,
      // DistributionStats.getStatTime() - start);
      // bserverStats.incInt(destroyResponsesId, 1);
      // }
    }

  }

}
