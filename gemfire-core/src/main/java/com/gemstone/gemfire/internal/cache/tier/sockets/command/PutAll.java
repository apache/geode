/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * Author: Gester Zhou
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.util.Collections;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PutAllPartialResultException;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.ResourceException;
import com.gemstone.gemfire.cache.operations.PutAllOperationContext;
import com.gemstone.gemfire.distributed.internal.DistributionStats;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

public class PutAll extends BaseCommand {
  
  private final static PutAll singleton = new PutAll();
  
  public static Command getCommand() {
    return singleton;
  }
  
  private PutAll() {
  }
  
  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
  throws IOException, InterruptedException {
    Part regionNamePart = null, numberOfKeysPart = null, keyPart = null, valuePart = null;
    String regionName = null;
    int numberOfKeys = 0;
    Object key = null;
    Part eventPart = null;
    StringBuffer errMessage = new StringBuffer();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    boolean replyWithMetaData = false;
    if (crHelper.emulateSlowServer() > 0) {
      // this.logger.fine("SlowServer", new Exception());
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(crHelper.emulateSlowServer());
      }
      catch (InterruptedException ugh) {
        interrupted = true;
        servConn.getCachedRegionHelper().getCache().getCancelCriterion()
            .checkCancelInProgress(ugh);
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    
    // requiresResponse = true;
    servConn.setAsTrue(REQUIRES_RESPONSE);
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadPutAllRequestTime(start - oldStart);
    }
    
    try {
      // Retrieve the data from the message parts
      // part 0: region name
      regionNamePart = msg.getPart(0);
      regionName = regionNamePart.getString();
      
      if (regionName == null) {
        String putAllMsg = LocalizedStrings.PutAll_THE_INPUT_REGION_NAME_FOR_THE_PUTALL_REQUEST_IS_NULL.toLocalizedString();
        logger.warn("{}: {}", servConn.getName(), putAllMsg);
        errMessage.append(putAllMsg);
        writeErrorResponse(msg, MessageType.PUT_DATA_ERROR,
            errMessage.toString(), servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
      LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
      if (region == null) {
        String reason = " was not found during put request";
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
      
      // part 1: eventID
      eventPart = msg.getPart(1);
      ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart
          .getSerializedForm());
      long threadId = EventID
      .readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
      long sequenceId = EventID
      .readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
      EventID eventId = new EventID(servConn.getEventMemberIDByteArray(),
          threadId, sequenceId);
      
      // part 2: number of keys
      numberOfKeysPart = msg.getPart(2);
      numberOfKeys = numberOfKeysPart.getInt();
      
      // building the map
      Map map = new LinkedHashMap();
//    Map isObjectMap = new LinkedHashMap();
      for (int i=0; i<numberOfKeys; i++) {
        keyPart = msg.getPart(3+i*2);
        key = keyPart.getStringOrObject();
        if (key == null) {
          String putAllMsg = LocalizedStrings.PutAll_ONE_OF_THE_INPUT_KEYS_FOR_THE_PUTALL_REQUEST_IS_NULL.toLocalizedString();
          logger.warn("{}: {}", servConn.getName(), putAllMsg);
          errMessage.append(putAllMsg);
          writeErrorResponse(msg, MessageType.PUT_DATA_ERROR,
              errMessage.toString(), servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
        
        valuePart = msg.getPart(3+i*2+1);
        if (valuePart.isNull()) {
          String putAllMsg = LocalizedStrings.PutAll_ONE_OF_THE_INPUT_VALUES_FOR_THE_PUTALL_REQUEST_IS_NULL.toLocalizedString();
          logger.warn("{}: {}", servConn.getName(), putAllMsg);
          errMessage.append(putAllMsg);
          writeErrorResponse(msg, MessageType.PUT_DATA_ERROR, errMessage
              .toString(), servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
        
//      byte[] value = valuePart.getSerializedForm();
        Object value;
        if (valuePart.isObject()) {
          value = CachedDeserializableFactory.create(valuePart.getSerializedForm());
        } else {
          value = valuePart.getSerializedForm();
        }
//      put serializedform for auth. It will be modified with auth callback
        map.put(key, value);
//      isObjectMap.put(key, new Boolean(isObject));
      } // for
      
      if ( msg.getNumberOfParts() == ( 3 + 2*numberOfKeys + 1) ) {//it means optional timeout has been added
        int timeout = msg.getPart(3 + 2*numberOfKeys).getInt();
        servConn.setRequestSpecificTimeout(timeout);
      }

      
      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        // TODO SW: This is to handle DynamicRegionFactory create
        // calls. Rework this when the semantics of DynamicRegionFactory
        // are
        // cleaned up.
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          authzRequest.createRegionAuthorize(regionName);
        }
        else {
          PutAllOperationContext putAllContext = authzRequest.putAllAuthorize(
              regionName, map, null);
          map = putAllContext.getMap();
        }
      } else {
        // no auth, so update the map based on isObjectMap here
        /*
         Collection entries = map.entrySet();
         Iterator iterator = entries.iterator();
         Map.Entry mapEntry = null;
         while (iterator.hasNext()) {
         mapEntry = (Map.Entry)iterator.next();
         Object currkey = mapEntry.getKey();
         byte[] serializedValue = (byte[])mapEntry.getValue();
         boolean isObject = ((Boolean)isObjectMap.get(currkey)).booleanValue();
         if (isObject) {
         map.put(currkey, CachedDeserializableFactory.create(serializedValue));
         }
         }
         */
      }
      
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Received putAll request ({} bytes) from {} for region {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString(), regionName);
      }
      
      region.basicBridgePutAll(map, Collections.<Object, VersionTag>emptyMap(), servConn.getProxyID(), eventId, false, null);
      
      if (region instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)region;
        if (pr.isNetworkHop() != (byte)0) {
          writeReplyWithRefreshMetadata(msg, servConn,pr,pr.isNetworkHop());
          pr.setIsNetworkHop((byte)0);
          pr.setMetadataVersion(Byte.valueOf((byte)0));
          replyWithMetaData = true;
        }
      }
    } 
    catch (RegionDestroyedException rde) {
      writeException(msg, rde, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    catch (ResourceException re) {
      writeException(msg, re, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    catch (PutAllPartialResultException pre) {
      writeException(msg, pre, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    catch (Exception ce) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(servConn, ce);
      
      // If an exception occurs during the put, preserve the connection
      writeException(msg, ce, false, servConn);
      servConn.setAsTrue(RESPONDED);
      logger.warn(LocalizedMessage.create(LocalizedStrings.Generic_0_UNEXPECTED_EXCEPTION, servConn.getName()), ce);
      return;
    }
    finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessPutAllTime(start - oldStart);
    }
    
    // Increment statistics and write the reply
    if (!replyWithMetaData) {
      writeReply(msg, servConn);
    }
    servConn.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent putAll response back to {} for region {}", servConn.getName(), servConn.getSocketString(), regionName);
    }
    stats.incWritePutAllResponseTime(DistributionStats.getStatTime() - start);
  }
}
