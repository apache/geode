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
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.operations.DestroyOperationContext;
import com.gemstone.gemfire.cache.operations.PutOperationContext;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.EventIDHolder;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.cache.wan.BatchException70;
import com.gemstone.gemfire.internal.cache.wan.GatewayReceiverStats;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.pdx.PdxConfigurationException;
import com.gemstone.gemfire.pdx.PdxRegistryMismatchException;
import com.gemstone.gemfire.pdx.internal.PeerTypeRegistration;
import com.gemstone.gemfire.i18n.StringId;

public class GatewayReceiverCommand extends BaseCommand {

  private final static GatewayReceiverCommand singleton = new GatewayReceiverCommand();

  public static Command getCommand() {
    return singleton;
  }

  private GatewayReceiverCommand() {
  }

  private void handleRegionNull(ServerConnection servConn, String regionName, int batchId) {
    GemFireCacheImpl gfc = (GemFireCacheImpl)servConn.getCachedRegionHelper().getCache();
    if (gfc != null && gfc.isCacheAtShutdownAll()) {
      throw new CacheClosedException("Shutdown occurred during message processing");
    } else {
      String reason = LocalizedStrings.ProcessBatch_WAS_NOT_FOUND_DURING_BATCH_CREATE_REQUEST_0.toLocalizedString(new Object[] {regionName, Integer.valueOf(batchId)});
      throw new RegionDestroyedException(reason, regionName);
    }
  }
  
  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, InterruptedException {
    Part regionNamePart = null, keyPart = null, valuePart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    int partNumber = 0;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    GatewayReceiverStats stats = (GatewayReceiverStats)servConn.getCacheServerStats();
    EventID eventId = null;
    LocalRegion region = null;
    List<BatchException70> exceptions = new ArrayList<BatchException70>();
    Throwable fatalException = null;
    //requiresResponse = true;// let PROCESS_BATCH deal with this itself
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadProcessBatchRequestTime(start - oldStart);
    }
    Part callbackArgExistsPart;
    // Get early ack flag. This test should eventually be moved up above this switch
    // statement so that all messages can take advantage of it.
    boolean earlyAck = false;//msg.getEarlyAck();

    stats.incBatchSize(msg.getPayloadLength());

    // Retrieve the number of events
    Part numberOfEventsPart = msg.getPart(0);
    int numberOfEvents = numberOfEventsPart.getInt();
    stats.incEventsReceived(numberOfEvents);
    
    // Retrieve the batch id
    Part batchIdPart = msg.getPart(1);
    int batchId = batchIdPart.getInt();

    // If this batch has already been seen, do not reply.
    // Instead, drop the batch and continue.
    if (batchId <= servConn.getLatestBatchIdReplied()) {
      if (GatewayReceiver.APPLY_RETRIES) {
        // Do nothing!!!
        logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_0_THAT_HAS_ALREADY_BEEN_OR_IS_BEING_PROCESSED_GEMFIRE_GATEWAY_APPLYRETRIES_IS_SET_SO_THIS_BATCH_WILL_BE_PROCESSED_ANYWAY, batchId));
      }
      else {
        logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_0_THAT_HAS_ALREADY_BEEN_OR_IS_BEING_PROCESSED__THIS_PROCESS_BATCH_REQUEST_IS_BEING_IGNORED, batchId));
        writeReply(msg, servConn, batchId, numberOfEvents);
        return;
      }
      stats.incDuplicateBatchesReceived();
    }

    // Verify the batches arrive in order
    if (batchId != servConn.getLatestBatchIdReplied() + 1) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_RECEIVED_PROCESS_BATCH_REQUEST_0_OUT_OF_ORDER_THE_ID_OF_THE_LAST_BATCH_PROCESSED_WAS_1_THIS_BATCH_REQUEST_WILL_BE_PROCESSED_BUT_SOME_MESSAGES_MAY_HAVE_BEEN_LOST, new Object[] { batchId, servConn.getLatestBatchIdReplied() }));
      stats.incOutoforderBatchesReceived();
    }
    

    if (logger.isDebugEnabled()) {
      logger.debug("Received process batch request {} that will be processed.", batchId);
    }
    // If early ack mode, acknowledge right away
    // Not sure if earlyAck makes sense with sliding window
    if (earlyAck) {
      servConn.incrementLatestBatchIdReplied(batchId);
      
      //writeReply(msg, servConn);
      //servConn.setAsTrue(RESPONDED);
      {
        long oldStart = start;
        start = DistributionStats.getStatTime();
        stats.incWriteProcessBatchResponseTime(start - oldStart);
      }
      stats.incEarlyAcks();
    }
   

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received process batch request {} containing {} events ({} bytes) with {} acknowledgement on {}", servConn.getName(), batchId, numberOfEvents, msg.getPayloadLength(), (earlyAck ? "early" : "normal"), servConn.getSocketString());
      if (earlyAck) {
        logger.debug("{}: Sent process batch early response for batch {} containing {} events ({} bytes) with {} acknowledgement on {}", servConn.getName(), batchId, numberOfEvents, msg.getPayloadLength(), (earlyAck ? "early" : "normal"), servConn.getSocketString());
      }
    }
    // logger.warn("Received process batch request " + batchId + " containing
    // " + numberOfEvents + " events (" + msg.getPayloadLength() + " bytes) with
    // " + (earlyAck ? "early" : "normal") + " acknowledgement on " +
    // getSocketString());
    // if (earlyAck) {
    // logger.warn("Sent process batch early response for batch " + batchId +
    // " containing " + numberOfEvents + " events (" + msg.getPayloadLength() +
    // " bytes) with " + (earlyAck ? "early" : "normal") + " acknowledgement on
    // " + getSocketString());
    // }

    // Retrieve the events from the message parts. The '2' below
    // represents the number of events (part0) and the batchId (part1)
    partNumber = 2;
    int dsid = msg.getPart(partNumber++).getInt();
    
    boolean removeOnException = msg.getPart(partNumber++).getSerializedForm()[0]==1?true:false;
    
    // Keep track of whether a response has been written for
    // exceptions
    boolean wroteResponse = earlyAck;
    // event received in batch also have PDX events at the start of the batch,to
    // represent correct index on which the exception occurred, number of PDX
    // events need to be subtratced.  
    int indexWithoutPDXEvent = -1; //
    for (int i = 0; i < numberOfEvents; i++) {
      indexWithoutPDXEvent++;
      // System.out.println("Processing event " + i + " in batch " + batchId + "
      // starting with part number " + partNumber);
      Part actionTypePart = msg.getPart(partNumber);
      int actionType = actionTypePart.getInt();
      
      long versionTimeStamp = VersionTag.ILLEGAL_VERSION_TIMESTAMP;
      EventIDHolder clientEvent = null;
      
      boolean callbackArgExists = false;

      try {
        Part possibleDuplicatePart = msg.getPart(partNumber + 1);
        byte[] possibleDuplicatePartBytes;
        try {
          possibleDuplicatePartBytes = (byte[])possibleDuplicatePart
              .getObject();
        }
        catch (Exception e) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_REQUEST_1_CONTAINING_2_EVENTS, new Object[] { servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents) }), e);
          throw e;
        }
        boolean possibleDuplicate = possibleDuplicatePartBytes[0] == 0x01;

        // Make sure instance variables are null before each iteration
        regionName = null;
        key = null;
        callbackArg = null;

        // Retrieve the region name from the message parts
        regionNamePart = msg.getPart(partNumber + 2);
        regionName = regionNamePart.getString();
        if (regionName.equals(PeerTypeRegistration.REGION_FULL_PATH)) {
          indexWithoutPDXEvent --;
        }

        // Retrieve the event id from the message parts
        // This was going to be used to determine possible
        // duplication of events, but it is unused now. In
        // fact the event id is overridden by the FROM_GATEWAY
        // token.
        Part eventIdPart = msg.getPart(partNumber + 3);
        eventIdPart.setVersion(servConn.getClientVersion()); 
        // String eventId = eventIdPart.getString();
        try {
          eventId = (EventID)eventIdPart.getObject();
        }
        catch (Exception e) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_REQUEST_1_CONTAINING_2_EVENTS, new Object[] { servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents) }), e);
          throw e;
        }

        // Retrieve the key from the message parts
        keyPart = msg.getPart(partNumber + 4);
        try {
          key = keyPart.getStringOrObject();
        }
        catch (Exception e) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_REQUEST_1_CONTAINING_2_EVENTS, new Object[] { servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents) }), e);
          throw e;
        }
        switch (actionType) {
        case 0: // Create

          /*
           * CLIENT EXCEPTION HANDLING TESTING CODE String keySt = (String) key;
           * System.out.println("Processing new key: " + key); if
           * (keySt.startsWith("failure")) { throw new
           * Exception(LocalizedStrings
           * .ProcessBatch_THIS_EXCEPTION_REPRESENTS_A_FAILURE_ON_THE_SERVER
           * .toLocalizedString()); }
           */

          // Retrieve the value from the message parts (do not deserialize it)
          valuePart = msg.getPart(partNumber + 5);
          // try {
          // logger.warn(getName() + ": Creating key " + key + " value " +
          // valuePart.getObject());
          // } catch (Exception e) {}

          // Retrieve the callbackArg from the message parts if necessary
          int index = partNumber+6;
          callbackArgExistsPart = msg.getPart(index++);
          {
            byte[] partBytes = (byte[])callbackArgExistsPart.getObject();
            callbackArgExists = partBytes[0] == 0x01;
          }
          if (callbackArgExists) {
            callbackArgPart = msg.getPart(index++);
            try {
              callbackArg = callbackArgPart.getObject();
            } catch (Exception e) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_CREATE_REQUEST_1_FOR_2_EVENTS, new Object[] { servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents) }), e);
              throw e;
            }
          }
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Processing batch create request {} on {} for region {} key {} value {} callbackArg {}, eventId={}", servConn.getName(), batchId, servConn.getSocketString(), regionName, key, valuePart, callbackArg, eventId);
          }
          versionTimeStamp = msg.getPart(index++).getLong();
          // Process the create request
          if (key == null || regionName == null) {
            StringId message = null;
            Object[] messageArgs = new Object[] { servConn.getName(),
                Integer.valueOf(batchId) };
            if (key == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_CREATE_REQUEST_1_IS_NULL;
            }
            if (regionName == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_CREATE_REQUEST_1_IS_NULL;
            }
            String s = message.toLocalizedString(messageArgs);
            logger.warn(s);
            throw new Exception(s);
          }
          region = (LocalRegion)crHelper.getRegion(regionName);
          if (region == null) {
            handleRegionNull(servConn, regionName, batchId);
          } else {
            clientEvent = new EventIDHolder(eventId);
            if (versionTimeStamp > 0) {
              VersionTag tag = VersionTag.create(region.getVersionMember());
              tag.setIsGatewayTag(true);
              tag.setVersionTimeStamp(versionTimeStamp);
              tag.setDistributedSystemId(dsid);
              clientEvent.setVersionTag(tag);
            }
            clientEvent.setPossibleDuplicate(possibleDuplicate);
            handleMessageRetry(region, clientEvent);
            try {
              byte[] value = valuePart.getSerializedForm();
              boolean isObject = valuePart.isObject();
              // [sumedh] This should be done on client while sending
              // since that is the WAN gateway
              AuthorizeRequest authzRequest = servConn.getAuthzRequest();
              if (authzRequest != null) {
                PutOperationContext putContext = authzRequest.putAuthorize(
                    regionName, key, value, isObject, callbackArg);
                value = putContext.getSerializedValue();
                isObject = putContext.isObject();
              }
              // Attempt to create the entry
              boolean result = false;
              result = region.basicBridgeCreate(key, value, isObject, callbackArg,
                      servConn.getProxyID(), false, clientEvent, false); 
              // If the create fails (presumably because it already exists),
              // attempt to update the entry
              if (!result) {
                result = region.basicBridgePut(key, value, null, isObject,
                    callbackArg, servConn.getProxyID(), false, clientEvent);
              }

              if (result || clientEvent.isConcurrencyConflict()) {
                servConn.setModificationInfo(true, regionName, key);
                stats.incCreateRequest();
              } else {
                // This exception will be logged in the catch block below
                throw new Exception(
                    LocalizedStrings.ProcessBatch_0_FAILED_TO_CREATE_OR_UPDATE_ENTRY_FOR_REGION_1_KEY_2_VALUE_3_CALLBACKARG_4
                        .toLocalizedString(new Object[] { servConn.getName(),
                            regionName, key, valuePart, callbackArg }));
              }
            } catch (Exception e) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_CREATE_REQUEST_1_FOR_2_EVENTS, new Object[] { servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents) }), e);
              throw e;
            }
          }
          break;
        case 1: // Update
          /*
           * CLIENT EXCEPTION HANDLING TESTING CODE keySt = (String) key;
           * System.out.println("Processing updated key: " + key); if
           * (keySt.startsWith("failure")) { throw new
           * Exception(LocalizedStrings
           * .ProcessBatch_THIS_EXCEPTION_REPRESENTS_A_FAILURE_ON_THE_SERVER
           * .toLocalizedString()); }
           */

          // Retrieve the value from the message parts (do not deserialize it)
          valuePart = msg.getPart(partNumber + 5);
          // try {
          // logger.warn(getName() + ": Updating key " + key + " value " +
          // valuePart.getObject());
          // } catch (Exception e) {}

          // Retrieve the callbackArg from the message parts if necessary
          index = partNumber + 6;
          callbackArgExistsPart = msg.getPart(index++);
          {
            byte[] partBytes = (byte[])callbackArgExistsPart.getObject();
            callbackArgExists = partBytes[0] == 0x01;
          }
          if (callbackArgExists) {
            callbackArgPart = msg.getPart(index++);
            try {
              callbackArg = callbackArgPart.getObject();
            } catch (Exception e) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_REQUEST_1_CONTAINING_2_EVENTS, new Object[] { servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents) }), e);
              throw e;
            }
          }
          versionTimeStamp = msg.getPart(index++).getLong();
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Processing batch update request {} on {} for region {} key {} value {} callbackArg {}", servConn.getName(), batchId, servConn.getSocketString(), regionName, key, valuePart, callbackArg);
          }
          // Process the update request
          if (key == null || regionName == null) {
            StringId message = null;
            Object[] messageArgs = new Object[] { servConn.getName(),
                Integer.valueOf(batchId) };
            if (key == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_KEY_FOR_THE_BATCH_UPDATE_REQUEST_1_IS_NULL;
            }
            if (regionName == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_UPDATE_REQUEST_1_IS_NULL;
            }
            String s = message.toLocalizedString(messageArgs);
            logger.warn(s);
            throw new Exception(s);
          }
          region = (LocalRegion)crHelper.getRegion(regionName);
          if (region == null) {
            handleRegionNull(servConn, regionName, batchId);
          } else {
            clientEvent = new EventIDHolder(eventId);
            if (versionTimeStamp > 0) {
              VersionTag tag = VersionTag.create(region.getVersionMember());
              tag.setIsGatewayTag(true);
              tag.setVersionTimeStamp(versionTimeStamp);
              tag.setDistributedSystemId(dsid);
              clientEvent.setVersionTag(tag);
            }
            clientEvent.setPossibleDuplicate(possibleDuplicate);
            handleMessageRetry(region, clientEvent);
            try {
              byte[] value = valuePart.getSerializedForm();
              boolean isObject = valuePart.isObject();
              AuthorizeRequest authzRequest = servConn.getAuthzRequest();
              if (authzRequest != null) {
                PutOperationContext putContext = authzRequest.putAuthorize(
                    regionName, key, value, isObject, callbackArg,
                    PutOperationContext.UPDATE);
                value = putContext.getSerializedValue();
                isObject = putContext.isObject();
              }
              boolean result = region.basicBridgePut(key, value, null, isObject,
                  callbackArg, servConn.getProxyID(), false, clientEvent);
              if (result|| clientEvent.isConcurrencyConflict()) {
                servConn.setModificationInfo(true, regionName, key);
                stats.incUpdateRequest();
              } else {
                final Object[] msgArgs = new Object[] { servConn.getName(),
                    regionName, key, valuePart, callbackArg };
                final StringId message = LocalizedStrings.ProcessBatch_0_FAILED_TO_UPDATE_ENTRY_FOR_REGION_1_KEY_2_VALUE_3_AND_CALLBACKARG_4;
                String s = message.toLocalizedString(msgArgs);
                logger.info(s);
                throw new Exception(s);
              }
            } catch (CancelException e) {
              // FIXME better exception hierarchy would avoid this check
              if (servConn.getCachedRegionHelper().getCache()
                  .getCancelCriterion().isCancelInProgress()) {
                if (logger.isDebugEnabled()) {
                  logger.debug("{} ignoring message of type {} from client {} because shutdown occurred during message processing.", servConn.getName(), MessageType.getString(msg.getMessageType()), servConn.getProxyID());
                }
                servConn.setFlagProcessMessagesAsFalse();
              } else {
                throw e;
              }
              return;
            } catch (Exception e) {
              // Preserve the connection under all circumstances
              logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_REQUEST_1_CONTAINING_2_EVENTS, new Object[] { servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents) }), e);
              throw e;
            }
          }
          break;
        case 2: // Destroy
          // Retrieve the callbackArg from the message parts if necessary
          index =  partNumber + 5;
          callbackArgExistsPart = msg.getPart(index++);
          {
            byte[] partBytes = (byte[])callbackArgExistsPart.getObject();
            callbackArgExists = partBytes[0] == 0x01;
          }
          if (callbackArgExists) {
            callbackArgPart = msg.getPart(index++);
            try {
              callbackArg = callbackArgPart.getObject();
            } catch (Exception e) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_DESTROY_REQUEST_1_CONTAINING_2_EVENTS, new Object[] { servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents) }), e);
              throw e;
            }
          }

          versionTimeStamp = msg.getPart(index++).getLong();
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Processing batch destroy request {} on {} for region {} key {}", servConn.getName(), batchId, servConn.getSocketString(), regionName, key);
          }

          // Process the destroy request
          if (key == null || regionName == null) {
            StringId message = null;
            if (key == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_KEY_FOR_THE_BATCH_DESTROY_REQUEST_1_IS_NULL;
            }
            if (regionName == null) {
              message = LocalizedStrings.ProcessBatch_0_THE_INPUT_REGION_NAME_FOR_THE_BATCH_DESTROY_REQUEST_1_IS_NULL;
            }
            Object[] messageArgs = new Object[] { servConn.getName(),
                Integer.valueOf(batchId) };
            String s = message.toLocalizedString(messageArgs);
            logger.warn(s);
            throw new Exception(s);
          }
          region = (LocalRegion)crHelper.getRegion(regionName);
          if (region == null) {
            handleRegionNull(servConn, regionName, batchId);
          } else {
            clientEvent = new EventIDHolder(eventId);
            if (versionTimeStamp > 0) {
              VersionTag tag = VersionTag.create(region.getVersionMember());
              tag.setIsGatewayTag(true);
              tag.setVersionTimeStamp(versionTimeStamp);
              tag.setDistributedSystemId(dsid);
              clientEvent.setVersionTag(tag);
            }
            handleMessageRetry(region, clientEvent);
            // Destroy the entry
            try {
              AuthorizeRequest authzRequest = servConn.getAuthzRequest();
              if (authzRequest != null) {
                DestroyOperationContext destroyContext = authzRequest
                    .destroyAuthorize(regionName, key, callbackArg);
                callbackArg = destroyContext.getCallbackArg();
              }
              region.basicBridgeDestroy(key, callbackArg,
                  servConn.getProxyID(), false, clientEvent);
              servConn.setModificationInfo(true, regionName, key);
              stats.incDestroyRequest();
            } catch (EntryNotFoundException e) {
              logger.info(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_DURING_BATCH_DESTROY_NO_ENTRY_WAS_FOUND_FOR_KEY_1, new Object[] { servConn.getName(), key }));
              // throw new Exception(e);
            }
          }
          break;
        case 3: // Update Time-stamp for a RegionEntry
          
          try {
            // Region name
            regionNamePart = msg.getPart(partNumber + 2);
            regionName = regionNamePart.getString();

            // Retrieve the event id from the message parts
            eventIdPart = msg.getPart(partNumber + 3);
            eventId = (EventID)eventIdPart.getObject();
            
            // Retrieve the key from the message parts
            keyPart = msg.getPart(partNumber + 4);
            key = keyPart.getStringOrObject();
       
            // Retrieve the callbackArg from the message parts if necessary
            index = partNumber + 5;
            callbackArgExistsPart = msg.getPart(index++);

            byte[] partBytes = (byte[])callbackArgExistsPart.getObject();
            callbackArgExists = partBytes[0] == 0x01;

            if (callbackArgExists) {
              callbackArgPart = msg.getPart(index++);
              callbackArg = callbackArgPart.getObject();
            }

          } catch (Exception e) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_VERSION_REQUEST_1_CONTAINING_2_EVENTS, new Object[] {servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)}), e);
            throw e;
          }

          versionTimeStamp = msg.getPart(index++).getLong();
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Processing batch update-version request {} on {} for region {} key {} value {} callbackArg {}", servConn.getName(), batchId, servConn.getSocketString(), regionName, key, valuePart, callbackArg);
          }
          // Process the update time-stamp request
          if (key == null || regionName == null) {
            StringId message = LocalizedStrings.ProcessBatch_0_CAUGHT_EXCEPTION_PROCESSING_BATCH_UPDATE_VERSION_REQUEST_1_CONTAINING_2_EVENTS;
            
            Object[] messageArgs = new Object[] { servConn.getName(), Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)};
            String s = message.toLocalizedString(messageArgs);
            logger.warn(s);
            throw new Exception(s);
          
          } else {
            region = (LocalRegion)crHelper.getRegion(regionName);
            
            if (region == null) {
              handleRegionNull(servConn, regionName, batchId);
            } else {

              clientEvent = new EventIDHolder(eventId);
              
              if (versionTimeStamp > 0) {
                VersionTag tag = VersionTag.create(region.getVersionMember());
                tag.setIsGatewayTag(true);
                tag.setVersionTimeStamp(versionTimeStamp);
                tag.setDistributedSystemId(dsid);
                clientEvent.setVersionTag(tag);
              }
              
              // Update the version tag
              try {

                region.basicBridgeUpdateVersionStamp(key, callbackArg, servConn.getProxyID(), false, clientEvent);

              } catch (EntryNotFoundException e) {
                logger.info(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_DURING_BATCH_UPDATE_VERSION_NO_ENTRY_WAS_FOUND_FOR_KEY_1, new Object[] { servConn.getName(), key }));
                // throw new Exception(e);
              }
            }
          }
          
          break;
        default:
          logger.fatal(LocalizedMessage.create(LocalizedStrings.Processbatch_0_UNKNOWN_ACTION_TYPE_1_FOR_BATCH_FROM_2, new Object[] { servConn.getName(), Integer.valueOf(actionType), servConn.getSocketString() }));
        stats.incUnknowsOperationsReceived();
        }
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("{} ignoring message of type {} from client {} because shutdown occurred during message processing.", servConn.getName(), MessageType.getString(msg.getMessageType()), servConn.getProxyID());
        }
        servConn.setFlagProcessMessagesAsFalse();
        return;
      } catch (Exception e) {
        // If an interrupted exception is thrown , rethrow it
        checkForInterrupt(servConn, e);
        
        //If we have an issue with the PDX registry, stop processing more data
        if(e.getCause() instanceof PdxRegistryMismatchException) {
          fatalException = e.getCause();
          logger.fatal(LocalizedMessage.create(LocalizedStrings.GatewayReceiver_PDX_CONFIGURATION, new Object[] {servConn.getMembershipID()}), e.getCause());
          break;
        }

        // logger.warn("Caught exception for batch " + batchId + " containing
        // " + numberOfEvents + " events (" + msg.getPayloadLength() + " bytes)
        // with " + (earlyAck ? "early" : "normal") + " acknowledgement on " +
        // getSocketString());
        // If the response has not already been written (it is not
        // early ack mode), increment the latest batch id replied,
        // write the batch exception to the caller and break
        if (!wroteResponse) {
          // Increment the batch id unless the received batch id is -1 (a
          // failover batch)
          DistributedSystem ds = crHelper.getCache().getDistributedSystem(); 
          String exceptionMessage = LocalizedStrings.GatewayReceiver_EXCEPTION_WHILE_PROCESSING_BATCH.toLocalizedString(
        	new Object[] {((InternalDistributedSystem) ds).getDistributionManager().getDistributedSystemId(), 
        		ds.getDistributedMember()}); 
          BatchException70 be = new BatchException70(exceptionMessage, e, indexWithoutPDXEvent, batchId);
          exceptions.add(be);
          if(!removeOnException) {
            break;
          }
          
          //servConn.setAsTrue(RESPONDED);
          //wroteResponse = true;
          //break;
        } else {
          // If it is early ack mode, attempt to process the remaining messages
          // in the batch.
          // This could be problematic depending on where the exception
          // occurred.
          return;
        }
      } finally {
        // Increment the partNumber
        if (actionType == 0 /* create */|| actionType == 1 /* update */) {
          if (callbackArgExists) {
            partNumber += 9;
          } else {
            partNumber += 8;
          } 
        } else if (actionType == 2 /* destroy */) {
          if (callbackArgExists) {
            partNumber += 8;
          } else {
            partNumber += 7;
          }
        } else if (actionType == 3 /* update-version */) {
          if (callbackArgExists) {
            partNumber += 8;
          } else {
            partNumber += 7;
          }
        }
      }
    }

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessBatchTime(start - oldStart);
    }
    if(fatalException != null) {
      servConn.incrementLatestBatchIdReplied(batchId);
      writeFatalException(msg, fatalException, servConn, batchId);
      servConn.setAsTrue(RESPONDED);
    }
    else if(!exceptions.isEmpty()) {
      servConn.incrementLatestBatchIdReplied(batchId);
      writeBatchException(msg, exceptions, servConn, batchId);
      servConn.setAsTrue(RESPONDED);
    }
    else if (!wroteResponse) {
      // Increment the batch id unless the received batch id is -1 (a failover
      // batch)
      servConn.incrementLatestBatchIdReplied(batchId);
      
      writeReply(msg, servConn, batchId, numberOfEvents);
      servConn.setAsTrue(RESPONDED);
      stats.incWriteProcessBatchResponseTime(DistributionStats.getStatTime()
          - start);
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sent process batch normal response for batch {} containing {} events ({} bytes) with {} acknowledgement on {}", servConn.getName(), batchId, numberOfEvents, msg.getPayloadLength(), (earlyAck ? "early" : "normal"), servConn.getSocketString());
      }
      // logger.warn("Sent process batch normal response for batch " +
      // batchId + " containing " + numberOfEvents + " events (" +
      // msg.getPayloadLength() + " bytes) with " + (earlyAck ? "early" :
      // "normal") + " acknowledgement on " + getSocketString());
    }
  }

  private void handleMessageRetry(LocalRegion region, EntryEventImpl clientEvent) {
    if (clientEvent.isPossibleDuplicate()) {
      if (region.getAttributes().getConcurrencyChecksEnabled()) {
        // recover the version tag from other servers
        clientEvent.setRegion(region);
        if (!recoverVersionTagForRetriedOperation(clientEvent)) {
          // no-one has seen this event
          clientEvent.setPossibleDuplicate(false);
        }
      }
    }
  }

  private void writeReply(Message msg, ServerConnection servConn, int batchId,
      int numberOfEvents) throws IOException {
    Message replyMsg = servConn.getResponseMessage();
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setTransactionId(msg.getTransactionId());
    replyMsg.setNumberOfParts(2);
    replyMsg.addIntPart(batchId);
    replyMsg.addIntPart(numberOfEvents);
    replyMsg.setTransactionId(msg.getTransactionId());
    replyMsg.send(servConn);
    servConn.setAsTrue(Command.RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: rpl tx: {} batchId {} numberOfEvents: {}", servConn.getName(), msg.getTransactionId(), batchId, numberOfEvents);
    }
  }

  private static void writeBatchException(Message origMsg, List<BatchException70> exceptions,
      ServerConnection servConn, int batchId) throws IOException {
    Message errorMsg = servConn.getErrorResponseMessage();
    errorMsg.setMessageType(MessageType.EXCEPTION);
    errorMsg.setNumberOfParts(2);
    errorMsg.setTransactionId(origMsg.getTransactionId());
    
    errorMsg.addObjPart(exceptions);
    //errorMsg.addStringPart(be.toString());
    errorMsg.send(servConn);
    for(Exception e: exceptions) {
      ((GatewayReceiverStats)servConn.getCacheServerStats()).incExceptionsOccured();
    }
    for(Exception be: exceptions) {
      if (logger.isWarnEnabled()) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_WROTE_BATCH_EXCEPTION, servConn.getName()), be);
      }  
    }
    
  }
  
  private static void writeFatalException(Message origMsg, Throwable exception,
      ServerConnection servConn, int batchId) throws IOException {
    Message errorMsg = servConn.getErrorResponseMessage();
    errorMsg.setMessageType(MessageType.EXCEPTION);
    errorMsg.setNumberOfParts(2);
    errorMsg.setTransactionId(origMsg.getTransactionId());

    //For older gateway senders, we need to send back an exception
    //they can deserialize.
    if((servConn.getClientVersion() == null 
        || servConn.getClientVersion().compareTo(Version.GFE_80) < 0)
        && exception instanceof PdxRegistryMismatchException) {
      PdxConfigurationException newException = new PdxConfigurationException(exception.getMessage());
      newException.setStackTrace(exception.getStackTrace());
      exception = newException;
    }
    errorMsg.addObjPart(exception);
    //errorMsg.addStringPart(be.toString());
    errorMsg.send(servConn);
    logger.warn(LocalizedMessage.create(LocalizedStrings.ProcessBatch_0_WROTE_BATCH_EXCEPTION, servConn.getName()), exception);
  }
}
