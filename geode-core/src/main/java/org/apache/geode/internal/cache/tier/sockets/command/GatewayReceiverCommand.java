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
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.CancelException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.operations.DestroyOperationContext;
import org.apache.geode.cache.operations.PutOperationContext;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.BatchException70;
import org.apache.geode.internal.cache.wan.GatewayReceiverStats;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.pdx.PdxConfigurationException;
import org.apache.geode.pdx.PdxRegistryMismatchException;
import org.apache.geode.pdx.internal.EnumId;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PeerTypeRegistration;

public class GatewayReceiverCommand extends BaseCommand {

  private static final GatewayReceiverCommand singleton = new GatewayReceiverCommand();

  public static Command getCommand() {
    return singleton;
  }

  private GatewayReceiverCommand() {}

  private void handleRegionNull(ServerConnection servConn, String regionName, int batchId) {
    InternalCache cache = servConn.getCachedRegionHelper().getCache();
    if (cache != null && cache.isCacheAtShutdownAll()) {
      throw cache.getCacheClosedException("Shutdown occurred during message processing");
    } else {
      String reason = String.format("Region %s was not found during batch create request %s",
          new Object[] {regionName, Integer.valueOf(batchId)});
      throw new RegionDestroyedException(reason, regionName);
    }
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Part regionNamePart = null, keyPart = null, valuePart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    int partNumber = 0;
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    GatewayReceiverStats stats = (GatewayReceiverStats) serverConnection.getCacheServerStats();
    EventID eventId = null;
    LocalRegion region = null;
    List<BatchException70> exceptions = new ArrayList<BatchException70>();
    Throwable fatalException = null;
    // requiresResponse = true;// let PROCESS_BATCH deal with this itself
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadProcessBatchRequestTime(start - oldStart);
    }
    Part callbackArgExistsPart;

    stats.incBatchSize(clientMessage.getPayloadLength());

    // Retrieve the number of events
    Part numberOfEventsPart = clientMessage.getPart(0);
    int numberOfEvents = numberOfEventsPart.getInt();
    stats.incEventsReceived(numberOfEvents);

    // Retrieve the batch id
    Part batchIdPart = clientMessage.getPart(1);
    int batchId = batchIdPart.getInt();

    // If this batch has already been seen, do not reply.
    // Instead, drop the batch and continue.
    if (batchId <= serverConnection.getLatestBatchIdReplied()) {
      if (GatewayReceiver.APPLY_RETRIES) {
        // Do nothing!!!
        logger.warn(
            "Received process batch request {} that has already been or is being processed. gemfire.gateway.ApplyRetries is set, so this batch will be processed anyway.",
            batchId);
      } else {
        logger.warn(
            "Received process batch request {} that has already been or is being processed. This process batch request is being ignored.",
            batchId);
        writeReply(clientMessage, serverConnection, batchId, numberOfEvents);
        return;
      }
      stats.incDuplicateBatchesReceived();
    }

    // Verify the batches arrive in order
    if (batchId != serverConnection.getLatestBatchIdReplied() + 1) {
      logger.warn(
          "Received process batch request {} out of order. The id of the last batch processed was {}. This batch request will be processed, but some messages may have been lost.",
          new Object[] {batchId, serverConnection.getLatestBatchIdReplied()});
      stats.incOutoforderBatchesReceived();
    }


    if (logger.isDebugEnabled()) {
      logger.debug("Received process batch request {} that will be processed.", batchId);
    }


    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Received process batch request {} containing {} events ({} bytes) with {} acknowledgement on {}",
          serverConnection.getName(), batchId, numberOfEvents, clientMessage.getPayloadLength(),
          "normal", serverConnection.getSocketString());
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
    int dsid = clientMessage.getPart(partNumber++).getInt();

    boolean removeOnException =
        clientMessage.getPart(partNumber++).getSerializedForm()[0] == 1 ? true : false;

    // event received in batch also have PDX events at the start of the batch,to
    // represent correct index on which the exception occurred, number of PDX
    // events need to be subtracted.
    int indexWithoutPDXEvent = -1; //
    for (int i = 0; i < numberOfEvents; i++) {
      boolean retry = true;
      boolean isPdxEvent = false;
      indexWithoutPDXEvent++;
      // System.out.println("Processing event " + i + " in batch " + batchId + "
      // starting with part number " + partNumber);
      Part actionTypePart = clientMessage.getPart(partNumber);
      int actionType = actionTypePart.getInt();

      long versionTimeStamp = VersionTag.ILLEGAL_VERSION_TIMESTAMP;
      EventIDHolder clientEvent = null;

      boolean callbackArgExists = false;

      try {
        do {
          if (isPdxEvent) {
            // This is a retried event. Reset the PDX event index.
            indexWithoutPDXEvent++;
          }
          isPdxEvent = false;
          Part possibleDuplicatePart = clientMessage.getPart(partNumber + 1);
          byte[] possibleDuplicatePartBytes;
          try {
            possibleDuplicatePartBytes = (byte[]) possibleDuplicatePart.getObject();
          } catch (Exception e) {
            logger.warn(String.format(
                "%s: Caught exception processing batch request %s containing %s events",
                new Object[] {serverConnection.getName(), Integer.valueOf(batchId),
                    Integer.valueOf(numberOfEvents)}),
                e);
            handleException(removeOnException, stats, e);
            break;
          }
          boolean possibleDuplicate = possibleDuplicatePartBytes[0] == 0x01;

          // Make sure instance variables are null before each iteration
          regionName = null;
          key = null;
          callbackArg = null;

          // Retrieve the region name from the message parts
          regionNamePart = clientMessage.getPart(partNumber + 2);
          regionName = regionNamePart.getString();
          if (regionName.equals(PeerTypeRegistration.REGION_FULL_PATH)) {
            indexWithoutPDXEvent--;
            isPdxEvent = true;
          }

          // Retrieve the event id from the message parts
          // This was going to be used to determine possible
          // duplication of events, but it is unused now. In
          // fact the event id is overridden by the FROM_GATEWAY
          // token.
          Part eventIdPart = clientMessage.getPart(partNumber + 3);
          eventIdPart.setVersion(serverConnection.getClientVersion());
          // String eventId = eventIdPart.getString();
          try {
            eventId = (EventID) eventIdPart.getObject();
          } catch (Exception e) {
            logger.warn(String.format(
                "%s: Caught exception processing batch request %s containing %s events",
                new Object[] {serverConnection.getName(), Integer.valueOf(batchId),
                    Integer.valueOf(numberOfEvents)}),
                e);
            handleException(removeOnException, stats, e);
            break;
          }

          // Retrieve the key from the message parts
          keyPart = clientMessage.getPart(partNumber + 4);
          try {
            key = keyPart.getStringOrObject();
          } catch (Exception e) {
            logger.warn(String.format(
                "%s: Caught exception processing batch request %s containing %s events",
                new Object[] {serverConnection.getName(), Integer.valueOf(batchId),
                    Integer.valueOf(numberOfEvents)}),
                e);
            handleException(removeOnException, stats, e);
            break;
          }
          int index = -1;
          switch (actionType) {
            case 0: // Create
              try {

                /*
                 * CLIENT EXCEPTION HANDLING TESTING CODE String keySt = (String) key;
                 * System.out.println("Processing new key: " + key); if
                 * (keySt.startsWith("failure")) { throw new Exception(LocalizedStrings
                 * .ProcessBatch_THIS_EXCEPTION_REPRESENTS_A_FAILURE_ON_THE_SERVER
                 * )); }
                 */

                // Retrieve the value from the message parts (do not deserialize it)
                valuePart = clientMessage.getPart(partNumber + 5);
                // try {
                // logger.warn(getName() + ": Creating key " + key + " value " +
                // valuePart.getObject());
                // } catch (Exception e) {}

                // Retrieve the callbackArg from the message parts if necessary
                index = partNumber + 6;
                callbackArgExistsPart = clientMessage.getPart(index++);
                {
                  byte[] partBytes = (byte[]) callbackArgExistsPart.getObject();
                  callbackArgExists = partBytes[0] == 0x01;
                }
                if (callbackArgExists) {
                  callbackArgPart = clientMessage.getPart(index++);
                  try {
                    callbackArg = callbackArgPart.getObject();
                  } catch (Exception e) {
                    logger
                        .warn(String.format(
                            "%s: Caught exception processing batch create request %s for %s events",
                            new Object[] {serverConnection.getName(),
                                Integer.valueOf(batchId),
                                Integer.valueOf(numberOfEvents)}),
                            e);
                    throw e;
                  }
                }
                if (logger.isDebugEnabled()) {
                  logger.debug(
                      "{}: Processing batch create request {} on {} for region {} key {} value {} callbackArg {}, eventId={}",
                      serverConnection.getName(), batchId, serverConnection.getSocketString(),
                      regionName, key, valuePart, callbackArg, eventId);
                }
                versionTimeStamp = clientMessage.getPart(index++).getLong();
                // Process the create request
                if (key == null || regionName == null) {
                  String message = null;
                  Object[] messageArgs =
                      new Object[] {serverConnection.getName(), Integer.valueOf(batchId)};
                  if (key == null) {
                    message =
                        "%s: The input region name for the batch create request %s is null";
                  }
                  if (regionName == null) {
                    message =
                        "%s: The input region name for the batch create request %s is null";
                  }
                  String s = String.format(message, messageArgs);
                  logger.warn(s);
                  throw new Exception(s);
                }
                region = (LocalRegion) crHelper.getRegion(regionName);
                if (region == null) {
                  handleRegionNull(serverConnection, regionName, batchId);
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
                  byte[] value = valuePart.getSerializedForm();
                  boolean isObject = valuePart.isObject();
                  // [sumedh] This should be done on client while sending
                  // since that is the WAN gateway
                  AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
                  if (authzRequest != null) {
                    PutOperationContext putContext =
                        authzRequest.putAuthorize(regionName, key, value, isObject, callbackArg);
                    value = putContext.getSerializedValue();
                    isObject = putContext.isObject();
                  }
                  // Attempt to create the entry
                  boolean result = false;
                  if (isPdxEvent) {
                    result = addPdxType(crHelper, key, value);
                  } else {
                    result = region.basicBridgeCreate(key, value, isObject, callbackArg,
                        serverConnection.getProxyID(), false, clientEvent, false);
                    // If the create fails (presumably because it already exists),
                    // attempt to update the entry
                    if (!result) {
                      result = region.basicBridgePut(key, value, null, isObject, callbackArg,
                          serverConnection.getProxyID(), false, clientEvent);
                    }
                  }

                  if (result || clientEvent.isConcurrencyConflict()) {
                    serverConnection.setModificationInfo(true, regionName, key);
                    stats.incCreateRequest();
                    retry = false;
                  } else {
                    // This exception will be logged in the catch block below
                    throw new Exception(
                        String.format(
                            "%s: Failed to create or update entry for region %s key %s value %s callbackArg %s",
                            new Object[] {serverConnection.getName(), regionName,
                                key, valuePart, callbackArg}));
                  }
                }
              } catch (Exception e) {
                logger.warn(String.format(
                    "%s: Caught exception processing batch create request %s for %s events",
                    new Object[] {serverConnection.getName(), Integer.valueOf(batchId),
                        Integer.valueOf(numberOfEvents)}),
                    e);
                handleException(removeOnException, stats, e);
              }
              break;
            case 1: // Update
              try {
                /*
                 * CLIENT EXCEPTION HANDLING TESTING CODE keySt = (String) key;
                 * System.out.println("Processing updated key: " + key); if
                 * (keySt.startsWith("failure")) { throw new Exception(LocalizedStrings
                 * .ProcessBatch_THIS_EXCEPTION_REPRESENTS_A_FAILURE_ON_THE_SERVER
                 * )); }
                 */

                // Retrieve the value from the message parts (do not deserialize it)
                valuePart = clientMessage.getPart(partNumber + 5);
                // try {
                // logger.warn(getName() + ": Updating key " + key + " value " +
                // valuePart.getObject());
                // } catch (Exception e) {}

                // Retrieve the callbackArg from the message parts if necessary
                index = partNumber + 6;
                callbackArgExistsPart = clientMessage.getPart(index++);
                {
                  byte[] partBytes = (byte[]) callbackArgExistsPart.getObject();
                  callbackArgExists = partBytes[0] == 0x01;
                }
                if (callbackArgExists) {
                  callbackArgPart = clientMessage.getPart(index++);
                  try {
                    callbackArg = callbackArgPart.getObject();
                  } catch (Exception e) {
                    logger
                        .warn(
                            String.format(
                                "%s: Caught exception processing batch update request %s containing %s events",
                                new Object[] {serverConnection.getName(),
                                    Integer.valueOf(batchId),
                                    Integer.valueOf(numberOfEvents)}),
                            e);
                    throw e;
                  }
                }
                versionTimeStamp = clientMessage.getPart(index++).getLong();
                if (logger.isDebugEnabled()) {
                  logger.debug(
                      "{}: Processing batch update request {} on {} for region {} key {} value {} callbackArg {}",
                      serverConnection.getName(), batchId, serverConnection.getSocketString(),
                      regionName, key, valuePart, callbackArg);
                }
                // Process the update request
                if (key == null || regionName == null) {
                  String message = null;
                  Object[] messageArgs =
                      new Object[] {serverConnection.getName(), Integer.valueOf(batchId)};
                  if (key == null) {
                    message =
                        "%s: The input key for the batch update request %s is null";
                  }
                  if (regionName == null) {
                    message =
                        "%s: The input region name for the batch update request %s is null";
                  }
                  String s = String.format(message, messageArgs);
                  logger.warn(s);
                  throw new Exception(s);
                }
                region = (LocalRegion) crHelper.getRegion(regionName);
                if (region == null) {
                  handleRegionNull(serverConnection, regionName, batchId);
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
                  byte[] value = valuePart.getSerializedForm();
                  boolean isObject = valuePart.isObject();
                  AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
                  if (authzRequest != null) {
                    PutOperationContext putContext = authzRequest.putAuthorize(regionName, key,
                        value, isObject, callbackArg, PutOperationContext.UPDATE);
                    value = putContext.getSerializedValue();
                    isObject = putContext.isObject();
                  }
                  boolean result = false;
                  if (isPdxEvent) {
                    result = addPdxType(crHelper, key, value);
                  } else {
                    result = region.basicBridgePut(key, value, null, isObject, callbackArg,
                        serverConnection.getProxyID(), false, clientEvent);
                  }
                  if (result || clientEvent.isConcurrencyConflict()) {
                    serverConnection.setModificationInfo(true, regionName, key);
                    stats.incUpdateRequest();
                    retry = false;
                  } else {
                    final Object[] msgArgs = new Object[] {serverConnection.getName(), regionName,
                        key, valuePart, callbackArg};
                    final String message =
                        "%s: Failed to update entry for region %s, key %s, value %s, and callbackArg %s";
                    String s = String.format(message, msgArgs);
                    logger.info(s);
                    throw new Exception(s);
                  }
                }
              } catch (Exception e) {
                // Preserve the connection under all circumstances
                logger.warn(String.format(
                    "%s: Caught exception processing batch update request %s containing %s events",
                    new Object[] {serverConnection.getName(), Integer.valueOf(batchId),
                        Integer.valueOf(numberOfEvents)}),
                    e);
                handleException(removeOnException, stats, e);
              }
              break;
            case 2: // Destroy
              try {
                // Retrieve the callbackArg from the message parts if necessary
                index = partNumber + 5;
                callbackArgExistsPart = clientMessage.getPart(index++);
                {
                  byte[] partBytes = (byte[]) callbackArgExistsPart.getObject();
                  callbackArgExists = partBytes[0] == 0x01;
                }
                if (callbackArgExists) {
                  callbackArgPart = clientMessage.getPart(index++);
                  try {
                    callbackArg = callbackArgPart.getObject();
                  } catch (Exception e) {
                    logger
                        .warn(
                            String.format(
                                "%s: Caught exception processing batch destroy request %s containing %s events",
                                new Object[] {serverConnection.getName(),
                                    Integer.valueOf(batchId),
                                    Integer.valueOf(numberOfEvents)}),
                            e);
                    throw e;
                  }
                }

                versionTimeStamp = clientMessage.getPart(index++).getLong();
                if (logger.isDebugEnabled()) {
                  logger.debug("{}: Processing batch destroy request {} on {} for region {} key {}",
                      serverConnection.getName(), batchId, serverConnection.getSocketString(),
                      regionName, key);
                }

                // Process the destroy request
                if (key == null || regionName == null) {
                  String message = null;
                  if (key == null) {
                    message =
                        "%s: The input key for the batch destroy request %s is null";
                  }
                  if (regionName == null) {
                    message =
                        "%s: The input region name for the batch destroy request %s is null";
                  }
                  Object[] messageArgs =
                      new Object[] {serverConnection.getName(), Integer.valueOf(batchId)};
                  String s = String.format(message, messageArgs);
                  logger.warn(s);
                  throw new Exception(s);
                }
                region = (LocalRegion) crHelper.getRegion(regionName);
                if (region == null) {
                  handleRegionNull(serverConnection, regionName, batchId);
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
                  AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
                  if (authzRequest != null) {
                    DestroyOperationContext destroyContext =
                        authzRequest.destroyAuthorize(regionName, key, callbackArg);
                    callbackArg = destroyContext.getCallbackArg();
                  }
                  try {
                    region.basicBridgeDestroy(key, callbackArg, serverConnection.getProxyID(),
                        false, clientEvent);
                    serverConnection.setModificationInfo(true, regionName, key);
                  } catch (EntryNotFoundException e) {
                    logger.info("{}: during batch destroy no entry was found for key {}",
                        new Object[] {serverConnection.getName(), key});
                    // throw new Exception(e);
                  }
                  stats.incDestroyRequest();
                  retry = false;
                }
              } catch (Exception e) {
                logger.warn(String.format(
                    "%s: Caught exception processing batch destroy request %s containing %s events",
                    new Object[] {serverConnection.getName(), Integer.valueOf(batchId),
                        Integer.valueOf(numberOfEvents)}),
                    e);
                handleException(removeOnException, stats, e);
              }
              break;
            case 3: // Update Time-stamp for a RegionEntry

              try {
                // Region name
                regionNamePart = clientMessage.getPart(partNumber + 2);
                regionName = regionNamePart.getString();

                // Retrieve the event id from the message parts
                eventIdPart = clientMessage.getPart(partNumber + 3);
                eventId = (EventID) eventIdPart.getObject();

                // Retrieve the key from the message parts
                keyPart = clientMessage.getPart(partNumber + 4);
                key = keyPart.getStringOrObject();

                // Retrieve the callbackArg from the message parts if necessary
                index = partNumber + 5;
                callbackArgExistsPart = clientMessage.getPart(index++);

                byte[] partBytes = (byte[]) callbackArgExistsPart.getObject();
                callbackArgExists = partBytes[0] == 0x01;

                if (callbackArgExists) {
                  callbackArgPart = clientMessage.getPart(index++);
                  callbackArg = callbackArgPart.getObject();
                }

                versionTimeStamp = clientMessage.getPart(index++).getLong();
                if (logger.isDebugEnabled()) {
                  logger.debug(
                      "{}: Processing batch update-version request {} on {} for region {} key {} value {} callbackArg {}",
                      serverConnection.getName(), batchId, serverConnection.getSocketString(),
                      regionName, key, valuePart, callbackArg);
                }
                // Process the update time-stamp request
                if (key == null || regionName == null) {
                  String message =
                      "%s: Caught exception processing batch update version request request %s containing %s events";

                  Object[] messageArgs = new Object[] {serverConnection.getName(),
                      Integer.valueOf(batchId), Integer.valueOf(numberOfEvents)};
                  String s = String.format(message, messageArgs);
                  logger.warn(s);
                  throw new Exception(s);

                } else {
                  region = (LocalRegion) crHelper.getRegion(regionName);

                  if (region == null) {
                    handleRegionNull(serverConnection, regionName, batchId);
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
                      region.basicBridgeUpdateVersionStamp(key, callbackArg,
                          serverConnection.getProxyID(), false, clientEvent);
                    } catch (EntryNotFoundException e) {
                      logger.info(
                          "Entry for key {} was not found in Region {} during ProcessBatch for Update Entry Version",
                          new Object[] {serverConnection.getName(), key});
                    }
                    retry = false;
                  }
                }
              } catch (Exception e) {
                logger.warn(String.format(
                    "%s: Caught exception processing batch update version request request %s containing %s events",
                    new Object[] {serverConnection.getName(), Integer.valueOf(batchId),
                        Integer.valueOf(numberOfEvents)}),
                    e);
                handleException(removeOnException, stats, e);
              }

              break;
            default:
              logger.fatal("{}: Unknown action type ({}) for batch from {}",
                  new Object[] {serverConnection.getName(), Integer.valueOf(actionType),
                      serverConnection.getSocketString()});
              stats.incUnknowsOperationsReceived();
          }
        } while (retry);
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{} ignoring message of type {} from client {} because shutdown occurred during message processing.",
              serverConnection.getName(), MessageType.getString(clientMessage.getMessageType()),
              serverConnection.getProxyID());
        }
        serverConnection.setFlagProcessMessagesAsFalse();
        serverConnection.setClientDisconnectedException(e);
        return;
      } catch (Exception e) {
        // If an interrupted exception is thrown , rethrow it
        checkForInterrupt(serverConnection, e);

        // If we have an issue with the PDX registry, stop processing more data
        if (e.getCause() instanceof PdxRegistryMismatchException) {
          fatalException = e.getCause();
          logger.fatal(String.format(
              "This gateway receiver has received a PDX type from %s that does match the existing PDX type. This gateway receiver will not process any more events, in order to prevent receiving objects which may not be deserializable.",
              new Object[] {serverConnection.getMembershipID()}), e.getCause());
          break;
        }

        // Increment the batch id unless the received batch id is -1 (a
        // failover batch)
        DistributedSystem ds = crHelper.getCache().getDistributedSystem();
        String exceptionMessage = String.format(
            "Exception occurred while processing a batch on the receiver running on DistributedSystem with Id: %s, DistributedMember on which the receiver is running: %s",
            new Object[] {
                ((InternalDistributedSystem) ds).getDistributionManager().getDistributedSystemId(),
                ds.getDistributedMember()});
        BatchException70 be =
            new BatchException70(exceptionMessage, e, indexWithoutPDXEvent, batchId);
        exceptions.add(be);
      } finally {
        // Increment the partNumber
        if (actionType == 0 /* create */ || actionType == 1 /* update */) {
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
    if (fatalException != null) {
      serverConnection.incrementLatestBatchIdReplied(batchId);
      writeFatalException(clientMessage, fatalException, serverConnection, batchId);
      serverConnection.setAsTrue(RESPONDED);
    } else if (!exceptions.isEmpty()) {
      serverConnection.incrementLatestBatchIdReplied(batchId);
      writeBatchException(clientMessage, exceptions, serverConnection, batchId);
      serverConnection.setAsTrue(RESPONDED);
    } else {
      // Increment the batch id unless the received batch id is -1 (a failover
      // batch)
      serverConnection.incrementLatestBatchIdReplied(batchId);

      writeReply(clientMessage, serverConnection, batchId, numberOfEvents);
      serverConnection.setAsTrue(RESPONDED);
      stats.incWriteProcessBatchResponseTime(DistributionStats.getStatTime() - start);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{}: Sent process batch normal response for batch {} containing {} events ({} bytes) with {} acknowledgement on {}",
            serverConnection.getName(), batchId, numberOfEvents, clientMessage.getPayloadLength(),
            "normal", serverConnection.getSocketString());
      }
    }
  }

  private boolean addPdxType(CachedRegionHelper crHelper, Object key, Object value)
      throws Exception {
    if (key instanceof EnumId) {
      EnumId enumId = (EnumId) key;
      value = BlobHelper.deserializeBlob((byte[]) value);
      crHelper.getCache().getPdxRegistry().addRemoteEnum(enumId.intValue(), (EnumInfo) value);
    } else {
      value = BlobHelper.deserializeBlob((byte[]) value);
      crHelper.getCache().getPdxRegistry().addRemoteType((int) key, (PdxType) value);
    }
    return true;
  }

  private void handleException(boolean removeOnException, GatewayReceiverStats stats, Exception e)
      throws Exception {
    if (shouldThrowException(removeOnException, e)) {
      throw e;
    } else {
      stats.incEventsRetried();
      Thread.sleep(500);
    }
  }

  private boolean shouldThrowException(boolean removeOnException, Exception e) {
    // Split out in case specific exceptions would short-circuit retry logic.
    // Currently it just considers the boolean.
    return removeOnException;
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

  private void writeReply(Message msg, ServerConnection servConn, int batchId, int numberOfEvents)
      throws IOException {
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
      logger.debug("{}: rpl tx: {} batchId {} numberOfEvents: {}", servConn.getName(),
          msg.getTransactionId(), batchId, numberOfEvents);
    }
  }

  private static void writeBatchException(Message origMsg, List<BatchException70> exceptions,
      ServerConnection servConn, int batchId) throws IOException {
    Message errorMsg = servConn.getErrorResponseMessage();
    errorMsg.setMessageType(MessageType.EXCEPTION);
    errorMsg.setNumberOfParts(2);
    errorMsg.setTransactionId(origMsg.getTransactionId());

    errorMsg.addObjPart(exceptions);
    // errorMsg.addStringPart(be.toString());
    errorMsg.send(servConn);
    for (Exception e : exceptions) {
      ((GatewayReceiverStats) servConn.getCacheServerStats()).incExceptionsOccurred();
    }
    for (Exception be : exceptions) {
      if (logger.isWarnEnabled()) {
        logger.warn(servConn.getName() + ": Wrote batch exception: ",
            be);
      }
    }
  }

  private static void writeFatalException(Message origMsg, Throwable exception,
      ServerConnection servConn, int batchId) throws IOException {
    Message errorMsg = servConn.getErrorResponseMessage();
    errorMsg.setMessageType(MessageType.EXCEPTION);
    errorMsg.setNumberOfParts(2);
    errorMsg.setTransactionId(origMsg.getTransactionId());

    // For older gateway senders, we need to send back an exception
    // they can deserialize.
    if ((servConn.getClientVersion() == null
        || servConn.getClientVersion().compareTo(Version.GFE_80) < 0)
        && exception instanceof PdxRegistryMismatchException) {
      PdxConfigurationException newException =
          new PdxConfigurationException(exception.getMessage());
      newException.setStackTrace(exception.getStackTrace());
      exception = newException;
    }
    errorMsg.addObjPart(exception);
    // errorMsg.addStringPart(be.toString());
    errorMsg.send(servConn);
    logger.warn(servConn.getName() + ": Wrote batch exception: ",
        exception);
  }
}
