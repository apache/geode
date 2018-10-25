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
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketServerLocation;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

/**
 * {@link Command} for {@link GetClientPRMetadataCommand}
 *
 *
 * @since GemFire 6.5
 */
public class GetClientPRMetadataCommand extends BaseCommand {

  private static final GetClientPRMetadataCommand singleton = new GetClientPRMetadataCommand();

  public static Command getCommand() {
    return singleton;
  }

  private GetClientPRMetadataCommand() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start)
      throws IOException, ClassNotFoundException, InterruptedException {
    String regionFullPath = null;
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    regionFullPath = clientMessage.getPart(0).getString();
    String errMessage = "";
    if (regionFullPath == null) {
      logger.warn("The input region path for the GetClientPRMetadata request is null");
      errMessage =
          "The input region path for the GetClientPRMetadata request is null";
      writeErrorResponse(clientMessage, MessageType.GET_CLIENT_PR_METADATA_ERROR,
          errMessage.toString(), serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    } else {
      Region region = crHelper.getRegion(regionFullPath);
      if (region == null) {
        logger.warn("Region was not found during GetClientPRMetadata request for region path : {}",
            regionFullPath);
        errMessage = "Region was not found during GetClientPRMetadata request for region path : "
            + regionFullPath;
        writeErrorResponse(clientMessage, MessageType.GET_CLIENT_PR_METADATA_ERROR,
            errMessage.toString(), serverConnection);
        serverConnection.setAsTrue(RESPONDED);
      } else {
        try {
          Message responseMsg = serverConnection.getResponseMessage();
          responseMsg.setTransactionId(clientMessage.getTransactionId());
          responseMsg.setMessageType(MessageType.RESPONSE_CLIENT_PR_METADATA);

          PartitionedRegion prRgion = (PartitionedRegion) region;
          Map<Integer, List<BucketServerLocation66>> bucketToServerLocations =
              prRgion.getRegionAdvisor().getAllClientBucketProfiles();
          responseMsg.setNumberOfParts(bucketToServerLocations.size());
          for (List<BucketServerLocation66> serverLocations : bucketToServerLocations.values()) {
            List<BucketServerLocation> oldServerLocations = new ArrayList<BucketServerLocation>();
            for (BucketServerLocation66 bs : serverLocations) {
              oldServerLocations.add(new BucketServerLocation(bs.getBucketId(), bs.getPort(),
                  bs.getHostName(), bs.isPrimary(), bs.getVersion()));
              responseMsg.addObjPart(oldServerLocations);
            }
          }
          responseMsg.send();
          clientMessage.clearParts();
        } catch (Exception e) {
          writeException(clientMessage, e, false, serverConnection);
        } finally {
          serverConnection.setAsTrue(Command.RESPONDED);
        }
      }
    }
  }

}
